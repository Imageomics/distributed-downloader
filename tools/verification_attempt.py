import hashlib
from typing import Iterator

import numpy as np
import pandas
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import StructField, StringType, StructType, LongType, BooleanType, ArrayType, \
    BinaryType

from distributed_downloader.utils import init_logger

success_scheme = StructType([
    StructField("uuid", StringType(), False),
    StructField("gbif_id", LongType(), False),
    StructField("identifier", StringType(), False),
    StructField("is_license_full", BooleanType(), False),
    StructField("license", StringType(), True),
    StructField("source", StringType(), True),
    StructField("title", StringType(), True),
    StructField("original_size", ArrayType(LongType(), False), False),
    StructField("resized_size", ArrayType(LongType(), False), False),
    StructField("hashsum_original", StringType(), False),
    StructField("hashsum_resized", StringType(), False),
    StructField("image", BinaryType())
])

verification_result_schema = StructType(success_scheme.fields + [
    StructField("is_corrupted", BooleanType(), False)
])


def verify_image(row):
    img_size = row["resized_size"]
    image_hdf5 = row["hashsum_resized"]
    img_bytes = row["image"]
    image_np = np.frombuffer(img_bytes, dtype=np.uint8).reshape([img_size[0], img_size[1], 3])
    return pandas.Series([min(img_size) > 255
                          and hashlib.md5(img_bytes).hexdigest() == image_hdf5
                          and np.any((image_np <= 255) & (image_np >= 0))], index="is_corrupted")


if __name__ == "__main__":
    logger = init_logger(__name__)

    downloaded_images = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/downloaded_images"

    spark = SparkSession.builder.appName("CopyPaste").getOrCreate()
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")


    # @pandas_udf("boolean")
    # def verify_images(img_size_s: pandas.Series,
    #                   img_hash_s: pandas.Series,
    #                   img_bytes_s: pandas.Series) -> pandas.Series:
    #     pdf = pandas.DataFrame([img_size_s, img_hash_s, img_bytes_s],
    #                            columns=["resized_size", "hashsum_resized", "image"])
    #     series: pandas.Series = pdf.apply(verify_image, axis=1)
    #     logger.info(series.info)
    #     return series


    @udf(returnType="boolean")
    def verify_image(img_size, image_hdf5, img_bytes):
        image_np = np.frombuffer(img_bytes, dtype=np.uint8).reshape([img_size[0], img_size[1], 3])
        return (min(img_size) > 255
                and hashlib.md5(img_bytes).hexdigest() == image_hdf5
                and np.any((image_np <= 255) & (image_np >= 0)))


    @udf(returnType="boolean")
    def dummy(resized_size, img_bytes):
        image_np = np.frombuffer(img_bytes, dtype=np.uint8).reshape([resized_size[0], resized_size[1], 3])
        return min(resized_size) > 255

    successes_df: DataFrame = (spark
                               .read
                               .schema(success_scheme)
                               .option("basePath", downloaded_images)
                               .option("ignoreCorruptFiles", "true")
                               .parquet(downloaded_images + "/*/*/successes.parquet")
                               .repartition(100_000))

    # logger.info(f"Total number: {successes_df.count()}")

    # verified_filter = successes_df.mapInPandas(verify_images, verification_result_schema)

    successes_df = successes_df.withColumn("is_corrupted", dummy("resized_size", "image"))

    uncorrupted_images = successes_df.filter(successes_df["is_corrupted"])

    logger.info(f"Uncorrupted number: {uncorrupted_images.count()}")

    spark.stop()
