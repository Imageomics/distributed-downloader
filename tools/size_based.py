import pyspark.sql.functions as func
from pyspark.sql import DataFrame, Window
from pyspark.sql import SparkSession
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
    StructField("image", BinaryType(), False)
])

if __name__ == "__main__":
    logger = init_logger(__name__)
    threshold_size = 224

    # downloaded_images = "/fs/scratch/PAS2136/gbif/processed/2024-05-01/multimedia_prep/downloaded_images"
    downloaded_images = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/downloaded_images"
    filter_results = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/filtered_out"

    spark = SparkSession.builder.appName("CopyPaste").getOrCreate()
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    successes_df: DataFrame = (spark
                               .read
                               .schema(success_scheme)
                               .option("basePath", downloaded_images)
                               .option("ignoreCorruptFiles", "true")
                               .parquet(downloaded_images + "/*/*/successes.parquet"))

    successes_df = successes_df.withColumn("is_big", func.array_min(func.col("original_size")) >= threshold_size)

    too_small_images = successes_df.filter(~successes_df["is_big"]).select("uuid", "gbif_id", "ServerName",
                                                                           "partition_id")

    logger.info(f"Too small images number: {too_small_images.count()}")

    (too_small_images
     .repartition(10)
     .write
     .csv(filter_results + "/too_small_small",
          header=True,
          mode="overwrite"))

    spark.stop()
