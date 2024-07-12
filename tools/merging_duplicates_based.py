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

    merge_target = "/fs/ess/PAS2136/TreeOfLife"
    merge_object = "/fs/scratch/PAS2136/gbif/processed/leftovers/multimedia/downloaded_images"
    filter_results = "/fs/scratch/PAS2136/gbif/processed/leftovers/multimedia/filtered_out"

    spark = SparkSession.builder.appName("CopyPaste").getOrCreate()
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    target_df: DataFrame = (spark
                            .read
                            .schema(success_scheme)
                            .option("basePath", merge_target)
                            # .option("ignoreCorruptFiles", "true")
                            .parquet(merge_target + "/source=*/data/server=*/data_*.parquet")
                            .select("uuid",
                                    "gbif_id",
                                    "hashsum_original")
                            .withColumnsRenamed({
                                "uuid": "uuid_main",
                                "gbif_id": "gbif_id_main"
                            })
                            )

    object_df = (spark
                 .read
                 .schema(success_scheme)
                 .option("basePath", merge_object)
                 # .option("ignoreCorruptFiles", "true")
                 .parquet(merge_object + "/*/*/successes.parquet")
                 .select("uuid",
                         "gbif_id",
                         "ServerName",
                         "partition_id",
                         "hashsum_original"))

    duplicate_records = object_df.join(target_df, on=["hashsum_original"], how="inner")

    (duplicate_records
     .repartition(10)
     .write
     .csv(filter_results + "/merged_duplicated_check",
          header=True,
          mode="overwrite"))

    logger.info(f"duplicated number: {duplicate_records.count()}")

    spark.stop()
