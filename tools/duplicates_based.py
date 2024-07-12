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

    base_path = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia_copy"
    downloaded_images = f"{base_path}/downloaded_images_copy"
    filter_results = f"{base_path}/filtered_out"

    spark = SparkSession.builder.appName("CopyPaste").getOrCreate()
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    successes_df: DataFrame = (spark
                               .read
                               .schema(success_scheme)
                               .option("basePath", downloaded_images)
                               .option("ignoreCorruptFiles", "true")
                               .parquet(downloaded_images + "/*/*/successes.parquet"))

    not_duplicate_records = (successes_df
                             .groupBy("hashsum_original")
                             .count()
                             .where('count = 1')
                             .drop('count'))

    duplicate_records = (successes_df
                         .join(not_duplicate_records, on="hashsum_original", how='left_anti')
                         .select("uuid", "gbif_id", "ServerName", "partition_id", "hashsum_original"))

    window = Window.partitionBy("hashsum_original").orderBy("partition_id", "ServerName")

    duplicate_records_top = (duplicate_records
                             .withColumn("rn", func.row_number().over(window))
                             .where("rn == 1")
                             .drop("rn"))

    duplicate_records_top = duplicate_records_top.withColumnsRenamed(
        {"uuid": "uuid_main",
         "gbif_id": "gbif_id_main",
         "ServerName": "ServerName_main",
         "partition_id": "partition_id_main"})

    duplicate_records = (duplicate_records
                         .join(duplicate_records_top, on="hashsum_original", how="left")
                         .where("uuid != uuid_main")
                         .drop("hashsum_original")
                         )

    (duplicate_records
     .repartition(10)
     .write
     .csv(filter_results + "/duplicated",
          header=True,
          mode="overwrite"))

    logger.info(f"duplicated number: {duplicate_records.count()}")

    spark.stop()
