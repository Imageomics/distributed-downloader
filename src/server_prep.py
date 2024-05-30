import argparse
import os.path
import uuid
from urllib.parse import urlparse

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from schemes import multimedia_scheme
from utils.utils import truncate_folder

BATCH_SIZE = 10_000


@udf(returnType=StringType())
def get_server_name(url: str):
    return urlparse(url).netloc


@udf(returnType=StringType())
def get_uuid():
    return str(uuid.uuid4())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert multimedia data to server batches')

    parser.add_argument('input_path', metavar='input_path', type=str, help='the path to the file with multimedia data, must be a tab-delimited text file')
    parser.add_argument('output_path', metavar='output_path', type=str, help='the path to the output folder (folder for download components (e.g., server batches and image folder))')

    # parse the arguments
    args = parser.parse_args()
    input_path: str = args.input_path
    output_path: str = args.output_path
    servers_batched_folder: str = os.getenv("DOWNLOADER_URLS_FOLDER", "servers_batched")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Multimedia prep").getOrCreate()
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    truncate_folder(output_path)

    if os.path.isfile(input_path):
        multimedia_df = spark.read.csv(
            input_path,
            sep="\t",
            header=True,
            schema=multimedia_scheme.schema
        )
    else:
        multimedia_df = spark.read.load(
            input_path
        )

    multimedia_df_prep = (multimedia_df
                          .filter((multimedia_df["gbifID"].isNotNull())
                                  & (multimedia_df["identifier"].isNotNull())
                                  & (
                                          (multimedia_df["type"] == "StillImage")
                                          | (
                                                  (multimedia_df["type"].isNull())
                                                  & (multimedia_df["format"].contains("image"))
                                          )
                                  ))
                          .repartition(20))

    multimedia_df_prep = multimedia_df_prep.withColumn("ServerName",
                                                       get_server_name(multimedia_df_prep.identifier))
    multimedia_df_prep = multimedia_df_prep.withColumn("UUID", get_uuid())

    columns = multimedia_df_prep.columns

    servers_batched_dir = os.path.join(output_path, servers_batched_folder)
    os.makedirs(servers_batched_dir, exist_ok=True)

    print("Starting batching")

    servers_grouped = (multimedia_df_prep
                       .select("ServerName")
                       .groupBy("ServerName")
                       .count()
                       .withColumn("batch_count", F.floor(F.col("count") / BATCH_SIZE)))

    window_part = Window.partitionBy("ServerName").orderBy("ServerName")
    master_df_filtered = (multimedia_df_prep
                          .withColumn("row_number", F.row_number().over(window_part))
                          .join(servers_grouped, ["ServerName"])
                          .withColumn("partition_id", F.col("row_number") % F.col("batch_count"))
                          .withColumn("partition_id", F.when(F.col("partition_id").isNull(), 0).otherwise(F.col("partition_id")))
                          .select(*columns, "partition_id"))

    (master_df_filtered
     .repartition("ServerName", "partition_id")
     .write
     .partitionBy("ServerName", "partition_id")
     .mode("overwrite")
     .format("parquet")
     .save(servers_batched_dir))

    spark.stop()
