import os.path
import uuid
from typing import Dict
from urllib.parse import urlparse

import pyspark.sql.functions as func
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from distributed_downloader.tools.config import Config
from distributed_downloader.tools.utils import load_dataframe, truncate_paths, init_logger


@udf(returnType=StringType())
def get_server_name(url: str):
    return urlparse(url).netloc


@udf(returnType=StringType())
def get_uuid():
    return str(uuid.uuid4())


def init_filestructure(file_structure: Dict[str, str]) -> None:
    filtered_fs = [value for key, value in file_structure.items() if key not in ["inner_checkpoint_file", "ignored_table"]]
    truncate_paths(filtered_fs)


if __name__ == "__main__":
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = Config.from_path(config_path, "downloader")

    # Initialize parameters
    input_path = config["path_to_input"]
    # init_filestructure(config)
    output_path = config.get_folder("urls_folder")
    logger = init_logger(__name__)

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Multimedia prep").getOrCreate()
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    multimedia_df = load_dataframe(spark, input_path)

    multimedia_df_prep = (multimedia_df
                          .filter((multimedia_df["uuid"].isNotNull())
                                  & (multimedia_df["url"].isNotNull())
                                  & (multimedia_df["valid"].cast("boolean")))
                          .repartition(20)
                          .withColumnsRenamed({"uuid": "source_id", "url": "identifier"})
                          .withColumn("license", func.lit("https://creativecommons.org/licenses/by-nc-nd/4.0/legalcode.en")))

    multimedia_df_prep = multimedia_df_prep.withColumn("server_name",
                                                       get_server_name(multimedia_df_prep.identifier))
    multimedia_df_prep = multimedia_df_prep.withColumn("uuid", get_uuid())

    columns = multimedia_df_prep.columns

    logger.info("Starting batching")

    servers_grouped = (multimedia_df_prep
                       .select("server_name")
                       .groupBy("server_name")
                       .count()
                       .withColumn("batch_count",
                                   func.floor(func.col("count") / config["downloader_parameters"]["batch_size"])))

    window_part = Window.partitionBy("server_name").orderBy("server_name")
    master_df_filtered = (multimedia_df_prep
                          .withColumn("row_number", func.row_number().over(window_part))
                          .join(servers_grouped, ["server_name"])
                          .withColumn("partition_id", func.col("row_number") % func.col("batch_count"))
                          .withColumn("partition_id",
                                      (func
                                       .when(func.col("partition_id").isNull(), 0)
                                       .otherwise(func.col("partition_id"))))
                          .select(*columns, "partition_id"))

    logger.info(f"Writing to parquet: {output_path}")

    (master_df_filtered
     .repartition("server_name", "partition_id")
     .write
     .partitionBy("server_name", "partition_id")
     .mode("overwrite")
     .format("parquet")
     .save(output_path))

    logger.info("Finished batching")

    spark.stop()
