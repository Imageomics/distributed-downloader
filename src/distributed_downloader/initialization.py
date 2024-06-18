import os.path
import uuid
from typing import Sequence, Optional, List
from urllib.parse import urlparse

import pyspark.sql.functions as func
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from distributed_downloader.utils import load_config, load_dataframe, truncate_paths, init_logger
from schemes import multimedia_scheme


@udf(returnType=StringType())
def get_server_name(url: str):
    return urlparse(url).netloc


@udf(returnType=StringType())
def get_uuid():
    return str(uuid.uuid4())


def get_folders_path(path: str, folders: Sequence[str], exclude_folder: Optional[str] = None) -> List[str]:
    result = []
    for folder in folders:
        if folder != exclude_folder:
            result.append(f"{path}/{folder}")
    return result


def init_filestructure(_config: dict):
    _output_folder = _config["path_to_output_folder"]
    truncate_paths(
        [_output_folder,
         *get_folders_path(
             _output_folder,
             list(_config["output_structure"].values()),
             _config["output_structure"]["inner_checkpoint_file"]
         )])


if __name__ == "__main__":
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = load_config(config_path)

    # Initialize parameters
    input_path = config["path_to_input"]
    # init_filestructure(config)
    output_path = f"{config['path_to_output_folder']}/{config['output_structure']['urls_folder']}"
    logger = init_logger(__name__)

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Multimedia prep").getOrCreate()
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    multimedia_df = load_dataframe(spark, input_path, multimedia_scheme.schema)

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

    logger.info("Starting batching")

    servers_grouped = (multimedia_df_prep
                       .select("ServerName")
                       .groupBy("ServerName")
                       .count()
                       .withColumn("batch_count",
                                   func.floor(func.col("count") / config["downloader_parameters"]["batch_size"])))

    window_part = Window.partitionBy("ServerName").orderBy("ServerName")
    master_df_filtered = (multimedia_df_prep
                          .withColumn("row_number", func.row_number().over(window_part))
                          .join(servers_grouped, ["ServerName"])
                          .withColumn("partition_id", func.col("row_number") % func.col("batch_count"))
                          .withColumn("partition_id",
                                      (func
                                       .when(func.col("partition_id").isNull(), 0)
                                       .otherwise(func.col("partition_id"))))
                          .select(*columns, "partition_id"))

    logger.info("Writing to parquet")

    (master_df_filtered
     .repartition("ServerName", "partition_id")
     .write
     .partitionBy("ServerName", "partition_id")
     .mode("overwrite")
     .format("parquet")
     .save(output_path))

    logger.info("Finished batching")

    spark.stop()
