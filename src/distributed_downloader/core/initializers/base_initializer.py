import uuid
from abc import ABC, abstractmethod
from urllib.parse import urlparse

import pyspark.sql.functions as func
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from distributed_downloader.tools import Config, init_logger, load_dataframe


class BaseInitializer(ABC):
    def __init__(self, config: Config):
        self.config = config

        self.input_path = self.config["path_to_input"]
        self.output_path = self.config.get_folder("urls_folder")

        self.logger = init_logger(__name__)

        self.spark = SparkSession.builder.appName("Multimedia prep").getOrCreate()
        self.spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        self.spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    def load_raw_df(self):
        return load_dataframe(self.spark, self.input_path)

    def save_results(self, resul_df: DataFrame):
        (
            resul_df.repartition("server_name", "partition_id")
            .write.partitionBy("server_name", "partition_id")
            .mode("overwrite")
            .format("parquet")
            .save(self.output_path)
        )

    def partition_dataframe(self, data_frame: DataFrame) -> DataFrame:
        columns = data_frame.columns

        self.logger.info("Starting batching")

        servers_grouped = (
            data_frame.select("server_name")
            .groupBy("server_name")
            .count()
            .withColumn(
                "batch_count",
                func.floor(
                    func.col("count")
                    / self.config["downloader_parameters"]["batch_size"]
                ),
            )
        )

        window_part = Window.partitionBy("server_name").orderBy("server_name")
        master_df_filtered = (
            data_frame.withColumn("row_number", func.row_number().over(window_part))
            .join(servers_grouped, ["server_name"])
            .withColumn(
                "partition_id", func.col("row_number") % func.col("batch_count")
            )
            .withColumn(
                "partition_id",
                (
                    func.when(func.col("partition_id").isNull(), 0).otherwise(
                        func.col("partition_id")
                    )
                ),
            )
            .select(*columns, "partition_id")
        )

        self.logger.info("Finished batching")

        return master_df_filtered

    @staticmethod
    @udf(returnType=StringType())
    def get_server_name(url: str):
        return urlparse(url).netloc

    @staticmethod
    @udf(returnType=StringType())
    def get_uuid():
        return str(uuid.uuid4())

    def __del__(self):
        self.spark.stop()

    @abstractmethod
    def run(self):
        pass
