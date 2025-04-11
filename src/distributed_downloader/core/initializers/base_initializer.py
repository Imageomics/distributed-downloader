import uuid
from abc import ABC, abstractmethod
from urllib.parse import urlparse

import pyspark.sql.functions as func
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from distributed_downloader.tools import Config, init_logger, load_dataframe


class BaseInitializer(ABC):
    """
    Base class for all initializers.
    This class is responsible for initializing the Spark session and loading the raw dataframe.
    It also provides methods for saving the results and extracting the server name from the URL.

    It is an abstract class, therefore method run() must be implemented in the child class.
    """
    def __init__(self, config: Config):
        """
        Initializes the Spark session and loads the raw dataframe.
        Args:
            config: Config object containing the configuration parameters.
        """
        self.config = config

        self.input_path = self.config["path_to_input"]
        self.output_path = self.config.get_folder("urls_folder")

        self.logger = init_logger(__name__)

        self.spark = SparkSession.builder.appName("Multimedia prep").getOrCreate()
        self.spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        self.spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    def load_raw_df(self) -> DataFrame:
        """
        Loads the raw dataframe from the input path (taken from the config file).
        Returns:
            DataFrame: Raw dataframe.
        """
        return load_dataframe(self.spark, self.input_path)

    def save_results(self, resul_df: DataFrame) -> None:
        """
        Saves the results to the output path (taken from the config file).
        Args:
            resul_df: DataFrame to be saved.

        Returns:
            None
        """
        (
            resul_df.repartition("server_name", "partition_id")
            .write.partitionBy("server_name", "partition_id")
            .mode("overwrite")
            .format("parquet")
            .save(self.output_path)
        )

    def extract_server_name(self, data_frame: DataFrame) -> DataFrame:
        """
        Extracts the server name from the URL (`identifier` column) and adds it as a new column -
        `server_name` to the dataframe.
        Args:
            data_frame: DataFrame to be processed.

        Returns:
            DataFrame: DataFrame with the new column.
        """
        return data_frame.withColumn(
            "server_name", self.get_server_name(data_frame.identifier)
        )

    def generate_uuid(self, data_frame: DataFrame) -> DataFrame:
        """
        Generates a UUID for each row in the dataframe and adds it as a new column - `uuid`.
        Args:
            data_frame: DataFrame to be processed.

        Returns:
            DataFrame: DataFrame with the new column.
        """
        return data_frame.withColumn("uuid", self.get_uuid())

    def partition_dataframe(self, data_frame: DataFrame) -> DataFrame:
        """
        Partitions the dataframe into batches based on the `server_name` and batch size.
        Args:
            data_frame: DataFrame to be processed.

        Returns:
            DataFrame: DataFrame with the new column.
        """
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
        """
        PySpark UDF that extracts the server name from the URL.
        Args:
            url: URL to be processed.

        Returns:
            str: Server name.
        """
        return urlparse(url).netloc

    @staticmethod
    @udf(returnType=StringType())
    def get_uuid():
        """
        PySpark UDF that generates a UUID.
        Returns:
            str: UUID.
        """
        return str(uuid.uuid4())

    def __del__(self):
        """
        Destructor method that stops the Spark session when the object is deleted.
        """
        self.spark.stop()

    @abstractmethod
    def run(self):
        """
        Abstract method that must be implemented in the child class.
        Intended to be an entry point for the class.
        Returns:
            None
        """
        pass
