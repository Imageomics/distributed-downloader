import os.path
from functools import partial
from typing import Optional

import pandas as pd
import pyspark.sql as ps
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from distributed_downloader.core.mpi_downloader.dataclasses import SuccessEntry
from distributed_downloader.tools.config import Config
from distributed_downloader.tools.registry import ToolsBase
from distributed_downloader.tools.registry import ToolsRegistryBase

FilterRegister = partial(ToolsRegistryBase.register, "filter")
__all__ = [
    "FilterRegister",
    "SizeBasedFiltering",
    "DuplicatesBasedFiltering",
    "ResizeToolFilter",
    "ImageVerificationToolFilter",
]


class FilterToolBase(ToolsBase):
    def __init__(self, cfg: Config):
        super().__init__(cfg)

        self.filter_family = "filter"


class SparkFilterToolBase(FilterToolBase):
    success_scheme = SuccessEntry.get_success_spark_scheme()

    def __init__(self, cfg: Config, spark: SparkSession = None):
        super().__init__(cfg)
        self.spark: SparkSession = (
            spark
            if spark is not None
            else SparkSession.builder.appName("Filtering").getOrCreate()
        )
        self.spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        self.spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    def run(self):
        raise NotImplementedError()

    def load_data_parquet(self, scheme: Optional[StructType] = None):
        if scheme is None:
            scheme = self.success_scheme
        return (
            self.spark.read.schema(scheme)
            .option("basePath", self.downloaded_images_path)
            .parquet(
                self.downloaded_images_path
                + "/server_name=*/partition_id=*/successes.parquet"
            )
        )

    def save_filter(self, df: ps.DataFrame):
        if self.filter_name is None:
            raise ValueError("filter name was not defined")
        (
            df.repartition(10).write.csv(
                os.path.join(self.tools_path, self.filter_name, "filter_table"),
                header=True,
                mode="overwrite",
            )
        )

    def __del__(self):
        if self.spark is not None:
            self.spark.stop()


@FilterRegister("size_based")
class SizeBasedFiltering(SparkFilterToolBase):
    def __init__(self, cfg: Config, spark: SparkSession = None):
        super().__init__(cfg, spark)
        self.filter_name: str = "size_based"

        assert "threshold_size" in self.config["tools_parameters"], ValueError(
            "threshold_size have to be defined"
        )
        assert isinstance(
            self.config["tools_parameters"]["threshold_size"], int
        ), ValueError("threshold_size have to be Integer")

        self.threshold_size = self.config["tools_parameters"]["threshold_size"]

    def run(self):
        successes_df: ps.DataFrame = self.load_data_parquet()

        successes_df = successes_df.withColumn(
            "is_big", func.array_min(func.col("original_size")) >= self.threshold_size
        )

        too_small_images = successes_df.filter(~successes_df["is_big"]).select(
            "uuid", "source_id", "server_name", "partition_id"
        )

        self.save_filter(too_small_images)

        self.logger.info(f"Too small images number: {too_small_images.count()}")


@FilterRegister("duplication_based")
class DuplicatesBasedFiltering(SparkFilterToolBase):
    def __init__(self, cfg: Config, spark: SparkSession = None):
        super().__init__(cfg, spark)
        self.filter_name: str = "duplication_based"

    def run(self):
        successes_df: ps.DataFrame = self.load_data_parquet()

        not_duplicate_records = (
            successes_df.groupBy("hashsum_original")
            .count()
            .where("count = 1")
            .drop("count")
        )

        duplicate_records = successes_df.join(
            not_duplicate_records, on="hashsum_original", how="left_anti"
        ).select("uuid", "source_id", "server_name", "partition_id", "hashsum_original")

        window = ps.Window.partitionBy("hashsum_original").orderBy(
            "partition_id", "server_name"
        )

        duplicate_records_top = (
            duplicate_records.withColumn("rn", func.row_number().over(window))
            .where("rn == 1")
            .drop("rn")
        )

        duplicate_records_top = duplicate_records_top.withColumnsRenamed(
            {
                "uuid": "uuid_main",
                "source_id": "source_id_main",
                "server_name": "server_name_main",
                "partition_id": "partition_id_main",
            }
        )

        duplicate_records = (
            duplicate_records.join(
                duplicate_records_top, on="hashsum_original", how="left"
            )
            .where("uuid != uuid_main")
            .drop("hashsum_original")
        )

        self.save_filter(duplicate_records)

        self.logger.info(f"duplicated number: {duplicate_records.count()}")


class PythonFilterToolBase(FilterToolBase):
    def __init__(self, cfg: Config):
        super().__init__(cfg)

    def get_all_paths_to_merge(self) -> pd.DataFrame:
        all_schedules = []
        path = self.downloaded_images_path
        for folder in os.listdir(path):
            server_name = folder.split("=")[1]
            for partition in os.listdir(f"{path}/{folder}"):
                partition_path = f"{path}/{folder}/{partition}"
                if not os.path.exists(
                    f"{partition_path}/successes.parquet"
                ) or not os.path.exists(f"{partition_path}/completed"):
                    continue
                all_schedules.append([server_name, partition.split("=")[1]])
        return pd.DataFrame(all_schedules, columns=["server_name", "partition_id"])

    def run(self):
        filter_table = self.get_all_paths_to_merge()

        filter_table_folder = os.path.join(
            self.tools_path, self.filter_name, "filter_table"
        )
        os.makedirs(filter_table_folder, exist_ok=True)

        filter_table.to_csv(
            filter_table_folder + "/table.csv", header=True, index=False
        )


@FilterRegister("resize")
class ResizeToolFilter(PythonFilterToolBase):
    def __init__(self, cfg: Config):
        super().__init__(cfg)
        self.filter_name = "resize"


@FilterRegister("image_verification")
class ImageVerificationToolFilter(PythonFilterToolBase):
    def __init__(self, cfg: Config):
        super().__init__(cfg)
        self.filter_name = "image_verification"
