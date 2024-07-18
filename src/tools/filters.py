import glob
import os.path

import pandas as pd
from attr import define
import pyspark.sql as ps
import pyspark.sql.functions as func
from pyspark.sql import SparkSession

from tools.config import Config
from tools.tools_base import ToolsBase


@define
class FilterToolBase(ToolsBase):
    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)

    def create_filter_table(self):
        raise NotImplementedError()


@define
class SparkFilterToolBase(FilterToolBase):
    from pyspark.sql.types import StructField, StringType, StructType, LongType, BooleanType, ArrayType, \
        BinaryType

    spark: SparkSession = None
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

    def __attrs_pre_init__(self, cfg: Config, spark: SparkSession):
        super().__init__(cfg)
        self.spark = spark

    def __attrs_post_init__(self):
        if self.spark is None:
            ValueError("Requires spark session")
        self.spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        self.spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    def create_filter_table(self):
        raise NotImplementedError()

    def load_data_parquet(self):
        return (self.spark
                .read
                .schema(self.success_scheme)
                .option("basePath", self.downloaded_images_path)
                .parquet(self.downloaded_images_path + "/ServerName=*/partition_id=*/successes.parquet"))

    def save_filter(self, df: ps.DataFrame):
        if self.filter_name is None:
            raise ValueError("filter name was not defined")
        (df
         .repartition(10)
         .write
         .csv(os.path.join(self.tools_path, self.filter_name, "filter_table"),
              header=True,
              mode="overwrite"))

    def __del__(self):
        self.spark.stop()


@define
class SizeBasedFiltering(SparkFilterToolBase):
    filter_name: str = "size_based"

    def __attrs_pre_init__(self, cfg: Config, spark: SparkSession):
        super().__init__(cfg, spark)

    def create_filter_table(self):
        successes_df: ps.DataFrame = self.load_data_parquet()

        successes_df = (successes_df
                        .withColumn("is_big",
                                    func.array_min(func.col("original_size")) >=
                                    self.config["tools_parameters"]["threshold_size"])
                        .withColumnRenamed("ServerName", "server_name"))

        too_small_images = successes_df.filter(~successes_df["is_big"]).select("uuid",
                                                                               "gbif_id",
                                                                               "server_name",
                                                                               "partition_id")

        self.save_filter(too_small_images)

        self.logger.info(f"Too small images number: {too_small_images.count()}")


@define
class DuplicatesBasedFiltering(SparkFilterToolBase):
    filter_name: str = "duplication_based"

    def __attrs_pre_init__(self, cfg: Config, spark: SparkSession):
        super().__init__(cfg, spark)

    def create_filter_table(self):
        successes_df: ps.DataFrame = self.load_data_parquet()

        not_duplicate_records = (successes_df
                                 .groupBy("hashsum_original")
                                 .count()
                                 .where('count = 1')
                                 .drop('count'))

        duplicate_records = (successes_df
                             .join(not_duplicate_records, on="hashsum_original", how='left_anti')
                             .select("uuid", "gbif_id", "ServerName", "partition_id", "hashsum_original")
                             .withColumnRenamed("ServerName", "server_name"))

        window = ps.Window.partitionBy("hashsum_original").orderBy("partition_id", "server_name")

        duplicate_records_top = (duplicate_records
                                 .withColumn("rn", func.row_number().over(window))
                                 .where("rn == 1")
                                 .drop("rn"))

        duplicate_records_top = duplicate_records_top.withColumnsRenamed(
            {"uuid": "uuid_main",
             "gbif_id": "gbif_id_main",
             "server_name": "server_name_main",
             "partition_id": "partition_id_main"})

        duplicate_records = (duplicate_records
                             .join(duplicate_records_top, on="hashsum_original", how="left")
                             .where("uuid != uuid_main")
                             .drop("hashsum_original")
                             )

        self.save_filter(duplicate_records)

        self.logger.info(f"duplicated number: {duplicate_records.count()}")


@define
class PythonFilterToolBase(FilterToolBase):
    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)

    def get_all_paths_to_merge(self) -> pd.DataFrame:
        all_schedules = []
        path = self.downloaded_images_path
        for folder in os.listdir(path):
            server_name = folder.split("=")[1]
            for partition in os.listdir(f"{path}/{folder}"):
                partition_path = f"{path}/{folder}/{partition}"
                if (not os.path.exists(f"{partition_path}/successes.parquet") or
                        not os.path.exists(f"{partition_path}/completed")):
                    continue
                all_schedules.append([server_name, partition.split("=")[1]])
        return pd.DataFrame(all_schedules, columns=["server_name", "partition_id"])

    def create_filter_table(self):
        filter_table = self.get_all_paths_to_merge()

        filter_table_folder = os.path.join(self.tools_path, self.filter_name, "filter_table")
        os.makedirs(filter_table_folder, exist_ok=True)

        filter_table.to_csv(filter_table_folder + "/table.csv", header=True, index=False)


@define
class ResizeToolFilter(PythonFilterToolBase):
    filter_name = "resize"

    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)


@define
class ImageVerificationToolFilter(PythonFilterToolBase):
    filter_name = "image_verification"

    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)
