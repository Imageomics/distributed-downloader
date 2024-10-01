import os.path

import pyspark.sql.functions as func
from pyspark.sql import SparkSession, Window

from distributed_downloader.core.initialization import get_server_name, get_uuid
from distributed_downloader.tools.config import Config
from distributed_downloader.tools.utils import load_dataframe, init_logger

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

    multimedia_df_prep = (
        multimedia_df
        .filter((multimedia_df["EOL content ID"].isNotNull())
                & (multimedia_df["EOL Full-Size Copy URL"].isNotNull()))
        .repartition(20)
        .withColumnsRenamed(
            {
                "EOL content ID": "source_id",
                "EOL Full-Size Copy URL": "identifier",
                "License Name": "license",
                "Copyright Owner": "owner"
            })
    )

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
