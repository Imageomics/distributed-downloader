import pyspark.sql.functions as func

from distributed_downloader.core.initializers.base_initializer import BaseInitializer


class FathomNetInitializer(BaseInitializer):
    def run(self):
        multimedia_df = self.load_raw_df()

        multimedia_df_prep = (
            multimedia_df.filter(
                (multimedia_df["uuid"].isNotNull())
                & (multimedia_df["url"].isNotNull())
                & (multimedia_df["valid"].cast("boolean"))
            )
            .repartition(20)
            .withColumnsRenamed({"uuid": "source_id", "url": "identifier"})
            .withColumn(
                "license",
                func.lit(
                    "https://creativecommons.org/licenses/by-nc-nd/4.0/legalcode.en"
                ),
            )
        )

        multimedia_df_prep = multimedia_df_prep.withColumn(
            "server_name", self.get_server_name(multimedia_df_prep.identifier)
        )
        multimedia_df_prep = multimedia_df_prep.withColumn("uuid", self.get_uuid())

        master_df_filtered = self.partition_dataframe(multimedia_df_prep)

        self.logger.info(f"Writing to parquet: {self.output_path}")

        self.save_results(master_df_filtered)

        self.logger.info("Finished batching")
