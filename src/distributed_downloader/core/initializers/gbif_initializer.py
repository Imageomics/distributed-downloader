from distributed_downloader.core.initializers.base_initializer import BaseInitializer


class GBIFInitializer(BaseInitializer):
    def run(self):
        multimedia_df = self.load_raw_df()

        multimedia_df_prep = multimedia_df.filter(
            (multimedia_df["gbifID"].isNotNull())
            & (multimedia_df["identifier"].isNotNull())
            & (
                    (multimedia_df["type"] == "StillImage")
                    | (
                            (multimedia_df["type"].isNull())
                            & (multimedia_df["format"].contains("image"))
                    )
            )
            & ~(multimedia_df["basisOfRecord"].contains("MATERIAL_CITATION"))
        ).repartition(20)

        multimedia_df_prep = multimedia_df_prep.withColumn(
            "server_name", self.get_server_name(multimedia_df_prep.identifier)
        )
        multimedia_df_prep = multimedia_df_prep.withColumn("UUID", self.get_uuid())

        master_df_filtered = self.partition_dataframe(multimedia_df_prep)

        self.logger.info("Writing to parquet")

        self.save_results(master_df_filtered)

        self.logger.info("Finished batching")
