from distributed_downloader.core.initializers.base_initializer import BaseInitializer


class EoLInitializer(BaseInitializer):
    def run(self):
        multimedia_df = self.load_raw_df()

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

        multimedia_df_prep = multimedia_df_prep.withColumn(
            "server_name", self.get_server_name(multimedia_df_prep.identifier)
        )
        multimedia_df_prep = multimedia_df_prep.withColumn("UUID", self.get_uuid())

        master_df_filtered = self.partition_dataframe(multimedia_df_prep)

        self.logger.info("Writing to parquet")

        self.save_results(master_df_filtered)

        self.logger.info("Finished batching")
