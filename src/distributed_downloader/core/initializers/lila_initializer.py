import pyspark.sql.functions as func

from distributed_downloader.core.initializers.base_initializer import BaseInitializer


class LilaInitializer(BaseInitializer):
    def run(self):
        multimedia_df = self.load_raw_df()

        multimedia_df_prep = (
            multimedia_df.filter(
                (
                        multimedia_df["url_gcp"].isNotNull()
                        | multimedia_df["url_aws"].isNotNull()
                        | multimedia_df["url_azure"].isNotNull()
                )
                & (multimedia_df["original_label"] != "empty")
            )
            .repartition(20)
            .withColumn(
                "identifier",
                func.when(
                    multimedia_df["url_gcp"].isNotNull(), multimedia_df["url_gcp"]
                ).otherwise(
                    func.when(
                        multimedia_df["url_aws"].isNotNull(), multimedia_df["url_aws"]
                    ).otherwise(multimedia_df["url_azure"])
                ),
            )
            .withColumnsRenamed({"image_id": "source_id"})
        )

        multimedia_df_prep = self.extract_server_name(multimedia_df_prep)
        multimedia_df_prep = self.generate_uuid(multimedia_df_prep)
        master_df_filtered = self.partition_dataframe(multimedia_df_prep)
        self.logger.info("Writing to parquet")
        self.save_results(master_df_filtered)
        self.logger.info("Finished batching")
