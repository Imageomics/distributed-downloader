import pyspark.sql.functions as func

from distributed_downloader.core.initializers.base_initializer import BaseInitializer


class LilaInitializer(BaseInitializer):
    """
    Initializer for the Labeled Information Library of Alexandria (Lila) dataset.

    This initializer processes the Lila dataset with the following steps:
    1. Loads the raw dataframe from the specified input path.
    2. Filters out entries that:
       - Don't have a URL value (checking url_gcp, url_aws, and url_azure columns)
       - Have "empty" as the original_label
    3. Creates an 'identifier' column from available URL columns with the following priority:
       url_gcp -> url_aws -> url_azure
    4. Renames 'image_id' to 'source_id'
    5. Extracts server names from the identifiers
    6. Generates UUIDs for each entry
    7. Partitions the dataframe based on server names and batch size
    8. Saves the processed dataset to the specified output location
    """

    def run(self):
        """
        Executes the initialization process for the Lila dataset.
        
        This method performs the complete pipeline of loading, filtering, 
        processing, and saving the Lila data.
        """
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
