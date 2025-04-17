from distributed_downloader.core.initializers.base_initializer import BaseInitializer


class GBIFInitializer(BaseInitializer):
    """
    Initializer for the Global Biodiversity Information Facility (GBIF) dataset.

    This initializer processes the GBIF dataset with the following steps:
    1. Loads the raw dataframe from the specified input path.
    2. Filters out entries that:
       - Don't have a gbifID or identifier
       - Are not of type StillImage or don't contain 'image' in the format field
       - Have 'MATERIAL_CITATION' in the basisOfRecord field
    3. Extracts server names from the identifiers
    4. Generates UUIDs for each entry
    5. Partitions the dataframe based on server names and batch size
    6. Saves the processed dataset to the specified output location
    """

    def run(self):
        """
        Executes the initialization process for the GBIF dataset.
        
        This method performs the complete pipeline of loading, filtering, 
        processing, and saving the GBIF data.
        """
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

        multimedia_df_prep = self.extract_server_name(multimedia_df_prep)
        multimedia_df_prep = self.generate_uuid(multimedia_df_prep)
        master_df_filtered = self.partition_dataframe(multimedia_df_prep)

        self.logger.info("Writing to parquet")
        self.save_results(master_df_filtered)
        self.logger.info("Finished batching")
