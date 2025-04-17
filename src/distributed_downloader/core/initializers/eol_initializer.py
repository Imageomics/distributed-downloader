from distributed_downloader.core.initializers.base_initializer import BaseInitializer


class EoLInitializer(BaseInitializer):
    """
    Initializer for the Encyclopedia of Life (EoL) dataset.

    This initializer processes the EoL dataset with the following steps:
    1. Loads the raw dataframe from the specified input path.
    2. Filters out entries that don't have an 'EOL content ID' or 'EOL Full-Size Copy URL'.
    3. Renames columns to match the downloader schema:
       - 'EOL content ID' -> 'source_id'
       - 'EOL Full-Size Copy URL' -> 'identifier'
       - 'License Name' -> 'license'
       - 'Copyright Owner' -> 'owner'
    4. Extracts server names from the identifiers
    5. Generates UUIDs for each entry
    6. Partitions the dataframe based on server names and batch size
    7. Saves the processed dataset to the specified output location
    """

    def run(self):
        """
        Executes the initialization process for the Encyclopedia of Life dataset.
        
        This method performs the complete pipeline of loading, filtering, 
        processing, and saving the EoL data.
        """
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

        multimedia_df_prep = self.extract_server_name(multimedia_df_prep)
        multimedia_df_prep = self.generate_uuid(multimedia_df_prep)
        master_df_filtered = self.partition_dataframe(multimedia_df_prep)
        self.logger.info("Writing to parquet")
        self.save_results(master_df_filtered)
        self.logger.info("Finished batching")
