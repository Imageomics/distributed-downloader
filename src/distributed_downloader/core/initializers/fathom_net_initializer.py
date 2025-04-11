import pyspark.sql.functions as func

from distributed_downloader.core.initializers.base_initializer import BaseInitializer


class FathomNetInitializer(BaseInitializer):
    """
    Initializer for the FathomNet dataset.

    This initializer processes the FathomNet dataset with the following steps:
    1. Loads the raw dataframe from the specified input path.
    2. Filters out entries that:
       - Don't have a uuid or url
       - Are not valid (based on the 'valid' column)
    3. Renames columns to match the downloader schema:
       - 'uuid' -> 'source_id'
       - 'url' -> 'identifier'
    4. Adds license information to each entry
    5. Extracts server names from the identifiers
    6. Generates UUIDs for each entry
    7. Partitions the dataframe based on server names and batch size
    8. Saves the processed dataset to the specified output location
    """

    def run(self):
        """
        Executes the initialization process for the FathomNet dataset.
        
        This method performs the complete pipeline of loading, filtering, 
        processing, and saving the FathomNet data.
        """
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

        multimedia_df_prep = self.extract_server_name(multimedia_df_prep)
        multimedia_df_prep = self.generate_uuid(multimedia_df_prep)
        master_df_filtered = self.partition_dataframe(multimedia_df_prep)
        self.logger.info(f"Writing to parquet: {self.output_path}")
        self.save_results(master_df_filtered)
        self.logger.info("Finished batching")
