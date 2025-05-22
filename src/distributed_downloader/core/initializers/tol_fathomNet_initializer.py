import os

from distributed_downloader.core.initializers.base_initializer import BaseInitializer


class TolFathomNetInitializer(BaseInitializer):
    """
    Initializer for processing FathomNet multimedia data for the Tree of Life project.

    This class filters, deduplicates, and processes multimedia data from FathomNet,
    generates UUIDs, joins with bounding box information, and writes the results to parquet files.
    """

    def run(self):
        """
        Executes the initialization process:
        - Reads multimedia and bounding box data.
        - Filters multimedia data by included sources.
        - Deduplicates entries by source URL.
        - Generates UUIDs for deduplicated entries.
        - Joins multimedia data with bounding box information.
        - Extracts server names and partitions the dataframe.
        - Saves the processed data and UUID reference to parquet files.
        """
        multimedia_df = self.spark.read.parquet(self.input_path)
        included_sources = self.config["included_sources"]
        bbox_information_df = self.spark.read.parquet(self.config["bounding_box_information_path"])

        multimedia_df = multimedia_df.filter(
            multimedia_df["data_source"].isin(included_sources)
        )
        multimedia_df_dedup = multimedia_df.dropDuplicates(["source_url"])
        multimedia_df_dedup = self.generate_uuid(multimedia_df_dedup)
        uuid_ref_df = (
            multimedia_df_dedup.select("uuid", "source_url")
            .withColumnRenamed("uuid", "image_uuid")
            .join(
                multimedia_df.select("uuid", "source_id", "source_url"),
                on="source_url",
                how="inner",
            )
            .drop("source_url")
            .join(bbox_information_df, on="uuid", how="inner")
        )

        master_df = self.extract_server_name(
            multimedia_df_dedup.withColumnRenamed("source_url", "identifier")
        )
        master_df_filtered = self.partition_dataframe(master_df)

        self.logger.info("Writing to parquet")
        self.save_results(master_df_filtered)
        uuid_ref_df.write.parquet(
            os.path.join(self.config.get_folder("tools_folder"), "uuid_ref")
        )
        self.logger.info("Finished batching")
