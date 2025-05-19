from distributed_downloader.core import BaseInitializer


class TolGeneralInitializer(BaseInitializer):
    """
    Initializer for TOL general data processing.
    Reads multimedia data, filters by included sources and excluded servers,
    partitions the dataframe, and saves the results.
    """

    def run(self):
        """
        Executes the initialization process:
        - Reads multimedia parquet data.
        - Reads excluded servers from CSV.
        - Extracts server names from multimedia data.
        - Filters multimedia data by included sources and excluded servers.
        - Partitions the filtered dataframe.
        - Saves the results to parquet.
        """
        multimedia_df = self.spark.read.parquet(self.input_path)
        included_sources = self.config["included_sources"]
        if self.config.get("excluded_servers_path") in ["", None]:
            excluded_servers = self.spark.createDataFrame([], "server_name: string")
        else:
            excluded_servers = self.spark.read.csv(
                self.config["excluded_servers_path"], header=True
            )

        multimedia_df = self.extract_server_name(multimedia_df.withColumnRenamed("source_url", "identifier"))
        multimedia_df = multimedia_df.filter(
            multimedia_df["data_source"].isin(included_sources)
        ).filter(~multimedia_df["server_name"].isin(excluded_servers["server_name"]))
        master_df_filtered = self.partition_dataframe(multimedia_df)

        self.logger.info("Writing to parquet")
        self.save_results(master_df_filtered)
        self.logger.info("Finished batching")
