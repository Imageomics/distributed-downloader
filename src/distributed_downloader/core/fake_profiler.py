import os

import pandas as pd

from distributed_downloader.core.mpi_downloader.dataclasses import profile_dtype
from distributed_downloader.tools.config import Config


def main():
    """
    Generates a profiling table for servers with default rate limits.

    Background:
    -----------
    The original concept was to dynamically profile server performance by:
    - Running test downloads to measure actual download speeds
    - Identifying response times and download capacity before throttling occurs
    - Filtering out non-responsive or problematic servers

    Current Implementation:
    ----------------------
    This simpler approach was adopted because comprehensive profiling:
    - Is time-consuming to execute
    - Isn't compatible with the current downloader architecture (lacks dynamic
      allocation of new downloaders when bandwidth permits)

    Current behavior:
    1. Counts available download partitions for each server
    2. Assigns a default rate limit (from config) to all servers
    3. Creates a profiles table CSV with this information

    Future Direction:
    ----------------
    1. Move partition counting to initialization phase
    2. Implement controller-worker downloader architecture to:
       - Dynamically adjust speeds based on real-time performance
       - Vary the number of concurrent downloaders per server
       - Self-regulate without needing a separate profiling step
    3. Allow initial speed/concurrency suggestions to optimize early performance
    """
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = Config.from_path(config_path, "downloader")

    # Get parameters from config
    _DEFAULT_RATE_LIMIT: int = config["downloader_parameters"]["default_rate_limit"]
    server_urls_batched: str = config.get_folder("urls_folder")
    server_profiler_csv: str = config.get_folder("profiles_table")

    # Perform profiling
    server_list = os.listdir(server_urls_batched)

    profile_csv = []
    for i, server in enumerate(server_list):
        if not os.path.isdir(f"{server_urls_batched}/{server}"):
            continue

        server_name = server.split("=")[1]
        server_total_partitions = len(os.listdir(f"{server_urls_batched}/{server}"))
        profile_csv.append([server_name, server_total_partitions, 0, 0, _DEFAULT_RATE_LIMIT])

    profiles_df = pd.DataFrame(profile_csv, columns=profile_dtype.names)
    profiles_df.to_csv(server_profiler_csv, index=False, header=True)


if __name__ == "__main__":
    main()
