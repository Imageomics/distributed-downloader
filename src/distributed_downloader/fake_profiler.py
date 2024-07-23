import os

import pandas as pd

from distributed_downloader.mpi_downloader.dataclasses import profile_dtype
from tools.config import Config


def main():
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = Config.from_path(config_path)

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
