import argparse
import os
from logging import Logger
from typing import Dict

import pandas as pd

from distributed_downloader.mpi_downloader.utils import get_latest_schedule
from distributed_downloader.utils import verify_downloaded_batches
from tools.config import Config
from tools.utils import init_logger


def verify_batches(config: Config,
                   server_schedule: str,
                   logger: Logger) -> None:
    logger.info(f"Verifying batches for {server_schedule}")

    server_urls_downloaded = config.get_folder("images_folder")
    server_profiler_path = config.get_folder("profiles_table")

    config_file: str = f"{server_schedule}/_config.csv"
    verification_file: str = f"{server_schedule}/_verification.csv"

    if not os.path.exists(config_file):
        raise ValueError(f"Schedule config file {config_file} not found")

    if os.path.exists(verification_file):
        verification_df = pd.read_csv(verification_file)
    else:
        verification_df = pd.DataFrame(columns=["ServerName", "PartitionId", "Status"])

    verification_original_df = verification_df.copy()

    server_profiler_df = pd.read_csv(server_profiler_path)

    latest_schedule = get_latest_schedule(server_schedule)
    server_config_df = pd.read_csv(config_file)
    server_config_df["start_index"] = 0
    server_config_df["end_index"] = 0
    server_config_columns = server_config_df.columns.to_list()
    server_config_df = server_config_df.merge(server_profiler_df,
                                              on="server_name",
                                              how="left",
                                              validate="1:1")

    server_config_df["end_index"] = server_config_df["total_batches"] - 1
    server_config_df = server_config_df[server_config_columns]

    if latest_schedule is not None and len(latest_schedule) > 0:
        latest_schedule_aggr = latest_schedule.groupby("server_name").agg(
            {"partition_id_from": "min", "partition_id_to": "max"}).reset_index()
        server_config_df = server_config_df.merge(latest_schedule_aggr, on="server_name", how="left")
        server_config_df["start_index"] = server_config_df["partition_id_from"]
        server_config_df = server_config_df[server_config_columns]

    for idx, row in server_config_df.iterrows():
        new_verification_df = verify_downloaded_batches(row, server_urls_downloaded)
        verification_df = pd.concat([verification_df, pd.DataFrame(new_verification_df)],
                                    ignore_index=True).drop_duplicates()

    verification_df.to_csv(verification_file, index=False, header=True)

    logger.info(f"Verification done for {server_schedule}")

    if (verification_df.equals(verification_original_df)
            and len(verification_df) > 0
            and not os.path.exists(f"{server_schedule}/_DONE")):
        logger.debug(f"Verification unchanged for {server_schedule}")
        open(f"{server_schedule}/_UNCHANGED", "w").close()

    downloaded_count = verification_df.groupby("server_name").agg({"status": "count"}).reset_index()
    downloaded_count = downloaded_count.rename(columns={"status": "downloaded"})
    downloaded_count = downloaded_count.merge(server_config_df, on="server_name", how="outer")
    downloaded_count["downloaded"] = downloaded_count["downloaded"].fillna(0)
    downloaded_count = downloaded_count[["server_name", "downloaded"]]
    downloaded_count = downloaded_count.merge(server_profiler_df,
                                              on="server_name",
                                              how="left")
    downloaded_count = downloaded_count[["server_name", "total_batches", "downloaded"]]
    downloaded_count = downloaded_count[downloaded_count["downloaded"] < downloaded_count["total_batches"]]

    if len(downloaded_count) > 0:
        logger.info(f"Still {len(downloaded_count)} servers have not downloaded all the batches")
    else:
        logger.info("All servers have downloaded all the batches")
        open(f"{server_schedule}/_DONE", "w").close()


def main():
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = Config.from_path(config_path)
    logger = init_logger(__name__)

    parser = argparse.ArgumentParser(description='Server downloader verifier')
    parser.add_argument("schedule_path", metavar="schedule_path", type=str, help="the path to the schedule")
    _args = parser.parse_args()

    verify_batches(
        config,
        _args.schedule_path,
        logger
    )


if __name__ == "__main__":
    main()
