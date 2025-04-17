import argparse
import os
from collections import deque
from logging import Logger
from typing import Any, Deque, Dict, List

import pandas as pd

from distributed_downloader.core.mpi_downloader.utils import (
    generate_ids_to_download,
    get_largest_nonempty_bucket,
    get_latest_schedule,
    get_schedule_count,
    separate_to_blocks,
)
from distributed_downloader.tools.config import Config
from distributed_downloader.tools.utils import init_logger


def create_new_schedule(config: Config,
                        server_schedule: str,
                        logger: Logger) -> None:
    """
    Creates a new download schedule based on verification results.

    This function orchestrates the download scheduling process by:
    1. Loading server configuration and previous verification results
    2. Identifying which batches still need to be downloaded
    3. Organizing batches into optimal server-specific processing blocks
    4. Distributing the workload across available workers
    5. Creating a new schedule file for the downloader to use

    The scheduling algorithm prioritizes servers with higher process_per_node
    values to maximize throughput and uses a bucket-based allocation to efficiently
    distribute work across available worker processes.

    Args:
        config: Configuration object with download parameters
        server_schedule: Path to the schedule directory
        logger: Logger instance for output messages
    """
    logger.info(f"Creating new schedule for {server_schedule}")

    number_of_workers: int = (config["downloader_parameters"]["max_nodes"]
                              * config["downloader_parameters"]["workers_per_node"])
    server_profiler_path = config.get_folder("profiles_table")

    server_profiler_df = pd.read_csv(server_profiler_path)
    server_config_df = pd.read_csv(f"{server_schedule}/_config.csv")
    server_verifier_df = pd.read_csv(f"{server_schedule}/_verification.csv")

    if os.path.exists(f"{server_schedule}/_DONE"):
        logger.info(f"Schedule {server_schedule} already done")
        return

    # Set up batch ranges for each server
    server_config_df["start_index"] = 0
    server_config_df["end_index"] = 0
    server_config_columns = server_config_df.columns.to_list()
    server_config_df = server_config_df.merge(server_profiler_df,
                                              on="server_name",
                                              how="left",
                                              validate="1:1",
                                              suffixes=("", "_y"))
    server_config_df["end_index"] = server_config_df["total_batches"] - 1
    server_config_df = server_config_df[server_config_columns]

    # Incorporate data from latest schedule if it exists
    latest_schedule = get_latest_schedule(server_schedule)
    if latest_schedule is not None and len(latest_schedule) > 0:
        latest_schedule_aggr = latest_schedule.groupby("server_name").agg(
            {"partition_id_from": "min", "partition_id_to": "max"}).reset_index()
        server_config_df = server_config_df.merge(latest_schedule_aggr, on="server_name", how="left")
        server_config_df = server_config_df.fillna(0)
        server_config_df["start_index"] = server_config_df["partition_id_from"].astype(int)
        server_config_df = server_config_df[server_config_columns]

    # Find which batches still need downloading by comparing with verification results
    batches_to_download: pd.DataFrame = server_config_df.apply(generate_ids_to_download, axis=1,
                                                               args=(server_verifier_df,))
    batches_to_download = batches_to_download.merge(server_config_df, on="server_name", how="left").drop(
        columns=["start_index", "end_index"])
    batches_to_download["batches"] = batches_to_download.apply(separate_to_blocks, axis=1)

    batches_to_download.sort_values(by=["process_per_node", "nodes"], inplace=True, ascending=False)

    # Create buckets based on process_per_node for efficient worker allocation
    ids_to_schedule_in_buckets: Dict[int, Deque[Dict[str, Any]]] = {}
    process_per_nodes = batches_to_download["process_per_node"].unique()
    for process_per_node in process_per_nodes:
        ids_to_schedule_in_buckets[process_per_node] = deque(
            batches_to_download[batches_to_download["process_per_node"] == process_per_node].to_dict("records"))

    logger.info("Filtered out already downloaded batches, creating schedule...")
    logger.debug(ids_to_schedule_in_buckets)

    # Generate schedule by assigning batches to workers
    schedule_list: List[Dict[str, Any]] = []
    worker_id = 0

    while len(ids_to_schedule_in_buckets) != 0:
        worker_id = worker_id % number_of_workers
        largest_key = get_largest_nonempty_bucket(
            ids_to_schedule_in_buckets,
            number_of_workers - worker_id
        )

        if largest_key == 0:
            worker_id = 0
            continue

        # Pop a server from the highest priority bucket
        current_server = ids_to_schedule_in_buckets[largest_key].popleft()
        current_server["nodes"] -= 1
        server_rate_limit = server_profiler_df[server_profiler_df["server_name"] == current_server["server_name"]][
            "rate_limit"].array[0]

        if len(current_server["batches"]) > 0:
            # Schedule batches for this server across multiple workers
            batches_to_schedule = [current_server["batches"].pop(0) for _ in range(current_server["process_per_node"])]
            main_worker_id = worker_id
            for batches in batches_to_schedule:
                for batch in batches:
                    schedule_list.append({
                        "rank": worker_id,
                        "server_name": current_server["server_name"],
                        "partition_id_from": batch[0],
                        "partition_id_to": batch[1],
                        "main_rank": main_worker_id,
                        "rate_limit": server_rate_limit,
                    })
                worker_id += 1

        # Return server to bucket if it still has nodes to allocate
        if current_server["nodes"] > 0:
            ids_to_schedule_in_buckets[largest_key].append(current_server)

        # Remove empty buckets
        if len(ids_to_schedule_in_buckets[largest_key]) == 0:
            del ids_to_schedule_in_buckets[largest_key]

    # Write the new schedule to disk
    schedule_number = get_schedule_count(server_schedule)
    pd.DataFrame(schedule_list).to_csv(f"{server_schedule}/{schedule_number:0=4}.csv", index=False, header=True)

    logger.info(f"Schedule created for {server_schedule}")


def main():
    """
    Main entry point that loads configuration and triggers schedule creation.
    
    This function:
    1. Reads the configuration from the environment
    2. Parses command line arguments to get the schedule path
    3. Initializes a logger
    4. Calls create_new_schedule() to generate the next download schedule
    
    Raises:
        ValueError: If the CONFIG_PATH environment variable is not set
    """
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = Config.from_path(config_path, "downloader")
    logger = init_logger(__name__)

    parser = argparse.ArgumentParser(description='Server downloader controller')
    parser.add_argument("schedule_path", metavar="schedule_path", type=str, help="the path to the schedule")
    _args = parser.parse_args()

    create_new_schedule(
        config,
        _args.schedule_path,
        logger
    )


if __name__ == "__main__":
    main()
