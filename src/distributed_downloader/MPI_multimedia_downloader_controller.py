import argparse
import os
from collections import deque
from logging import Logger
from typing import Any, Dict, List, Deque

import pandas as pd

from distributed_downloader.mpi_downloader.utils import get_latest_schedule, generate_ids_to_download, \
    separate_to_blocks, \
    get_largest_nonempty_bucket, get_schedule_count
from distributed_downloader.utils import load_config, init_logger


def create_new_schedule(config: Dict[str, str | int | bool | Dict[str, int | str]],
                        server_schedule: str,
                        logger: Logger) -> None:
    logger.info(f"Creating new schedule for {server_schedule}")

    number_of_workers: int = (config["downloader_parameters"]["max_nodes"]
                              * config["downloader_parameters"]["workers_per_node"])
    server_profiler_path = os.path.join(config['path_to_output_folder'],
                                        config['output_structure']['profiles_table'])

    server_profiler_df = pd.read_csv(server_profiler_path)
    server_config_df = pd.read_csv(f"{server_schedule}/_config.csv")
    server_verifier_df = pd.read_csv(f"{server_schedule}/_verification.csv")

    if os.path.exists(f"{server_schedule}/_DONE"):
        logger.info(f"Schedule {server_schedule} already done")
        return

    server_config_df["StartIndex"] = 0
    server_config_df["EndIndex"] = 0
    server_config_columns = server_config_df.columns.to_list()
    server_config_df = server_config_df.merge(server_profiler_df,
                                              left_on="ServerName",
                                              right_on="server_name",
                                              how="left",
                                              validate="1:1")
    server_config_df["EndIndex"] = server_config_df["total_batches"] - 1
    server_config_df = server_config_df[server_config_columns]

    latest_schedule = get_latest_schedule(server_schedule)
    if latest_schedule is not None and len(latest_schedule) > 0:
        latest_schedule_aggr = latest_schedule.groupby("ServerName").agg(
            {"PartitionIdFrom": "min", "PartitionIdTo": "max"}).reset_index()
        server_config_df = server_config_df.merge(latest_schedule_aggr, on="ServerName", how="left")
        server_config_df = server_config_df.fillna(0)
        server_config_df["StartIndex"] = server_config_df["PartitionIdFrom"].astype(int)
        server_config_df = server_config_df[server_config_columns]

    batches_to_download: pd.DataFrame = server_config_df.apply(generate_ids_to_download, axis=1,
                                                               args=(server_verifier_df,))
    batches_to_download = batches_to_download.merge(server_config_df, on="ServerName", how="left").drop(
        columns=["StartIndex", "EndIndex"])
    batches_to_download["Batches"] = batches_to_download.apply(separate_to_blocks, axis=1)

    batches_to_download.sort_values(by=["ProcessPerNode", "Nodes"], inplace=True, ascending=False)

    ids_to_schedule_in_buckets: Dict[int, Deque[Dict[str, Any]]] = {}
    process_per_nodes = batches_to_download["ProcessPerNode"].unique()
    for process_per_node in process_per_nodes:
        ids_to_schedule_in_buckets[process_per_node] = deque(
            batches_to_download[batches_to_download["ProcessPerNode"] == process_per_node].to_dict("records"))

    logger.info("Filtered out already downloaded batches, creating schedule...")
    logger.debug(ids_to_schedule_in_buckets)

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

        current_server = ids_to_schedule_in_buckets[largest_key].popleft()
        current_server["Nodes"] -= 1
        server_rate_limit = server_profiler_df[server_profiler_df["server_name"] == current_server["ServerName"]][
            "rate_limit"].array[0]

        if len(current_server["Batches"]) > 0:
            batches_to_schedule = [current_server["Batches"].pop(0) for _ in range(current_server["ProcessPerNode"])]
            main_worker_id = worker_id
            for batches in batches_to_schedule:
                for batch in batches:
                    schedule_list.append({
                        "Rank": worker_id,
                        "ServerName": current_server["ServerName"],
                        "PartitionIdFrom": batch[0],
                        "PartitionIdTo": batch[1],
                        "MainRank": main_worker_id,
                        "RateLimit": server_rate_limit,
                    })
                worker_id += 1

        if current_server["Nodes"] > 0:
            ids_to_schedule_in_buckets[largest_key].append(current_server)

        if len(ids_to_schedule_in_buckets[largest_key]) == 0:
            del ids_to_schedule_in_buckets[largest_key]

    schedule_number = get_schedule_count(server_schedule)
    pd.DataFrame(schedule_list).to_csv(f"{server_schedule}/{schedule_number:0=4}.csv", index=False, header=True)

    logger.info(f"Schedule created for {server_schedule}")


def main():
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = load_config(config_path)
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
