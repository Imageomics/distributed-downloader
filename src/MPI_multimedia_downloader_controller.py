import argparse
import os
from collections import deque
from typing import Any, Dict, List, Deque

import pandas

from mpi_downloader.utils import get_latest_schedule, generate_ids_to_download, separate_to_blocks, \
    get_largest_nonempty_bucket, get_schedule_count

parser = argparse.ArgumentParser(description='Server downloader controller')

parser.add_argument('input_path', metavar='input_path', type=str, help='the path to folder of work')
parser.add_argument("schedule_path", metavar="schedule_path", type=str, help="the path to the schedule")
parser.add_argument('max_nodes', metavar='max_nodes', type=int, help='max number of nodes')
parser.add_argument('max_workers_per_nodes', metavar='max_workers_per_nodes', type=int,
                    help='max number of workers per node')

# parse the arguments
_args = parser.parse_args()
Input_path: str = _args.input_path
Server_schedule: str = _args.schedule_path
Number_of_workers: int = _args.max_nodes * _args.max_workers_per_nodes

Server_urls_downloaded = f"{Input_path}/downloaded_images"
# Server_profiler_df = pandas.DataFrame(np.array(h5py.File(f"{Input_path}/servers_profiles.hdf5", 'r')['profiles']),
#                                       columns=profile_dtype.names)
# Server_profiler_df["server_name"] = Server_profiler_df["server_name"].astype(str)

Server_profiler_df = pandas.read_csv(f"{Input_path}/servers_profiles.csv")

Server_verifier_df = pandas.read_csv(f"{Server_schedule}/_verification.csv")

Server_config_df = pandas.read_csv(f"{Server_schedule}/_config.csv")
Server_config_df["StartIndex"] = 0
Server_config_df["EndIndex"] = 0
server_config_columns = Server_config_df.columns.to_list()
Server_config_df = Server_config_df.merge(Server_profiler_df, left_on="ServerName", right_on="server_name", how="left")
Server_config_df["EndIndex"] = Server_config_df["total_batches"] - 1
Server_config_df = Server_config_df[server_config_columns]

Latest_schedule = get_latest_schedule(Server_schedule)
if Latest_schedule is not None and len(Latest_schedule) > 0:
    Latest_schedule_aggr = Latest_schedule.groupby("ServerName").agg(
        {"PartitionIdFrom": "min", "PartitionIdTo": "max"}).reset_index()
    Server_config_df = Server_config_df.merge(Latest_schedule_aggr, on="ServerName", how="left")
    Server_config_df = Server_config_df.fillna(0)
    Server_config_df["StartIndex"] = Server_config_df["PartitionIdFrom"].astype(int)
    Server_config_df = Server_config_df[server_config_columns]

batches_to_download: pandas.DataFrame = Server_config_df.apply(generate_ids_to_download, axis=1,
                                                               args=(Server_verifier_df,))
batches_to_download = batches_to_download.merge(Server_config_df, on="ServerName", how="left").drop(
    columns=["StartIndex", "EndIndex"])
batches_to_download["Batches"] = batches_to_download.apply(separate_to_blocks, axis=1)

batches_to_download.sort_values(by=["ProcessPerNode", "Nodes"], inplace=True, ascending=False)

ids_to_schedule_in_buckets: Dict[int, Deque[Dict[str, Any]]] = {}
process_per_nodes = batches_to_download["ProcessPerNode"].unique()
for process_per_node in process_per_nodes:
    ids_to_schedule_in_buckets[process_per_node] = deque(
        batches_to_download[batches_to_download["ProcessPerNode"] == process_per_node].to_dict("records"))

print(ids_to_schedule_in_buckets)

schedule_list: List[Dict[str, Any]] = []
worker_id = 0

while len(ids_to_schedule_in_buckets) != 0:
    worker_id = worker_id % Number_of_workers
    largest_key = get_largest_nonempty_bucket(
        ids_to_schedule_in_buckets,
        Number_of_workers - worker_id
    )

    if largest_key == 0:
        worker_id = 0
        continue

    current_server = ids_to_schedule_in_buckets[largest_key].popleft()
    current_server["Nodes"] -= 1
    server_rate_limit = Server_profiler_df[Server_profiler_df["server_name"] == current_server["ServerName"]][
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

schedule_number = get_schedule_count(Server_schedule)
pandas.DataFrame(schedule_list).to_csv(f"{Server_schedule}/{schedule_number:0=4}.csv", index=False, header=True)
