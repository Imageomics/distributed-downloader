import argparse
import os
import shutil

import pandas as pd
from pandas._libs.missing import NAType

from mpi_downloader.dataclasses import profile_dtype
from mpi_downloader.utils import verify_batches_for_prep
from distributed_downloader.utils import ensure_created, create_schedule_configs

_DEFAULT_RATE_LIMIT = 10
_CREATE_PROFILES = False
_DOWNLOADER_URLS_FOLDER = os.getenv("DOWNLOADER_URLS_FOLDER", "servers_batched")
_DOWNLOADER_LOGS_FOLDER = os.getenv("DOWNLOADER_LOGS_FOLDER", "logs")
_DOWNLOADER_IMAGES_FOLDER = os.getenv("DOWNLOADER_IMAGES_FOLDER", "downloaded_images")
_DOWNLOADER_SCHEDULES_FOLDER = os.getenv("DOWNLOADER_SCHEDULES_FOLDER", "schedules")
_DOWNLOADER_PROFILES_PATH = os.getenv("DOWNLOADER_PROFILES_PATH", "servers_profiles.csv")
_DOWNLOADER_IGNORED_PATH = os.getenv("DOWNLOADER_IGNORED_PATH", "ignored_servers.csv")


def small_rule(total_batches: int) -> int | NAType:
    if total_batches >= 5000:
        return 40
    elif total_batches >= 1000:
        return 20
    elif total_batches >= 500:
        return 10
    elif total_batches >= 200:
        return 8
    elif total_batches >= 100:
        return 4
    elif total_batches >= 50:
        return 2
    elif total_batches >= 1:
        return 1

    return pd.NA


parser = argparse.ArgumentParser(description='Server downloader prep')

parser.add_argument('input_path', metavar='input_path', type=str, help='the path to folder with download components (e.g., image folder, server profiles, and schedule)')
parser.add_argument('max_nodes', metavar='max_nodes', type=int, help='the max number of nodes to use for download')
parser.add_argument('max_workers_per_nodes', metavar='max_workers_per_nodes', type=int,
                    help='the max number of workers per node to use for download')

# parse the arguments
_args = parser.parse_args()
Input_path: str = _args.input_path
Number_of_workers: int = _args.max_nodes * _args.max_workers_per_nodes

Server_urls_batched = f"{Input_path}/{_DOWNLOADER_URLS_FOLDER}"
Server_profiler_csv = f"{Input_path}/{_DOWNLOADER_PROFILES_PATH}"
Server_ignored_csv = f"{Input_path}/{_DOWNLOADER_IGNORED_PATH}"
Server_schedules_path = f"{Input_path}/{_DOWNLOADER_SCHEDULES_FOLDER}"

ensure_created([
    Server_urls_batched,
    f"{Input_path}/{_DOWNLOADER_LOGS_FOLDER}",
    f"{Input_path}/{_DOWNLOADER_IMAGES_FOLDER}",
    Server_schedules_path,
])

server_list = os.listdir(Server_urls_batched)
server_count = len(server_list)

profile_csv = []
for i, server in enumerate(server_list):
    if not os.path.isdir(f"{Server_urls_batched}/{server}"):
        continue

    server_name = server.split("=")[1]
    server_total_partitions = len(os.listdir(f"{Server_urls_batched}/{server}"))
    profile_csv.append([server_name, server_total_partitions, 0, 0, _DEFAULT_RATE_LIMIT])

profiles_df = pd.DataFrame(profile_csv, columns=profile_dtype.names)
if _CREATE_PROFILES:
    profiles_df.to_csv(Server_profiler_csv, index=False, header=True)

if os.path.exists(Server_ignored_csv):
    ignored_servers_df = pd.read_csv(Server_ignored_csv)
else:
    ignored_servers_df = pd.DataFrame(columns=["ServerName"])

if len(os.listdir(f"{Input_path}/{_DOWNLOADER_IMAGES_FOLDER}")) > 0:
    downloaded_batches: pd.DataFrame = verify_batches_for_prep(profiles_df, f"{Input_path}/{_DOWNLOADER_IMAGES_FOLDER}")
    downloaded_batches = downloaded_batches.groupby("ServerName").count().reset_index().dropna()
    downloaded_batches = downloaded_batches.rename(columns={"ServerName": "server_name", "Status": "already_downloaded"})
    profiles_df = profiles_df.merge(downloaded_batches, on="server_name", how="left").fillna(0)
    profiles_df["left_to_download"] = profiles_df["total_batches"] - profiles_df["already_downloaded"]
else:
    profiles_df["left_to_download"] = profiles_df["total_batches"]

profiles_df["Nodes"] = profiles_df["left_to_download"].apply(small_rule)
profiles_df["ProcessPerNode"] = 1
profiles_df = profiles_df.rename(columns={"total_batches": "TotalBatches"}).dropna().reset_index(drop=True)
profiles_df = profiles_df[["ServerName", "TotalBatches", "ProcessPerNode", "Nodes"]]
profiles_df = profiles_df[~profiles_df["ServerName"].isin(ignored_servers_df["ServerName"])]

shutil.rmtree(Server_schedules_path, ignore_errors=True)
os.makedirs(Server_schedules_path, exist_ok=True)

create_schedule_configs(profiles_df, Number_of_workers, Server_schedules_path)

print("Done")
