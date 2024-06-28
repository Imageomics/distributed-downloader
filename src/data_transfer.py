import concurrent.futures
import glob
import os
import shutil
import time
import uuid
from typing import List, Tuple

import pandas as pd

from distributed_downloader.utils import init_logger

copy_from = "/fs/scratch/PAS2136/gbif/processed/2024-05-01/multimedia_prep/downloaded_images"
copy_to = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/filtered_out/normal_images_copy"
parquet_name = "successes.parquet"
schedule_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/tools/data_transfer"
verification_folder = f"{schedule_folder}/verification"
os.makedirs(verification_folder, exist_ok=True)
logger = init_logger(__name__)
total_time = 30


# max_nodes = 1
# max_workers_per_node = 40
# total_workers = max_nodes * max_workers_per_node
# speed = 0.105


def is_enough_time() -> None:
    if time.time() > int(os.getenv("SLURM_JOB_END_TIME", 0)) - total_time:
        raise TimeoutError("Not enough time")


def load_table(folder: str, columns: List[str] = None) -> pd.DataFrame:
    all_files = glob.glob(os.path.join(folder, "*.csv"))
    if len(all_files) == 0:
        if columns is None:
            raise ValueError("No files found and columns are not defined")
        return pd.DataFrame(columns=columns)
    return pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)


def create_filename_dict(src_path: str,
                         dst_path: str,
                         verification_path: str) \
        -> Tuple[List[Tuple[str, str]], List[str]]:
    verification_df = load_table(verification_path, ["server_name", "partition_id"])
    all_schedules = []
    all_server_folders = []
    for folder in os.listdir(src_path):
        all_server_folders.append(folder)
        server_name = folder.split("=")[1]
        for partition in os.listdir(f"{src_path}/{folder}"):
            partition_path = f"{src_path}/{folder}/{partition}"
            if (not os.path.exists(f"{partition_path}/successes.parquet") or
                    not os.path.exists(f"{partition_path}/completed")):
                continue
            all_schedules.append([server_name, partition.split("=")[1]])
            # all_schedules.append((f"{partition_path}/successes.parquet",
            #                       f"{dst_path}/{folder}/{str(uuid.uuid4())}.parquet"))
    schedule_df = pd.DataFrame(all_schedules, columns=["server_name", "partition_id"])
    outer_join = schedule_df.merge(verification_df, how='outer', indicator=True, on=["server_name", "partition_id"])
    leftover_df = outer_join[(outer_join["_merge"] == 'left_only')].drop('_merge', axis=1)

    leftover_df["path_from"] = src_path + "/ServerName=" + leftover_df["server_name"] + "/partition_id=" + leftover_df["partition_id"] + "/" + parquet_name
    return schedules, all_server_folders


def create_server_folders(dst_root: str, servers: List[str]) -> None:
    for server in servers:
        os.makedirs(os.path.join(dst_root, server), exist_ok=True)


def copy_file(file_dict: Tuple[str, str]) -> Tuple[str, str]:
    shutil.copy2(file_dict[0], file_dict[1])
    return file_dict
    # logger.debug(f"Copied file {file_dict[0]} to {file_dict[1]}")


def convert_time(total_seconds: float) -> Tuple[float, float, float]:
    _hours = total_seconds // 3600
    if _hours != 0:
        total_seconds %= 60
    _minutes = total_seconds // 60
    if _minutes != 0:
        total_seconds %= 60
    return _hours, _minutes, total_seconds


if __name__ == "__main__":
    filename_dict, server_folders = create_filename_dict(copy_from, copy_to)
    create_server_folders(copy_to, server_folders)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for src, dst in executor.map(copy_file, filename_dict):
            logger.debug(f"Copied file {src} to {dst}")

    logger.info("Finished copying")
