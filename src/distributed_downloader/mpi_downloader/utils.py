from __future__ import annotations

import logging
import os
import shutil
import time
from typing import Dict, Tuple, Union, List, Any, Deque, Set

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from .Downloader import Downloader
from .dataclasses import RateLimit


def create_new_session(url: str, max_rate: int) -> requests.Session:
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=max_rate, pool_connections=max_rate)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.mount(url, adapter)
    return session


def truncate_folder(path: str):
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)


def truncate_server_folders(path: str) -> None:
    for file in os.listdir(path):
        if os.path.isdir(f"{path}/{file}") and "ServerName" in file:
            shutil.rmtree(f"{path}/{file}", ignore_errors=True)


def get_latest_schedule(path_to_dir: str, rank: int = None) -> Union[pd.DataFrame, None]:
    if not os.path.exists(path_to_dir) or not os.path.isdir(path_to_dir):
        return None

    latest_schedule_file = [file for file in os.listdir(path_to_dir) if not file.startswith("_")]

    if len(latest_schedule_file) == 0:
        return None

    latest_schedule_file = sorted(latest_schedule_file, reverse=True)[0]

    latest_schedule_df = pd.read_csv(f"{path_to_dir}/{latest_schedule_file}")

    if rank is not None:
        return latest_schedule_df[latest_schedule_df["Rank"] == rank]
    return latest_schedule_df


def get_or_init_downloader(header: dict,
                           img_size: int,
                           schedule_dict: Dict[str, str],
                           downloader_schedule: Dict[str, Tuple],
                           rate_multiplier: float,
                           job_end_time: int,
                           logger: logging.Logger) -> Tuple[Downloader, requests.Session, RateLimit]:
    if schedule_dict["ServerName"] not in downloader_schedule.keys():
        server_name = schedule_dict["ServerName"].replace("%3A", ":")
        rate_limit = RateLimit(schedule_dict["RateLimit"], rate_multiplier)
        session = create_new_session(server_name, rate_limit.upper_bound)
        downloader = Downloader(header, session, rate_limit, img_size, job_end_time=job_end_time, logger=logger)
        downloader_schedule[schedule_dict["ServerName"]] = (downloader, session, rate_limit)

    downloader, session, rate_limit = downloader_schedule[schedule_dict["ServerName"]]
    return downloader, session, rate_limit


def generate_ids_to_download(schedule_row: pd.Series, verifier_df: pd.DataFrame) -> pd.Series:
    server_name = schedule_row["ServerName"]
    server_start_idx = schedule_row["StartIndex"]
    server_end_idx = schedule_row["EndIndex"]

    server_batches: Set[int] = set(range(server_start_idx, server_end_idx + 1))
    # max_batch_idx = 0

    verifier_df = verifier_df[
        (verifier_df["ServerName"] == server_name) & (verifier_df["PartitionId"] >= server_start_idx) & (
                verifier_df["PartitionId"] <= server_end_idx)]
    verifier_set = set(verifier_df["PartitionId"])

    server_batches = server_batches - verifier_set

    # server_batches.extend(range(max_batch_idx, server_end_idx + 1))
    return pd.Series([server_name, list(server_batches)], index=["ServerName", "Batches"])


def verify_batches_for_prep(schedule_row: pd.DataFrame, input_path: str) -> pd.DataFrame:
    schedule_row["ServerName"] = schedule_row["server_name"]
    schedule_row["StartIndex"] = 0
    schedule_row["EndIndex"] = schedule_row["total_batches"]

    verification_df = pd.DataFrame(columns=["ServerName", "PartitionId", "Status"])

    for idx, row in schedule_row.iterrows():
        new_verification_df = verify_downloaded_batches(row, input_path)
        verification_df = pd.concat([verification_df, pd.DataFrame(new_verification_df)],
                                    ignore_index=True).drop_duplicates()

    return verification_df


def verify_downloaded_batches(schedule_row: pd.Series, input_path: str) -> List[Dict[str, Any]]:
    server_name = schedule_row["ServerName"]
    server_start_idx = schedule_row["StartIndex"]
    server_end_idx = schedule_row["EndIndex"]
    verified_batches: List[Dict[str, Any]] = []

    if os.path.exists(f"{input_path}/ServerName={server_name}"):
        server_batches_names = os.listdir(f"{input_path}/ServerName={server_name}")
        for batch_name in server_batches_names:
            if not os.path.isdir(f"{input_path}/ServerName={server_name}/{batch_name}"):
                continue

            batch_idx = int(batch_name.split("=")[1])
            if server_start_idx > batch_idx or server_end_idx < batch_idx:
                continue

            if os.path.exists(f"{input_path}/ServerName={server_name}/{batch_name}/completed"):
                verified_batches.append({"ServerName": server_name, "PartitionId": batch_idx, "Status": "Completed"})
            elif os.path.exists(f"{input_path}/ServerName={server_name}/{batch_name}/failed"):
                verified_batches.append({"ServerName": server_name, "PartitionId": batch_idx, "Status": "Failed"})

    return verified_batches
    # if len(verified_batches) == 0:
    #     return pd.DataFrame(columns=["ServerName", "PartitionId", "Status"])
    # return pd.DataFrame(verified_batches)


def separate_to_blocks(data_row: pd.Series) -> List[List[Tuple[int, int]]]:
    batches: List[int] = data_row["Batches"]
    num_of_blocks: int = data_row["ProcessPerNode"] * data_row["Nodes"]

    blocks: List[List[Tuple[int, int]]] = []
    if len(batches) < 1:
        return blocks

    if len(batches) <= num_of_blocks:
        for batch in batches:
            blocks.append([(batch, batch + 1)])
        return blocks

    batch_per_block = len(batches) // num_of_blocks
    for i in range(num_of_blocks - 1):
        blocks.append(compress_ids(batches[i * batch_per_block: (i + 1) * batch_per_block]))
    blocks.append(compress_ids(batches[(num_of_blocks - 1) * batch_per_block:]))

    return blocks


def compress_ids(ids: List[int]) -> List[Tuple[int, int]]:
    if len(ids) < 1:
        return []
    compressed_ids = []
    start = ids[0]
    end = ids[0]
    for i in range(1, len(ids)):
        if ids[i] - end == 1:
            end = ids[i]
        else:
            compressed_ids.append((start, end + 1))
            start = ids[i]
            end = ids[i]
    compressed_ids.append((start, end + 1))
    return compressed_ids


def get_largest_nonempty_bucket(buckets: Dict[int, Deque[Dict[str, Any]]], avail_space: int) -> int:
    largest_bucket = 0

    for key, bucket in buckets.items():
        if key > avail_space or len(bucket) == 0:
            continue
        largest_bucket = max(largest_bucket, key)

    return largest_bucket


def is_enough_time(rate_limit: RateLimit, batch_size: int = 10000, avg_write_time: int = 600, job_end_time: int = int(os.getenv("SLURM_JOB_END_TIME", 0))) -> bool:
    current_time = time.time()

    # print(f"{current_time}|{job_end_time}")

    time_left = job_end_time - current_time - avg_write_time
    return rate_limit.initial_rate * time_left >= batch_size


def get_schedule_count(path_to_dir: str) -> int:
    schedule_files = [file for file in os.listdir(path_to_dir) if not file.startswith("_")]
    return len(schedule_files)
