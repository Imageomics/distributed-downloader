from __future__ import annotations

"""
Utility functions for the MPI-based distributed downloader.

This module provides helper functions for the downloader system, including:
- Session management for HTTP requests
- Schedule handling and parsing
- Downloader initialization and reuse
- Batch processing utilities
- Time management for job execution
"""

import logging
import os
import time
from typing import Any, Deque, Dict, List, Set, Tuple, Union

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from .dataclasses import RateLimit
from .Downloader import Downloader


def create_new_session(url: str, max_rate: int) -> requests.Session:
    """
    Create a new HTTP session with retry logic and connection pooling.
    
    Args:
        url: Base URL for the server
        max_rate: Maximum number of concurrent connections in the pool
        
    Returns:
        requests.Session: Configured session with retry and pooling settings
    """
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=max_rate, pool_connections=max_rate)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.mount(url, adapter)
    return session


def get_latest_schedule(path_to_dir: str, rank: int = None) -> Union[pd.DataFrame, None]:
    """
    Get the most recent schedule file from a directory.
    
    Args:
        path_to_dir: Directory containing schedule files
        rank: Optional MPI rank to filter the schedule for a specific worker
        
    Returns:
        pd.DataFrame or None: DataFrame containing schedule information, or None if no schedules found
    """
    if not os.path.exists(path_to_dir) or not os.path.isdir(path_to_dir):
        return None

    latest_schedule_file = [file for file in os.listdir(path_to_dir) if not file.startswith("_")]

    if len(latest_schedule_file) == 0:
        return None

    latest_schedule_file = sorted(latest_schedule_file, reverse=True)[0]

    latest_schedule_df = pd.read_csv(f"{path_to_dir}/{latest_schedule_file}")

    if rank is not None:
        return latest_schedule_df[latest_schedule_df["rank"] == rank]
    return latest_schedule_df


def get_or_init_downloader(header: dict,
                           img_size: int,
                           schedule_dict: Dict[str, str],
                           downloader_schedule: Dict[str, Tuple],
                           rate_multiplier: float,
                           job_end_time: int,
                           logger: logging.Logger) -> Tuple[Downloader, requests.Session, RateLimit]:
    """
    Get an existing downloader for a server or initialize a new one.
    
    This function maintains a cache of downloaders per server to avoid
    creating multiple connections to the same server.
    
    Args:
        header: HTTP headers to use for requests
        img_size: Target size for image resizing
        schedule_dict: Dictionary containing server information and rate limits
        downloader_schedule: Cache of existing downloaders
        rate_multiplier: Rate limit adjustment multiplier
        job_end_time: Unix timestamp when the job should end
        logger: Logger instance for output
        
    Returns:
        Tuple[Downloader, requests.Session, RateLimit]: 
            The downloader, its session, and the rate limiter
    """
    if schedule_dict["server_name"] not in downloader_schedule.keys():
        server_name = schedule_dict["server_name"].replace("%3A", ":")
        rate_limit = RateLimit(schedule_dict["rate_limit"], rate_multiplier)
        session = create_new_session(server_name, rate_limit.upper_bound)
        downloader = Downloader(header, session, rate_limit, img_size, job_end_time=job_end_time, logger=logger)
        downloader_schedule[schedule_dict["server_name"]] = (downloader, session, rate_limit)

    downloader, session, rate_limit = downloader_schedule[schedule_dict["server_name"]]
    return downloader, session, rate_limit


def generate_ids_to_download(schedule_row: pd.Series, verifier_df: pd.DataFrame) -> pd.Series:
    """
    Determine which batch IDs need to be downloaded for a server.
    
    This function compares the total range of batches for a server with
    those that have already been verified as downloaded, and returns
    the difference (batches that still need to be downloaded).
    
    Args:
        schedule_row: Row from schedule DataFrame with server information
        verifier_df: DataFrame containing verification status of downloaded batches
        
    Returns:
        pd.Series: Series with server_name and list of batch IDs to download
    """
    server_name = schedule_row["server_name"]
    server_start_idx = schedule_row["start_index"]
    server_end_idx = schedule_row["end_index"]

    server_batches: Set[int] = set(range(server_start_idx, server_end_idx + 1))

    verifier_df = verifier_df[
        (verifier_df["server_name"] == server_name) & (verifier_df["partition_id"] >= server_start_idx) & (
                verifier_df["partition_id"] <= server_end_idx)]
    verifier_set = set(verifier_df["partition_id"])

    server_batches = server_batches - verifier_set

    # server_batches.extend(range(max_batch_idx, server_end_idx + 1))
    return pd.Series([server_name, list(server_batches)], index=["server_name", "batches"])


def separate_to_blocks(data_row: pd.Series) -> List[List[Tuple[int, int]]]:
    """
    Organize batch IDs into processing blocks for efficient distribution.
    
    This function takes a list of batch IDs and organizes them into blocks
    based on the available processing capacity (nodes × processes per node).
    
    Args:
        data_row: Row containing server information and batch IDs to process
        
    Returns:
        List[List[Tuple[int, int]]]: Nested lists of batch ranges
    """
    batches: List[int] = data_row["batches"]
    num_of_blocks: int = data_row["process_per_node"] * data_row["nodes"]

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
    """
    Compress consecutive batch IDs into ranges for efficient storage.
    
    Args:
        ids: List of batch IDs (integers)
        
    Returns:
        List[Tuple[int, int]]: List of (start_id, end_id+1) tuples representing ranges
    """
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
    """
    Find the largest bucket that fits within available space.
    
    This function is used in scheduling to find the largest process_per_node
    value that can be accommodated with the remaining worker slots.
    
    Args:
        buckets: Dictionary mapping bucket sizes to queues of servers
        avail_space: Available space (number of worker slots)
        
    Returns:
        int: Size of the largest bucket that fits, or 0 if none fit
    """
    largest_bucket = 0

    for key, bucket in buckets.items():
        if key > avail_space or len(bucket) == 0:
            continue
        largest_bucket = max(largest_bucket, key)

    return largest_bucket


def is_enough_time(rate_limit: RateLimit, batch_size: int = 10000, avg_write_time: int = 600,
                   job_end_time: int = int(os.getenv("SLURM_JOB_END_TIME", 0))) -> bool:
    """
    Check if there is enough time left in the job to process a batch.
    
    This function estimates if there's enough time to download and process
    a batch based on the current rate limit and average write time.
    
    Args:
        rate_limit: Rate limit object with current download rate
        batch_size: Size of the batch to be processed
        avg_write_time: Average time in seconds needed to write results
        job_end_time: Unix timestamp when the job is scheduled to end
        
    Returns:
        bool: True if there is enough time, False otherwise
    """
    current_time = time.time()
    time_left = job_end_time - current_time - avg_write_time
    return rate_limit.initial_rate * time_left >= batch_size


def get_schedule_count(path_to_dir: str) -> int:
    """
    Count the number of schedule files in a directory.
    
    Args:
        path_to_dir: Path to the directory containing schedule files
        
    Returns:
        int: Count of schedule files (excluding files starting with '_')
    """
    schedule_files = [file for file in os.listdir(path_to_dir) if not file.startswith("_")]
    return len(schedule_files)
