"""
Batch loading utilities for the distributed downloader.

This module provides functions to load batches of download tasks from
parquet files. It supports both loading multiple batches and single batches
for processing by the downloader workers.
"""

from typing import Any, Dict, Iterator, List

import pandas as pd


def load_batch(
        path_to_parquet: str,
        server_name: str,
        batches_to_download: List[int],
) -> Iterator[List[Dict[str, Any]]]:
    """
    Load multiple batches for a specific server.
    
    This generator function loads batches of URLs from parquet files
    for a given server, one batch at a time.
    
    Args:
        path_to_parquet: Base directory for input parquet files
        server_name: Name of the server to load batches for
        batches_to_download: List of batch IDs to download
        
    Yields:
        List[Dict[str, Any]]: List of dictionaries containing URL and metadata for each batch
    """
    for batch_id in batches_to_download:
        server_df = pd.read_parquet(
            f"{path_to_parquet}/ServerName={server_name.replace(':', '%3A')}/partition_id={batch_id}")
        yield server_df.to_dict("records")


def load_one_batch(input_path: str) -> List[Dict[str, Any]]:
    """
    Load a single batch from a parquet file.
    
    Args:
        input_path: Path to the parquet file to load
        
    Returns:
        List[Dict[str, Any]]: List of dictionaries containing URL and metadata
    """
    return pd.read_parquet(input_path).to_dict("records")
