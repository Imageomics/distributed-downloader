"""
Storage module for the distributed downloader.

This module handles writing downloaded image data to persistent storage.
It processes both successful and failed downloads, storing them in
separate parquet files with appropriate metadata.
"""

import logging
import os
import time
from typing import Any, List

import pandas as pd

from .dataclasses import CompletedBatch, ErrorEntry, SuccessEntry


def write_batch(
        completed_batch: CompletedBatch,
        output_path: str,
        job_end_time: int,
        logger: logging.Logger = logging.getLogger()
) -> None:
    """
    Write a completed batch of downloads to storage.
    
    This function processes successful and failed downloads from a batch,
    writing them to separate parquet files with appropriate metadata.
    It also creates marker files indicating completion status.
    
    Args:
        completed_batch: CompletedBatch object containing successful and failed downloads
        output_path: Directory path to write the batch data
        job_end_time: UNIX timestamp when the job should end
        logger: Logger instance for output messages
        
    Raises:
        TimeoutError: If there is not enough time left to complete writing
        ValueError: If the batch is empty (no successes or errors)
        Exception: For other errors during the write process
    """
    logger.debug(f"Writing batch to {output_path}")

    os.makedirs(output_path, exist_ok=True)
    successes_list: List[List[Any]] = []
    errors_list: List[List[Any]] = []

    if job_end_time - time.time() < 0:
        raise TimeoutError("Not enough time")

    if completed_batch.success_queue.qsize() == completed_batch.error_queue.qsize() == 0:
        raise ValueError("Empty batch")

    try:
        for _ in range(completed_batch.success_queue.qsize()):
            success = completed_batch.success_queue.get()
            success_entity = SuccessEntry.to_list_download(success)
            successes_list.append(success_entity)
            completed_batch.success_queue.task_done()

            logger.debug(f"Writing success entry {success_entity}")

        for _ in range(completed_batch.error_queue.qsize()):
            error = completed_batch.error_queue.get()
            error_entity = ErrorEntry.to_list_download(error)
            errors_list.append(error_entity)
            completed_batch.error_queue.task_done()

            logger.debug(f"Writing error entry {error_entity}")

        logger.info(f"Completed collecting entries for {output_path}")

        pd.DataFrame(successes_list, columns=SuccessEntry.get_names()).to_parquet(f"{output_path}/successes.parquet",
                                                                                  index=False, compression="zstd",
                                                                                  compression_level=3)
        pd.DataFrame(errors_list, columns=ErrorEntry.get_names()).to_parquet(f"{output_path}/errors.parquet",
                                                                             index=False, compression="zstd",
                                                                             compression_level=3)

        logger.info(f"Completed writing to {output_path}")

        open(f"{output_path}/completed", "w").close()
    except (TimeoutError, ValueError) as error:
        raise error
    except Exception as e:
        open(f"{output_path}/failed", "w").close()
        raise e
