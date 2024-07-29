import logging
import os
import time
from typing import List, Any

import pandas as pd

from .dataclasses import ErrorEntry, SuccessEntry, CompletedBatch


def write_batch(
        completed_batch: CompletedBatch,
        output_path: str,
        job_end_time: int,
        logger: logging.Logger = logging.getLogger()
) -> None:
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
                                                                                  index=False)
        pd.DataFrame(errors_list, columns=ErrorEntry.get_names()).to_parquet(f"{output_path}/errors.parquet",
                                                                             index=False)

        logger.info(f"Completed writing to {output_path}")

        open(f"{output_path}/completed", "w").close()
    except (TimeoutError, ValueError) as error:
        raise error
    except Exception as e:
        open(f"{output_path}/failed", "w").close()
        raise e
