import os
import time
from typing import List, Any

import pandas as pd
import py7zr
from py7zr import FILTER_ZSTD

from mpi_downloader import CompletedBatch
from mpi_downloader.dataclasses import error_entry, success_entry, success_dtype, error_dtype


# Batch_size = 1000

def write_batch(
        completed_batch: CompletedBatch,
        output_path: str,
        job_end_time: int
) -> None:
    os.makedirs(output_path, exist_ok=True)
    successes_list: List[List[Any]] = []
    errors_list: List[List[Any]] = []

    if job_end_time - time.time() < 0:
        raise TimeoutError("Not enough time")

    if completed_batch.success_queue.qsize() == completed_batch.error_queue.qsize() == 0:
        raise ValueError("Empty batch")

    try:
        with py7zr.SevenZipFile(f"{output_path}/images.7z", 'w', filters=[{'id': FILTER_ZSTD, 'level': 3}]) as f:
            for _ in range(completed_batch.success_queue.qsize()):
                if job_end_time - time.time() < 0:
                    raise TimeoutError("Not enough time")

                success = completed_batch.success_queue.get()
                successes_list.append(success_entry.to_list_download(success))
                f.writestr(success.image, success.UUID)
                completed_batch.success_queue.task_done()

        for i in range(completed_batch.error_queue.qsize()):
            error = completed_batch.error_queue.get()
            errors_list.append(error_entry.to_list_download(error))
            completed_batch.error_queue.task_done()

        pd.DataFrame(successes_list, columns=success_dtype.names).to_parquet(f"{output_path}/successes.parquet", index=False)
        pd.DataFrame(errors_list, columns=error_dtype.names).to_parquet(f"{output_path}/errors.parquet", index=False)

        open(f"{output_path}/completed", "w").close()
    except (TimeoutError, ValueError) as error:
        raise error
    except Exception as e:
        open(f"{output_path}/failed", "w").close()
        raise e
