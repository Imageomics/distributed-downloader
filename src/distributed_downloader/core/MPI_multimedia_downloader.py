import argparse
import logging
import os
import time
from typing import Dict, Tuple

import mpi4py.MPI as MPI

from distributed_downloader.core.mpi_downloader import DirectWriter
from distributed_downloader.core.mpi_downloader.dataclasses import CompletedBatch
from distributed_downloader.core.mpi_downloader.Downloader import Downloader
from distributed_downloader.core.mpi_downloader.PreLoader import load_one_batch
from distributed_downloader.core.mpi_downloader.utils import (
    get_latest_schedule,
    get_or_init_downloader,
    is_enough_time,
)
from distributed_downloader.tools.config import Config
from distributed_downloader.tools.utils import init_logger


def download_batch(
        _downloader: Downloader,
        _input_path: str,
        _batch_id: int,
) -> Tuple[CompletedBatch, float]:
    """
    Downloads a single batch of images using the provided downloader.

    This function loads URL data from a parquet file at the input path,
    passes it to the downloader, and returns both the completed batch
    and the final download rate achieved.

    Args:
        _downloader: Instance of Downloader class to use for downloading
        _input_path: Path to the directory containing batch data
        _batch_id: Identifier for the batch being downloaded

    Returns:
        Tuple[CompletedBatch, float]: Completed batch object containing downloaded items and
                                      the final download rate achieved
    """
    batch = load_one_batch(_input_path)

    _completed_batch, _finish_rate = _downloader.get_images(batch)

    return _completed_batch, _finish_rate


def download_schedule(
        config: Config,
        server_schedule: str,
        logger: logging.Logger,
):
    """
    Main download function that processes a schedule using MPI parallelism.
    
    This function coordinates multiple MPI processes to download images in parallel:
    1. Each MPI rank loads its assigned part of the schedule
    2. For each server assigned, it initializes or reuses a downloader with appropriate rate limits
    3. Downloads assigned batches using exclusive MPI locks to prevent server overloading
    4. Writes downloaded batch data to storage
    5. Tracks performance metrics (download time, write time)
    
    The function handles synchronization between MPI ranks using window locking
    to ensure server rate limits are respected globally.
    
    Args:
        config: Configuration object with download parameters
        server_schedule: Path to the directory containing schedule files
        logger: Logger instance for output
    """
    header_str = config["downloader_parameters"]["header"]
    header = {header_str.split(": ")[0]: header_str.split(": ")[1]}
    img_size = config["downloader_parameters"]["image_size"]
    server_urls_batched = config.get_folder("urls_folder")
    server_downloader_output = config.get_folder("images_folder")
    _RATE_MULTIPLIER: float = config["downloader_parameters"]["rate_multiplier"]

    if os.path.exists(f"{server_schedule}/_DONE"):
        logger.info(f"Schedule {server_schedule} already done")
        return

    # Initialize MPI communication
    comm = MPI.COMM_WORLD
    rank = comm.rank
    mem = MPI.Alloc_mem(1)
    window = MPI.Win.Create(mem, comm=comm)
    comm.Barrier()

    try:
        logger.info(f"Rank {rank} started, getting latest schedule")
        latest_schedule = get_latest_schedule(server_schedule, rank)

        if latest_schedule is None or len(latest_schedule) < 1:
            raise ValueError(f"Rank {rank} not found in the scheduler")

        latest_schedule = latest_schedule.to_dict("records")
        job_end_time: int = int(os.getenv("SLURM_JOB_END_TIME", 0))

        # Dictionary for reusing downloaders across batches for the same server
        downloader_schedule: Dict[str, Tuple] = {}

        downloading_time = 0
        writing_time = 0

        logger.info(f"Rank {rank} started downloading")

        for schedule_dict in latest_schedule:
            # Get or initialize a downloader for this server
            downloader, _, rate_limit = get_or_init_downloader(header,
                                                               img_size,
                                                               schedule_dict,
                                                               downloader_schedule,
                                                               _RATE_MULTIPLIER,
                                                               job_end_time,
                                                               logger)

            for batch_id in range(schedule_dict["partition_id_from"], schedule_dict["partition_id_to"]):
                # Lock to ensure exclusive access when downloading from a server
                window.Lock(schedule_dict["main_rank"], MPI.LOCK_EXCLUSIVE)
                try:
                    if not is_enough_time(rate_limit, job_end_time=job_end_time):
                        raise TimeoutError("Not enough time to download batch")

                    logger.info(f"Rank {rank} started downloading batch {batch_id} of {schedule_dict['server_name']}")

                    t0 = time.perf_counter()

                    input_path = f"{server_urls_batched}/server_name={schedule_dict['server_name']}/partition_id={batch_id}"  # TODO: Make "ServerName" and "partition_id" changeable column from config
                    output_path = f"{server_downloader_output}/server_name={schedule_dict['server_name']}/partition_id={batch_id}"
                    completed_batch, finish_rate = download_batch(downloader, input_path, batch_id)
                    rate_limit.change_rate(finish_rate)

                    downloading_time += time.perf_counter() - t0

                    logger.info(f"Rank {rank} finished downloading batch {batch_id} of {schedule_dict['server_name']}")
                except Exception as e:
                    window.Unlock(schedule_dict["main_rank"])
                    raise e
                else:
                    window.Unlock(schedule_dict["main_rank"])

                    t0 = time.perf_counter()
                    DirectWriter.write_batch(completed_batch, output_path, job_end_time, logger=logger)
                    logger.info(f"Rank {rank} finished writing batch {batch_id} of {schedule_dict['server_name']}")

                    writing_time += time.perf_counter() - t0

            logger.info(f"Rank {rank} spent {downloading_time} seconds downloading and {writing_time} seconds writing")
    except Exception as e:
        logger.error(f"Rank {rank} failed with error: {e}")
    finally:
        # Clean up MPI resources
        window.Free()
        mem.release()


def main():
    """
    Main entry point that parses arguments and initiates the download process.
    
    This function:
    1. Loads configuration from the environment
    2. Initializes logging
    3. Parses the schedule path from command line arguments
    4. Calls the download_schedule function to begin downloading
    
    Raises:
        ValueError: If the CONFIG_PATH environment variable is not set
    """
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = Config.from_path(config_path, "downloader")
    logger = init_logger(__name__)

    parser = argparse.ArgumentParser(description='Server downloader')
    parser.add_argument("schedule_path", metavar="schedule_path", type=str, help="the path to the schedule")
    _args = parser.parse_args()

    download_schedule(
        config,
        _args.schedule_path,
        logger
    )


if __name__ == "__main__":
    main()
