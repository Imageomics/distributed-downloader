import argparse
import logging
import os
import time
from typing import Dict, Tuple

import mpi4py.MPI as MPI

from distributed_downloader.mpi_downloader import DirectWriter
from distributed_downloader.mpi_downloader.dataclasses import CompletedBatch
from distributed_downloader.mpi_downloader.Downloader import Downloader
from distributed_downloader.mpi_downloader.PreLoader import load_one_batch
from distributed_downloader.mpi_downloader.utils import get_latest_schedule, \
    get_or_init_downloader, is_enough_time
from distributed_downloader.utils import init_logger, load_config


def download_batch(
        _downloader: Downloader,
        _input_path: str,
        _batch_id: int,
) -> Tuple[CompletedBatch, float]:
    batch = load_one_batch(_input_path)

    _completed_batch, _finish_rate = _downloader.get_images(batch)

    return _completed_batch, _finish_rate


def download_schedule(
        config: Dict[str, str | int | bool | Dict[str, int | str]],
        server_schedule: str,
        logger: logging.Logger,
):
    header_str = config["downloader_parameters"]["header"]
    header = {header_str.split(": ")[0]: header_str.split(": ")[1]}
    img_size = config["downloader_parameters"]["image_size"]
    server_urls_batched = os.path.join(config['path_to_output_folder'],
                                       config['output_structure']['urls_folder'])
    server_downloader_output = os.path.join(config['path_to_output_folder'],
                                            config['output_structure']['images_folder'])
    _RATE_MULTIPLIER: float = config["downloader_parameters"]["rate_multiplier"]

    if os.path.exists(f"{server_schedule}/_DONE"):
        logger.info(f"Schedule {server_schedule} already done")
        return

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

        downloader_schedule: Dict[str, Tuple] = {}

        downloading_time = 0
        writing_time = 0

        logger.info(f"Rank {rank} started downloading")

        for schedule_dict in latest_schedule:
            downloader, _, rate_limit = get_or_init_downloader(header,
                                                               img_size,
                                                               schedule_dict,
                                                               downloader_schedule,
                                                               _RATE_MULTIPLIER,
                                                               job_end_time,
                                                               logger)

            for batch_id in range(schedule_dict["PartitionIdFrom"], schedule_dict["PartitionIdTo"]):
                window.Lock(schedule_dict["MainRank"], MPI.LOCK_EXCLUSIVE)
                try:
                    if not is_enough_time(rate_limit, job_end_time=job_end_time):
                        raise TimeoutError("Not enough time to download batch")

                    logger.info(f"Rank {rank} started downloading batch {batch_id} of {schedule_dict['ServerName']}")

                    t0 = time.perf_counter()

                    input_path = f"{server_urls_batched}/ServerName={schedule_dict['ServerName']}/partition_id={batch_id}"  # TODO: Make "ServerName" and "partition_id" changeable column from config
                    output_path = f"{server_downloader_output}/ServerName={schedule_dict['ServerName']}/partition_id={batch_id}"
                    completed_batch, finish_rate = download_batch(downloader, input_path, batch_id)
                    rate_limit.change_rate(finish_rate)

                    downloading_time += time.perf_counter() - t0

                    logger.info(f"Rank {rank} finished downloading batch {batch_id} of {schedule_dict['ServerName']}")
                except Exception as e:
                    window.Unlock(schedule_dict["MainRank"])
                    raise e
                else:
                    window.Unlock(schedule_dict["MainRank"])

                    t0 = time.perf_counter()
                    DirectWriter.write_batch(completed_batch, output_path, job_end_time, logger=logger)
                    logger.info(f"Rank {rank} finished writing batch {batch_id} of {schedule_dict['ServerName']}")

                    writing_time += time.perf_counter() - t0

            logger.info(f"Rank {rank} spent {downloading_time} seconds downloading and {writing_time} seconds writing")
    except Exception as e:
        logger.error(f"Rank {rank} failed with error: {e}")
    finally:
        # comm.Barrier()
        window.Free()
        mem.release()


def main():
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = load_config(config_path)
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
