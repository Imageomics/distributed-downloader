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


def download_batch(
        _downloader: Downloader,
        _input_path: str,
        _batch_id: int,
) -> Tuple[CompletedBatch, float]:
    batch = load_one_batch(_input_path)

    _completed_batch, _finish_rate = _downloader.get_images(batch)

    return _completed_batch, _finish_rate


parser = argparse.ArgumentParser(description='Server downloader')

parser.add_argument('input_path', metavar='input_path', type=str, help='the path to with download components (e.g., image folder and server batches)')
parser.add_argument("schedule_path", metavar="schedule_path", type=str, help="the path to the schedule")
parser.add_argument("--header", required=True, type=str, help="the requests header")
parser.add_argument("--img-size", required=True, type=int, help="the max side-length of an image in pixels")
parser.add_argument("--rate-multiplier", required=False, type=float, help="the rate multiplier", default=0.5)
parser.add_argument("--logging-level", required=False, type=str, help="the logging level", default="INFO")

# parse the arguments
_args = parser.parse_args()
header_str = _args.header
header = {header_str.split(": ")[0]: header_str.split(": ")[1]}
img_size = _args.img_size
Input_path: str = _args.input_path
Server_urls_batched = f"{Input_path}/servers_batched"
Server_downloader_output = f"{Input_path}/downloaded_images"
Server_schedule: str = _args.schedule_path
_RATE_MULTIPLIER: float = _args.rate_multiplier
logging_level: str = _args.logging_level

logging.basicConfig(level=logging.getLevelName(logging_level), format="%(asctime)s - %(levelname)s - %(process)d - %(message)s")
logger = logging.getLogger(__name__)

comm = MPI.COMM_WORLD
rank = comm.rank
mem = MPI.Alloc_mem(1)
window = MPI.Win.Create(mem, comm=comm)
comm.Barrier()

try:
    logger.info(f"Rank {rank} started, getting latest schedule")
    Latest_schedule = get_latest_schedule(Server_schedule, rank)

    if Latest_schedule is None or len(Latest_schedule) < 1:
        raise ValueError(f"Rank {rank} not found in the scheduler")

    Latest_schedule = Latest_schedule.to_dict("records")
    job_end_time: int = int(os.getenv("SLURM_JOB_END_TIME", 0))

    downloader_schedule: Dict[str, Tuple] = {}

    downloading_time = 0
    writing_time = 0

    logger.info(f"Rank {rank} started downloading")

    for schedule_dict in Latest_schedule:
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

                input_path = f"{Server_urls_batched}/ServerName={schedule_dict['ServerName']}/partition_id={batch_id}"
                output_path = f"{Server_downloader_output}/ServerName={schedule_dict['ServerName']}/partition_id={batch_id}"
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
