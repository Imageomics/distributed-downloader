import argparse
import os
import time
from typing import Dict, Tuple

import mpi4py.MPI as MPI

from mpi_downloader import DirectWriter, CompletedBatch
from mpi_downloader.dataclasses import CompletedBatch
from mpi_downloader.Downloader import Downloader
from mpi_downloader.PreLoader import load_one_batch
from mpi_downloader.utils import get_latest_schedule, \
    get_or_init_downloader, is_enough_time

_RATE_MULTIPLIER = 0.5


def download_batch(
        _downloader: Downloader,
        _input_path: str,
        _batch_id: int,
) -> Tuple[CompletedBatch, float]:
    batch = load_one_batch(_input_path)

    _completed_batch, _finish_rate = _downloader.get_images(batch)
    print(f"{rank} {_finish_rate}")
    return _completed_batch, _finish_rate


parser = argparse.ArgumentParser(description='Server downloader')

parser.add_argument('input_path', metavar='input_path', type=str, help='the path to folder of work')
parser.add_argument("schedule_path", metavar="schedule_path", type=str, help="the path to the schedule")
parser.add_argument("--header", required=True, type=str, help="the requests header")
parser.add_argument("--img-size", required = True, type=int, help="the max side-length of an image in pixels")

# parse the arguments
_args = parser.parse_args()
header_str = _args.header
header = {header_str.split(": ")[0]: header_str.split(": ")[1]}
img_size = _args.img_size
Input_path: str = _args.input_path
Server_urls_batched = f"{Input_path}/servers_batched"
Server_downloader_output = f"{Input_path}/downloaded_images"
Server_schedule: str = _args.schedule_path

comm = MPI.COMM_WORLD
rank = comm.rank
print("Creating Window")
mem = MPI.Alloc_mem(1)
window = MPI.Win.Create(mem, comm=comm)
comm.Barrier()

try:
    print("Getting new Schedule")
    Latest_schedule = get_latest_schedule(Server_schedule, rank)

    if Latest_schedule is None or len(Latest_schedule) < 1:
        raise ValueError(f"Rank {rank} not found in the scheduler")

    Latest_schedule = Latest_schedule.to_dict("records")
    job_end_time: int = int(os.getenv("SLURM_JOB_END_TIME", 0))

    downloader_schedule: Dict[str, Tuple] = {}

    downloading_time = 0
    writing_time = 0

    print("Locking first window")

    for schedule_dict in Latest_schedule:
        downloader, _, rate_limit = get_or_init_downloader(header, img_size, schedule_dict, downloader_schedule,
                                                           _RATE_MULTIPLIER, job_end_time)

        for batch_id in range(schedule_dict["PartitionIdFrom"], schedule_dict["PartitionIdTo"]):
            window.Lock(schedule_dict["MainRank"], MPI.LOCK_EXCLUSIVE)
            try:
                if not is_enough_time(rate_limit):
                    raise TimeoutError("Not enough time to download batch")

                print(f"Rank {rank} starting batch {batch_id} of {schedule_dict['ServerName']}")

                t0 = time.perf_counter()

                input_path = f"{Server_urls_batched}/ServerName={schedule_dict['ServerName']}/partition_id={batch_id}"
                output_path = f"{Server_downloader_output}/ServerName={schedule_dict['ServerName']}/partition_id={batch_id}"
                completed_batch, finish_rate = download_batch(downloader, input_path, batch_id)
                rate_limit.change_rate(finish_rate)

                downloading_time += time.perf_counter() - t0

                print(f"Rank {rank} finished downloading batch {batch_id} of {schedule_dict['ServerName']}")
            except Exception as e:
                window.Unlock(schedule_dict["MainRank"])
                raise e
            else:
                window.Unlock(schedule_dict["MainRank"])

                t0 = time.perf_counter()
                DirectWriter.write_batch(completed_batch, output_path, job_end_time)
                print(f"Rank {rank} finished batch {batch_id} of {schedule_dict['ServerName']}")

                writing_time += time.perf_counter() - t0

        print(f"Rank {rank} spent {downloading_time} downloading and {writing_time} writing")
except Exception as e:
    print(e)
    raise e
finally:
    # comm.Barrier()
    window.Free()
    mem.release()
