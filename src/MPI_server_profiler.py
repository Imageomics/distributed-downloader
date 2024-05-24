import argparse
import re
from queue import Queue
from typing import Dict, Tuple

import h5py
import mpi4py.MPI as MPI
import pandas as pd

from mpi_downloader import CompletedBatch, ProfilerWriter
from mpi_downloader.Downloader import Downloader
from mpi_downloader.PreLoader import load_one_batch
from mpi_downloader.dataclasses import RateLimit
from mpi_downloader.utils import create_new_session

Initial_rate = 20
Rate_multiplier = 10
Time_to_profile = 2

parser = argparse.ArgumentParser(description='Server profiler')

parser.add_argument('input_path', metavar='input_path', type=str, help='the path to folder of work')
parser.add_argument('batch_size', metavar='batch_size', type=int, help='size of the batch to download')
parser.add_argument("--header", required=True, type=str, help="the requests header")
parser.add_argument("--img-size", required = True, type=int, help="the max side-length of an image in pixels")

# parse the arguments
_args = parser.parse_args()
header_str = _args.header
header = {header_str.split(": ")[0]: header_str.split(": ")[1]}
img_size = _args.img_size
Input_path: str = _args.input_path
Server_urls_batched = f"{Input_path}/servers_batched"
Server_profiler_hdf = f"{Input_path}/servers_profiles.hdf5"
Server_errors_hdf = f"{Input_path}/servers_errors.hdf5"
Server_profile_spec = f"{Input_path}/profile_spec.csv"
Batch_size: int = _args.batch_size

rank = MPI.COMM_WORLD.rank

scheduler_df = pd.read_csv(Server_profile_spec)
scheduler_dicts = scheduler_df[scheduler_df["Rank"] == rank].to_dict("records")

if len(scheduler_dicts) < 1:
    raise ValueError(f"Rank {rank} not found in the scheduler")

downloader_schedule: Dict[str, Tuple] = {}
profiles_hdf = h5py.File(Server_profiler_hdf, 'r+', driver='mpio', comm=MPI.COMM_WORLD)
errors_hdf = h5py.File(Server_errors_hdf, 'r+', driver='mpio', comm=MPI.COMM_WORLD)


def download_batch(
        _downloader: Downloader,
        _rate_limit: RateLimit,
        _input_path: str,
) -> Tuple[CompletedBatch, float]:
    batch = load_one_batch(_input_path)

    _completed_batch, finish_rate = _downloader.get_images(batch, _rate_limit)
    return _completed_batch, finish_rate


for idx, schedule_dict in enumerate(scheduler_dicts):
    if schedule_dict["ServerName"] not in downloader_schedule.keys():
        server_name = re.sub(':', '%3A', schedule_dict["ServerName"])
        rate_limit = RateLimit(Initial_rate, Rate_multiplier)
        session = create_new_session(server_name, rate_limit.upper_bound)
        downloader = Downloader(header, session, rate_limit, img_size, False)
        downloader_schedule[schedule_dict["ServerName"]] = (downloader, rate_limit)

    downloader, rate_limit = downloader_schedule[schedule_dict["ServerName"]]
    completed_batch_final: CompletedBatch = CompletedBatch(Queue(), Queue())
    new_rate_limit_final: float = 0

    for _ in range(Time_to_profile):
        completed_batch_final = CompletedBatch(Queue(), Queue())
        print(f"Rank {rank} starting batch 0|{rate_limit}|{schedule_dict['ServerName']}")

        input_path = f"{Server_urls_batched}/ServerName={schedule_dict['ServerName']}/partition_id=0"
        completed_batch, new_rate_limit = download_batch(downloader, rate_limit, input_path)
        rate_limit.change_rate(new_rate_limit)

        completed_batch_final = completed_batch
        new_rate_limit_final = new_rate_limit

        print(f"Rank {rank} finished batch 0|{schedule_dict['ServerName']}")

    ProfilerWriter.write_batch(
        profiles_hdf["profiles"],
        errors_hdf["errors"],
        completed_batch_final,
        new_rate_limit_final,
        rank,
        schedule_dict['Offset'],
        Batch_size,
        schedule_dict['ServerName'],
        schedule_dict["BatchesCount"],
        Input_path
    )

profiles_hdf.close()
errors_hdf.close()
