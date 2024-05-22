import math
import os
import subprocess
from typing import List

import pandas
import pandas as pd

NUM_DOWNLOADERS: int = 10
RECHECK = False
SCHEDULES: List[str] = []

input_path = "/fs/scratch/PAS2136/gbif/processed/2024-05-01/multimedia_prep"
logs_path = f"{input_path}/logs"
schedules_path = f"{input_path}/schedules"
submitted_jobs_file = "_jobs_ids.csv"


def get_logs_offset(path: str) -> int:
    if not os.path.exists(path):
        return 0

    dirs: List[int] = [int(_path) for _path in os.listdir(path) if os.path.isdir(f"{path}/{_path}")]

    dirs.sort(reverse=True)
    if len(dirs) == 0:
        return 0
    return dirs[0]


def get_id(output: bytes) -> int:
    return int(output.decode().strip().split(" ")[-1])


def submit_downloader(_schedule: str, iteration_id: int, dep_id: int) -> int:
    iteration = str(iteration_id).zfill(4)
    output = subprocess.check_output(f"/users/PAS2119/andreykopanev/gbif/scripts/submit_mpi_download.sh "
                                     f"/users/PAS2119/andreykopanev/gbif/scripts/server_downloading.slurm "
                                     f"{_schedule} "
                                     f"{iteration} "
                                     f"{dep_id}", shell=True)
    idx = get_id(output)
    print(f"Submitted downloader {idx} for {_schedule}")
    return idx


def submit_verifier(_schedule: str, iteration_id: int, dep_id: int = None) -> int:
    iteration = str(iteration_id).zfill(4)

    command_str = (f"/users/PAS2119/andreykopanev/gbif/scripts/submit_mpi_download.sh "
                   f"/users/PAS2119/andreykopanev/gbif/scripts/server_verifing.slurm {_schedule} {iteration}")
    if dep_id is not None:
        command_str += f" {dep_id}"
    if RECHECK:
        command_str += f" --recheck"

    output = subprocess.check_output(command_str, shell=True)
    idx = get_id(output)
    print(f"Submitted verifier {idx} for {_schedule}")
    return idx


if __name__ == "__main__":
    schedules = SCHEDULES
    if len(schedules) == 0:
        schedules = [folder for folder in os.listdir(schedules_path) if os.path.isdir(f"{schedules_path}/{folder}")]

    for schedule in schedules:
        if os.path.exists(f"{schedules_path}/{schedule}/_DONE"):
            continue

        prev_jobs = pandas.DataFrame({
            "job_id": pd.Series(dtype="int"),
            "is_verification": pd.Series(dtype="bool")
        })
        if os.path.exists(f"{schedules_path}/{schedule}/{submitted_jobs_file}"):
            prev_jobs = pandas.read_csv(f"{schedules_path}/{schedule}/{submitted_jobs_file}")
        prev_jobs = prev_jobs.to_dict("records")
        offset = math.ceil(len(prev_jobs) / 2)

        if offset == 0 or not prev_jobs[-1]["is_verification"] or RECHECK:
            verifier_id = submit_verifier(schedule, offset)
            prev_jobs.append({
                "job_id": verifier_id,
                "is_verification": True
            })
            offset += 1

        for _ in range(NUM_DOWNLOADERS):
            download_id = submit_downloader(schedule, offset, prev_jobs[-1]["job_id"])
            prev_jobs.append({
                "job_id": download_id,
                "is_verification": False
            })

            verifier_id = submit_verifier(schedule, offset, download_id)
            prev_jobs.append({
                "job_id": verifier_id,
                "is_verification": True
            })

            offset += 1

        pandas.DataFrame(prev_jobs).to_csv(f"{schedules_path}/{schedule}/{submitted_jobs_file}", index=False, header=True)
