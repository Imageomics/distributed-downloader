import math
import os
import argparse
import subprocess
from typing import List
from dotenv import load_dotenv

import pandas as pd

NUM_DOWNLOADERS: int = 10
RECHECK = False
SCHEDULES: List[str] = []

# internal job record used by submitter for tracking progress
SUBMITTED_JOBS_FILE = "_jobs_ids.csv"


def get_env_vars(env_path):
    '''
    Fetch path information from .env for download and schedule directories.
    Also paths to slurm scripts and bash python-slurm coordination script.

    Parameters:
    env_path - String. Path to .env file. Ex: 'path/to/hpc.env'.

    Returns:
    schedules_path - String. Path to schedule in download directory.
    mpi_submitter_script - String. Path to bash script to coordinate Python and slurm scripts.
    downloading_script - String. Path to slurm script to run download.
    verifying_script - String. Path to slurm script to run verifier.
    
    '''
    load_dotenv(env_path)
    download_path = f"{os.getenv('PROCESSED_DATA_ROOT')}/{os.getenv('TIME_STAMP')}/{os.getenv('DOWNLOAD_DIR')}"
    schedules_path = f"{download_path}/{os.getenv('DOWNLOADER_SCHEDULES_FOLDER')}"
    mpi_submitter_script = os.getenv("MPI_SUBMITTER_SCRIPT")
    downloading_script = os.getenv("DOWNLOADING_SCRIPT")
    verifying_script = os.getenv("VERIFYING_SCRIPT")
    
    return schedules_path, mpi_submitter_script, downloading_script, verifying_script


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


def submit_downloader(_schedule: str, iteration_id: int, dep_id: int, mpi_submitter_script: str, downloading_script: str) -> int:
    iteration = str(iteration_id).zfill(4)
    output = subprocess.check_output(f"{mpi_submitter_script} "
                                     f"{downloading_script} "
                                     f"{_schedule} "
                                     f"{iteration} "
                                     f"{dep_id}", shell=True)
    idx = get_id(output)
    print(f"Submitted downloader {idx} for {_schedule}")
    return idx


def submit_verifier(_schedule: str, iteration_id: int, mpi_submitter_script: str, verifying_script: str, dep_id: int = None) -> int:
    iteration = str(iteration_id).zfill(4)

    command_str = f"{mpi_submitter_script} {verifying_script} {_schedule} {iteration}"
    if dep_id is not None:
        command_str += f" {dep_id}"
    if RECHECK:
        command_str += f" --recheck"

    output = subprocess.check_output(command_str, shell=True)
    idx = get_id(output)
    print(f"Submitted verifier {idx} for {_schedule}")
    return idx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-path", required = True, help = "Path to .env file. Ex: 'path/to/hpc.env'.", nargs = "?")
    args = parser.parse_args()
    schedules_path, mpi_submitter_script, downloading_script, verifying_script = get_env_vars(args.env_path)

    # manage scheduling, run jobs
    schedules = SCHEDULES
    if len(schedules) == 0:
        schedules = [folder for folder in os.listdir(schedules_path) if os.path.isdir(f"{schedules_path}/{folder}")]

    for schedule in schedules:
        if os.path.exists(f"{schedules_path}/{schedule}/_DONE"):
            continue
        submitted_jobs_path = f"{schedules_path}/{schedule}/{SUBMITTED_JOBS_FILE}"

        prev_jobs = pd.DataFrame({
            "job_id": pd.Series(dtype="int"),
            "is_verification": pd.Series(dtype="bool")
        })
        if os.path.exists(submitted_jobs_path):
            prev_jobs = pd.read_csv(submitted_jobs_path)
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
            download_id = submit_downloader(schedule, offset, prev_jobs[-1]["job_id"], mpi_submitter_script, downloading_script)
            prev_jobs.append({
                "job_id": download_id,
                "is_verification": False
            })

            verifier_id = submit_verifier(schedule, offset, mpi_submitter_script, verifying_script, download_id)
            prev_jobs.append({
                "job_id": verifier_id,
                "is_verification": True
            })

            offset += 1

        pd.DataFrame(prev_jobs).to_csv(submitted_jobs_path, index=False, header=True)


if __name__ == "__main__":
    main()
