"""
Preparation module for MPI-based distributed image downloads.

This module handles the creation of download schedules and submission of download jobs
based on configuration parameters. It coordinates the distribution of download tasks
across multiple nodes and handles dependency chains between jobs.

The main workflow:
1. Create schedules based on server profiles and available resources
2. Submit downloaders with appropriate dependencies
3. Submit verifiers to check the completion of download jobs
"""
import os
from logging import Logger
from typing import Dict, List, Tuple

import pandas as pd
from pandas._libs.missing import NAType

from distributed_downloader.core.utils import (
    create_schedule_configs,
    verify_batches_for_prep,
)
from distributed_downloader.tools.checkpoint import Checkpoint
from distributed_downloader.tools.config import Config
from distributed_downloader.tools.utils import (
    init_logger,
    preprocess_dep_ids,
    submit_job,
)


def schedule_rule(total_batches: int, rule: List[Tuple[int, int]]) -> int | NAType:
    """
    Determine the number of nodes to allocate based on batch count and rules.
    
    Args:
        total_batches: The total number of batches to process
        rule: List of tuples (min_batches, nodes) for resource allocation rules
    
    Returns:
        int: Number of nodes to allocate or pd.NA if no rule matches
    """
    for min_batches, nodes in rule:
        if total_batches >= min_batches:
            return nodes
    return pd.NA


def init_new_current_folder(old_folder: str) -> None:
    """
    Initialize a new 'current' folder for schedules or logs.
    
    If a 'current' folder exists, rename it with a sequential number and create a new empty one.
    
    Args:
        old_folder: Path to the parent folder containing the 'current' directory
    """
    if os.path.exists(f"{old_folder}/current"):
        number_of_folders = len(
            [folder for folder in os.listdir(old_folder) if os.path.isdir(f"{old_folder}/{folder}")])
        new_name = str(number_of_folders).zfill(4)
        os.rename(f"{old_folder}/current", f"{old_folder}/{new_name}")
    os.mkdir(f"{old_folder}/current")


def fix_rule(rule: Dict[str, int]) -> List[Tuple[int, int]]:
    """
    Convert scheduling rules from config dict format to sorted list of tuples.
    
    Args:
        rule: Dict mapping minimum batch count (as string) to number of nodes
    
    Returns:
        List[Tuple[int, int]]: List of (min_batches, nodes) tuples, sorted by min_batches in descending order
    """
    fixed_rule = []
    for key, value in rule.items():
        fixed_rule.append((int(key), value))
    fixed_rule.sort(key=lambda x: x[0], reverse=True)
    return fixed_rule


def submit_downloader(_schedule: str,
                      iteration_id: int,
                      dep_id: int,
                      mpi_submitter_script: str,
                      downloading_script: str) -> int:
    """
    Submit a download job through the MPI submitter script.
    
    Args:
        _schedule: Path to the schedule directory
        iteration_id: Iteration identifier for the job
        dep_id: ID of the job this submission depends on, or None
        mpi_submitter_script: Path to the MPI job submission script
        downloading_script: Path to the downloading script to be executed
    
    Returns:
        int: Job ID of the submitted job
    """
    iteration = str(iteration_id).zfill(4)

    idx = submit_job(mpi_submitter_script,
                     downloading_script,
                     _schedule,
                     iteration,
                     *preprocess_dep_ids([dep_id]))

    return idx


def submit_verifier(_schedule: str,
                    iteration_id: int,
                    mpi_submitter_script: str,
                    verifying_script: str,
                    dep_id: int = None) -> int:
    """
    Submit a verification job through the MPI submitter script.
    
    Args:
        _schedule: Path to the schedule directory
        iteration_id: Iteration identifier for the job
        mpi_submitter_script: Path to the MPI job submission script
        verifying_script: Path to the verifying script to be executed
        dep_id: ID of the job this submission depends on, or None
    
    Returns:
        int: Job ID of the submitted job
    """
    iteration = str(iteration_id).zfill(4)

    idx = submit_job(mpi_submitter_script,
                     verifying_script,
                     _schedule,
                     iteration,
                     *preprocess_dep_ids([dep_id]))

    return idx


def create_schedules(config: Config, logger: Logger) -> None:
    """
    Create download schedules based on server profiles and available resources.
    
    This function analyzes server profiles, determines how many resources to allocate
    to each server based on scheduling rules, and creates schedule configurations
    for the downloader jobs.
    
    Args:
        config: Configuration object with download parameters
        logger: Logger instance for output
    """
    logger.info("Creating schedules")
    # Get parameters from config
    server_ignored_csv: str = config.get_folder("ignored_table")
    schedules_path: str = os.path.join(config.get_folder("schedules_folder"),
                                       "current")
    server_profiler_csv: str = config.get_folder("profiles_table")
    downloaded_images_path: str = config.get_folder("images_folder")
    number_of_workers: int = (config['downloader_parameters']['max_nodes']
                              * config['downloader_parameters']['workers_per_node'])
    schedule_rule_dict: List[Tuple[int, int]] = fix_rule(config['schedule_rules'])

    # Get list to download
    profiles_df = pd.read_csv(server_profiler_csv)

    if os.path.exists(server_ignored_csv) and os.stat(server_ignored_csv).st_size != 0:
        ignored_servers_df = pd.read_csv(server_ignored_csv)
    else:
        ignored_servers_df = pd.DataFrame(columns=["server_name"])

    if os.path.exists(schedules_path) and len(os.listdir(schedules_path)) > 0:
        downloaded_batches: pd.DataFrame = verify_batches_for_prep(profiles_df, downloaded_images_path)
        downloaded_batches = downloaded_batches.groupby("server_name").count().reset_index().dropna()
        downloaded_batches = downloaded_batches.rename(
            columns={"status": "already_downloaded"})
        profiles_df = profiles_df.merge(downloaded_batches, on="server_name", how="left", validate="1:1").fillna(0)
        profiles_df["left_to_download"] = profiles_df["total_batches"] - profiles_df["already_downloaded"]
    else:
        profiles_df["left_to_download"] = profiles_df["total_batches"]

    profiles_df["nodes"] = profiles_df["left_to_download"].apply(lambda x: schedule_rule(x, schedule_rule_dict))
    profiles_df["process_per_node"] = 1
    profiles_df = (profiles_df
                   .dropna()
                   .reset_index(drop=True))
    profiles_df = profiles_df[["server_name", "total_batches", "process_per_node", "nodes"]]
    profiles_df = profiles_df.loc[:, ~profiles_df.columns.duplicated()].copy()
    profiles_df = profiles_df[~profiles_df["server_name"].isin(ignored_servers_df["server_name"])]

    # Rename old schedule and logs
    init_new_current_folder(config.get_folder("schedules_folder"))
    init_new_current_folder(config.get_folder("logs_folder"))

    # Create schedules
    create_schedule_configs(profiles_df, number_of_workers, schedules_path)
    logger.info("Schedules created")


def submit_downloaders(config: Config, logger: Logger) -> None:
    """
    Submit download and verification jobs for all schedules.
    
    For each schedule:
    1. Submit multiple download jobs with appropriate dependencies
    2. Submit a verification job dependent on the last download job
    3. Record job IDs for future reference
    
    Args:
        config: Configuration object with job submission parameters
        logger: Logger instance for output
    """
    logger.info("Submitting downloaders")
    # Get parameters from config
    schedules_path: str = os.path.join(config.get_folder("schedules_folder"),
                                       "current")
    mpi_submitter_script: str = config.get_script("mpi_submitter")
    downloading_script: str = config.get_script('download_script')
    verifying_script: str = config.get_script('verify_script')

    # Schedule downloaders
    for schedule in os.listdir(schedules_path):
        submission_records = []
        offset = 0
        download_id = None

        for _ in range(config["downloader_parameters"]["num_downloads"]):
            download_id = submit_downloader(schedule,
                                            offset,
                                            download_id,
                                            mpi_submitter_script,
                                            downloading_script)
            submission_records.append({
                "job_id": download_id,
                "is_verification": False
            })
            logger.info(f"Submitted downloader {download_id} for {schedule}")

            offset += 1

        verifier_id = submit_verifier(schedule,
                                      offset,
                                      mpi_submitter_script,
                                      verifying_script,
                                      download_id)
        submission_records.append({
            "job_id": verifier_id,
            "is_verification": True
        })
        logger.info(f"Submitted verifier {verifier_id} for {schedule}")
        offset += 1

        pd.DataFrame(submission_records).to_csv(os.path.join(schedules_path, schedule, "_jobs_ids.csv"),
                                                index=False,
                                                header=True)

    logger.info("All downloading scripts submitted")


def main():
    """
    Main entry point that coordinates the schedule creation and job submission process.
    
    1. Loads configuration from environment variables
    2. Creates downloading schedules based on server profiles
    3. Submits downloading and verification jobs
    4. Updates the checkpoint to indicate completion
    """
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = Config.from_path(config_path, "downloader")
    logger = init_logger(__name__)

    inner_checkpoint_path: str = config.get_folder("inner_checkpoint_file")
    if not os.path.exists(inner_checkpoint_path):
        raise FileNotFoundError(f"Inner checkpoint file {inner_checkpoint_path} not found")
    inner_checkpoint = Checkpoint.from_path(inner_checkpoint_path, {"schedule_creation_scheduled": False})

    create_schedules(config, logger)
    submit_downloaders(config, logger)

    inner_checkpoint["schedule_creation_scheduled"] = False


if __name__ == "__main__":
    main()
