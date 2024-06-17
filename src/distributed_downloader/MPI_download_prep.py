import os
from typing import Dict, List, Tuple

import pandas as pd
from pandas._libs.missing import NAType

from distributed_downloader.utils import create_schedule_configs, load_config
from mpi_downloader.utils import verify_batches_for_prep


def schedule_rule(total_batches: int, rule: List[Tuple[int, int]]) -> int | NAType:
    for min_batches, nodes in rule:
        if total_batches >= min_batches:
            return nodes
    return pd.NA


def init_new_current_folder(old_folder: str) -> None:
    number_of_folders = len([folder for folder in os.listdir(old_folder) if os.path.isdir(f"{old_folder}/{folder}")])
    new_name = str(number_of_folders).zfill(4)
    os.rename(f"{old_folder}/current", f"{old_folder}/{new_name}")
    os.mkdir(f"{old_folder}/current")


def fix_rule(rule: Dict[str, int]) -> List[Tuple[int, int]]:
    fixed_rule = []
    for key, value in rule.items():
        fixed_rule.append((int(key), value))
    fixed_rule.sort(key=lambda x: x[0], reverse=True)
    return fixed_rule


def main():
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = load_config(config_path)

    # Get parameters from config
    server_ignored_csv: str = os.path.join(config['path_to_output_folder'],
                                           config['output_structure']['ignored_table'])
    schedules_path: str = os.path.join(config['path_to_output_folder'],
                                       config['output_structure']['schedules_folder'],
                                       "current")
    server_profiler_csv: str = os.path.join(config['path_to_output_folder'],
                                            config['output_structure']['profiles_table'])
    number_of_workers: int = (config['downloader_parameters']['max_nodes']
                              * config['downloader_parameters']['workers_per_node'])
    schedule_rule_dict: List[Tuple[int, int]] = fix_rule(config['schedule_rule'])

    # Get list to download
    profiles_df = pd.read_csv(server_profiler_csv)

    if os.path.exists(server_ignored_csv):
        ignored_servers_df = pd.read_csv(server_ignored_csv)
    else:
        ignored_servers_df = pd.DataFrame(columns=["ServerName"])

    if len(os.listdir(schedules_path)) > 0:
        downloaded_batches: pd.DataFrame = verify_batches_for_prep(profiles_df, schedules_path)
        downloaded_batches = downloaded_batches.groupby("ServerName").count().reset_index().dropna()
        downloaded_batches = downloaded_batches.rename(
            columns={"ServerName": "server_name", "Status": "already_downloaded"})
        profiles_df = profiles_df.merge(downloaded_batches, on="server_name", how="left").fillna(0)
        profiles_df["left_to_download"] = profiles_df["total_batches"] - profiles_df["already_downloaded"]
    else:
        profiles_df["left_to_download"] = profiles_df["total_batches"]

    profiles_df["Nodes"] = profiles_df["left_to_download"].apply(lambda x: schedule_rule(x, schedule_rule_dict))
    profiles_df["ProcessPerNode"] = 1
    profiles_df = profiles_df.rename(columns={"total_batches": "TotalBatches"}).dropna().reset_index(drop=True)
    profiles_df = profiles_df[["ServerName", "TotalBatches", "ProcessPerNode", "Nodes"]]
    profiles_df = profiles_df[~profiles_df["ServerName"].isin(ignored_servers_df["ServerName"])]

    # Rename old schedule and logs
    init_new_current_folder(os.path.join(config['path_to_output_folder'],
                                         config['output_structure']['schedules_folder']))
    init_new_current_folder(os.path.join(config['path_to_output_folder'],
                                         config['output_structure']['logs_folder']))

    # Create schedules
    create_schedule_configs(profiles_df, number_of_workers, schedules_path)


if __name__ == "__main__":
    main()
