import glob
import os
import shutil
from typing import Tuple

import pandas as pd

base_path = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia_copy"
purge_table = f"{base_path}/filtered_out"
tool_schedule_folder = f"{base_path}/tools"
filter_name = "duplicated"

max_nodes = 6
max_workers_per_node = 5
total_workers = max_nodes * max_workers_per_node

avg_completion_time = 120


def load_table(path: str) -> pd.DataFrame:
    all_files = glob.glob(os.path.join(path, "*.csv"))
    df: pd.DataFrame = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
    df = df[["ServerName", "partition_id"]]
    df = df.drop_duplicates(subset=["ServerName", "partition_id"]).reset_index(drop=True)
    df = df.rename(columns={"ServerName": "server_name"})
    return df


def load_table_research(path: str) -> pd.DataFrame:
    all_files = glob.glob(os.path.join(path, "*.csv"))
    df: pd.DataFrame = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
    df = df[["input_file"]]
    df = df.drop_duplicates(subset=["input_file"]).reset_index(drop=True)
    return df


def save_table(_table: pd.DataFrame, path: str, folder_name: str = None) -> None:
    if folder_name is None:
        folder_name = str(len([folder for folder in os.listdir(path) if os.path.isdir(f"{path}/{folder}")])).zfill(4)
    if not os.path.exists(f'{path}/{folder_name}'):
        # shutil.rmtree(f'{path}/{folder_name}')
        os.makedirs(f'{path}/{folder_name}')
    _table.to_csv(f'{path}/{folder_name}/schedule.csv', header=True, index=False)


def convert_time(total_seconds: float) -> Tuple[float, float, float]:
    _hours = total_seconds // 3600
    if _hours != 0:
        total_seconds %= 60
    _minutes = total_seconds // 60
    if _minutes != 0:
        total_seconds %= 60
    return _hours, _minutes, total_seconds


if __name__ == "__main__":
    table = load_table(f"{purge_table}/{filter_name}")
    table["rank"] = table.index % total_workers
    save_table(table, tool_schedule_folder, filter_name)
    table_gp = table.groupby(by=["rank"]).count()
    total_eta = table_gp.mean("rows")["server_name"] * avg_completion_time
    hours, minutes, seconds = convert_time(total_eta)
    print(f"Expected time: {hours:0>2.0f}:{minutes:0>2.0f}:{seconds:0>5.2f}")
