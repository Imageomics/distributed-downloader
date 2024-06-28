import os
from typing import Tuple

import pandas as pd

image_folder = "/fs/scratch/PAS2136/gbif/processed/2024-05-01/multimedia_prep/downloaded_images"
filter_name = "verification"
tool_schedule_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/tools"

max_nodes = 12
max_workers_per_node = 5
total_workers = max_nodes * max_workers_per_node

avg_completion_time = 60


def load_table(path: str) -> pd.DataFrame:
    all_schedules = []
    for folder in os.listdir(path):
        server_name = folder.split("=")[1]
        for partition in os.listdir(f"{path}/{folder}"):
            partition_path = f"{path}/{folder}/{partition}"
            if (not os.path.exists(f"{partition_path}/successes.parquet") or
                    not os.path.exists(f"{partition_path}/completed")):
                continue
            all_schedules.append([server_name, partition.split("=")[1]])
    return pd.DataFrame(all_schedules, columns=["server_name", "partition_id"])


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
    table = load_table(image_folder)
    table["rank"] = table.index % total_workers
    # save_table(table, tool_schedule_folder, filter_name)
    table_gp = table.groupby(by=["rank"]).count()
    total_eta = table_gp.max("rows")["server_name"] * avg_completion_time
    hours, minutes, seconds = convert_time(total_eta)
    print(f"Expected time: {hours:0>2.0f}:{minutes:0>2.0f}:{seconds:0>5.2f}")
