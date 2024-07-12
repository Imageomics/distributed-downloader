from typing import Tuple

import pandas as pd

base_path = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia_copy"
filter_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia_copy/tools/hashsum_merging/"
name_table = f"{filter_folder}/name_table.csv"

max_nodes = 6
max_workers_per_node = 5
total_workers = max_nodes * max_workers_per_node

avg_completion_time = 120


def load_table(path: str) -> pd.DataFrame:
    df: pd.DataFrame = pd.read_csv(path)
    return df


def save_table(_table: pd.DataFrame, path: str) -> None:
    _table.to_csv(f'{path}/schedule.csv', header=True, index=False)


def convert_time(total_seconds: float) -> Tuple[float, float, float]:
    _hours = total_seconds // 3600
    if _hours != 0:
        total_seconds %= 60
    _minutes = total_seconds // 60
    if _minutes != 0:
        total_seconds %= 60
    return _hours, _minutes, total_seconds


if __name__ == "__main__":
    table = load_table(name_table)
    table["rank"] = table.index % total_workers
    save_table(table, filter_folder)
    table_gp = table.groupby(by=["rank"]).count()
    total_eta = table_gp.mean("rows")["src_path"] * avg_completion_time
    hours, minutes, seconds = convert_time(total_eta)
    print(f"Expected time: {hours:0>2.0f}:{minutes:0>2.0f}:{seconds:0>5.2f}")
