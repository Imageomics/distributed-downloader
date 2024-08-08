import os
from collections import deque
from typing import List, Deque, Dict, Any

try:
    from typing import LiteralString
except ImportError:
    from typing_extensions import LiteralString

import pandas as pd


def verify_downloaded_batches(schedule_row: pd.Series, input_path: str) -> List[Dict[str, Any]]:
    server_name = schedule_row["server_name"]
    server_start_idx = schedule_row["start_index"]
    server_end_idx = schedule_row["end_index"]
    verified_batches: List[Dict[str, Any]] = []

    if os.path.exists(
            f"{input_path}/server_name={server_name}"):  # TODO: Make "server_name" changeable column from config
        server_batches_names = os.listdir(f"{input_path}/server_name={server_name}")
        for batch_name in server_batches_names:
            if not os.path.isdir(f"{input_path}/server_name={server_name}/{batch_name}"):
                continue

            batch_idx = int(batch_name.split("=")[1])
            if server_start_idx > batch_idx or server_end_idx < batch_idx:
                continue

            if os.path.exists(f"{input_path}/server_name={server_name}/{batch_name}/completed"):
                verified_batches.append({"server_name": server_name, "partition_id": batch_idx, "status": "Completed"})
            elif os.path.exists(f"{input_path}/server_name={server_name}/{batch_name}/failed"):
                verified_batches.append({"server_name": server_name, "partition_id": batch_idx, "status": "Failed"})

    return verified_batches


def verify_batches_for_prep(schedule_row: pd.DataFrame, input_path: str) -> pd.DataFrame:
    schedule_row["start_index"] = 0
    schedule_row["end_index"] = schedule_row["total_batches"]

    verification_df = pd.DataFrame(columns=["server_name", "partition_id", "status"])

    for idx, row in schedule_row.iterrows():
        new_verification_df = verify_downloaded_batches(row, input_path)
        verification_df = pd.concat([verification_df, pd.DataFrame(new_verification_df)],
                                    ignore_index=True).drop_duplicates()

    return verification_df


def split_dataframe(df: pd.DataFrame, by_column: str = "nodes", chunk_size=20) -> List[pd.DataFrame]:
    chunks: List[pd.DataFrame] = []

    row_list = df.to_dict("records")

    if len(row_list) == 0:
        raise ValueError("Empty list")

    chunks.append(pd.DataFrame(row_list[0], index=[0]))
    del row_list[0]

    while len(row_list) > 0:
        i = 0

        chunk = chunks[-1]

        while len(row_list) > 0 and i < len(row_list):
            new_chunk = row_list[i]
            column_value = chunk[by_column].sum() + new_chunk[by_column]

            if column_value <= chunk_size:
                chunks[-1] = pd.concat([chunk, pd.DataFrame(new_chunk, index=[0])], ignore_index=True)
                del row_list[i]
                break

            i += 1
        else:
            if len(row_list) == 0:
                break

            chunks.append(pd.DataFrame(row_list[0], index=[0]))
            del row_list[0]

    return chunks


def create_schedule_configs(group: pd.DataFrame, number_of_workers: int, schedule_path: str,
                            by_column: str = "nodes") -> None:
    group = group.sort_values(by=[by_column], ascending=False).reset_index()

    chunked_group: Deque[pd.DataFrame] = deque(split_dataframe(group, by_column, number_of_workers))
    all_schedules = [int(folder) for folder in os.listdir(schedule_path) if os.path.isdir(f"{schedule_path}/{folder}")]
    number_of_schedules = 0
    if len(all_schedules) > 0:
        number_of_schedules: int = sorted(all_schedules, reverse=True)[0] + 1

    while len(chunked_group) > 0:
        chunk = chunked_group.popleft()

        while len(chunked_group) > 0 and chunk["total_batches"].sum() < number_of_workers * 50:
            chunk = pd.concat([chunk, chunked_group.popleft()], ignore_index=True)

        chunk_folder = f"{schedule_path}/{number_of_schedules:0=4}"
        os.mkdir(chunk_folder)
        chunk.to_csv(f"{chunk_folder}/_config.csv", index=False, header=True)

        number_of_schedules += 1
