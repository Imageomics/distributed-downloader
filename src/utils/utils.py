import os
import shutil
import sys
from collections import deque
from typing import List, Deque, Any, Dict

import pandas as pd
from pyspark.sql import DataFrame, SparkSession


def print_progress(iteration, total, prefix='', suffix='', decimals=2, bar_length=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '@' * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()


def generate_analyzer_sheet(columns: list[tuple[str, str]]) -> list:
    return [
        {
            "name": name,
            "type": column_type,
            "is_null": False,
            "is_nullable": False,
            "sparsity": 0,
            "is_unique": False,
            "is_atomic": False,
            "atomic_likelihood": False,
            "description": ""
        }
        for name, column_type in columns
    ]


def write_to_csv(path: str, result_df: DataFrame) -> None:
    result_df.coalesce(1).write.csv(path,
                                    header=True,
                                    mode="overwrite",
                                    sep="\t",
                                    quote="\"",
                                    quoteAll=True)


def write_to_parquet(path: str, result_df: DataFrame, num_parquet: int = 100) -> None:
    if num_parquet > 0:
        result_df = result_df.repartition(num_parquet)

    # Write the DataFrame to Parquet
    result_df.write.mode('overwrite').parquet(path)


def load_dataframe(spark: SparkSession, input_path: str) -> DataFrame:
    file_extension = input_path.split('.')[-1].lower()

    def infer_delimiter(first_line):
        if '\t' in first_line:
            return '\t'
        elif ',' in first_line:
            return ','
        elif ' ' in first_line:
            return ' '
        elif '|' in first_line:
            return '|'
        elif ';' in first_line:
            return ';'
        else:
            return None

    if file_extension in ['csv', 'tsv', 'txt']:
        if file_extension == 'csv':
            sep = ','
        elif file_extension == 'tsv':
            sep = '\t'
        elif file_extension == 'txt':
            with open(input_path, 'r') as file:
                first_line = file.readline()
                sep = infer_delimiter(first_line)
            if sep is None:
                raise Exception(f"Could not infer delimiter for file {input_path}")
        df = spark.read.csv(input_path, sep=sep, header=True)
    else:
        try:
            df = spark.read.load(input_path)
        except:
            raise Exception(f"File not supported")

    return df


def ensure_created(list_of_path: List[str]) -> None:
    for path in list_of_path:
        os.makedirs(path, exist_ok=True)


def truncate_folder(path: str):
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)


def split_dataframe(df: pd.DataFrame, by_column: str = "Nodes", chunk_size=20) -> List[pd.DataFrame]:
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
                            by_column: str = "Nodes") -> None:
    print("Creating schedules")

    group = group.sort_values(by=[by_column], ascending=False).reset_index()

    chunked_group: Deque[pd.DataFrame] = deque(split_dataframe(group, by_column, number_of_workers))
    all_schedules = [int(folder) for folder in os.listdir(schedule_path) if os.path.isdir(f"{schedule_path}/{folder}")]
    number_of_schedules = 0
    if len(all_schedules) > 0:
        number_of_schedules: int = sorted(all_schedules, reverse=True)[0] + 1

    while len(chunked_group) > 0:
        chunk = chunked_group.popleft()

        while len(chunked_group) > 0 and chunk["TotalBatches"].sum() < number_of_workers * 50:
            chunk = pd.concat([chunk, chunked_group.popleft()], ignore_index=True)

        chunk_folder = f"{schedule_path}/{number_of_schedules:0=4}"
        os.mkdir(chunk_folder)
        chunk.to_csv(f"{chunk_folder}/_config.csv", index=False, header=True)

        print(f"{number_of_schedules}={chunk['Nodes'].sum()}")

        number_of_schedules += 1


def load_env(env: str) -> Dict[str, Any]:
    from dotenv import load_dotenv, dotenv_values

    load_dotenv(env)
    return dotenv_values(env)
