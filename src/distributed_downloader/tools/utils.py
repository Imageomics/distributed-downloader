import logging
import os
import shutil
import subprocess
from typing import List, Sequence, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


def load_dataframe(spark: SparkSession, input_path: str, scheme: Optional[StructType | str] = None) -> DataFrame:
    file_extension = input_path.split('.')[-1].lower()

    def infer_delimiter(_first_line):
        if '\t' in _first_line:
            return '\t'
        elif ',' in _first_line:
            return ','
        elif ' ' in _first_line:
            return ' '
        elif '|' in _first_line:
            return '|'
        elif ';' in _first_line:
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
                raise ValueError(f"Could not infer delimiter for file {input_path}")
        df = spark.read.csv(input_path, sep=sep, header=True, schema=scheme)
    else:
        try:
            df = spark.read.load(input_path, scheme=scheme)
        except Exception as e:
            raise FileNotFoundError(f"File not supported: {e}")

    return df


def ensure_created(list_of_path: Sequence[str]) -> None:
    for path in list_of_path:
        os.makedirs(path, exist_ok=True)


def truncate_paths(paths: Sequence[str]) -> None:
    for path in paths:
        is_dir = "." not in path.split("/")[-1]
        if is_dir:
            if os.path.exists(path):
                shutil.rmtree(path)
            os.makedirs(path)
        else:
            open(path, "w").close()


def get_id(output: bytes) -> int:
    return int(output.decode().strip().split(" ")[-1])


def init_logger(logger_name: str, output_path: str = None, logging_level: str = "INFO") -> logging.Logger:
    logging.basicConfig(
        filename=output_path,
        level=logging.getLevelName(logging_level),
        format="%(asctime)s - %(levelname)s - %(process)d - %(message)s")
    return logging.getLogger(logger_name)


def submit_job(submitter_script: str, script: str, *args) -> int:
    output = subprocess.check_output(f"{submitter_script} {script} {' '.join(args)}", shell=True)
    idx = get_id(output)
    return idx


def preprocess_dep_ids(ids: List[int | None]) -> List[str]:
    return [str(_id) for _id in ids if _id is not None]
