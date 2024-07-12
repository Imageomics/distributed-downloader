import glob
import os
import shutil
import time
from typing import TextIO, List

import pandas as pd
import mpi4py.MPI as MPI

from distributed_downloader.utils import init_logger

csv_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/filtered_out"
schedule_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/tools"
filter_name = "duplicated_check"
verification_folder = f"{schedule_folder}/{filter_name}/verification"
os.makedirs(verification_folder, exist_ok=True)
logger = init_logger(__name__, logging_level="DEBUG")
total_time = 150


def load_table(folder: str, columns: List[str] = None) -> pd.DataFrame:
    all_files = glob.glob(os.path.join(folder, "*.csv"))
    if len(all_files) == 0:
        if columns is None:
            raise ValueError("No files found and columns are not defined")
        return pd.DataFrame(columns=columns)
    return pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)


def get_schedule(path: str, _rank: int) -> pd.DataFrame:
    schedule_df = load_table(path)
    schedule_df = schedule_df.query(f"rank == {_rank}")
    verification_df = load_table(verification_folder, ["input_file"])
    outer_join = schedule_df.merge(verification_df, how='outer', indicator=True, on=["input_file"])
    return outer_join[(outer_join["_merge"] == 'left_only')].drop('_merge', axis=1)


def filter_parquet(df_local: pd.DataFrame, verification_file: TextIO) -> int:
    filtering_df = df_local.reset_index(drop=True)

    parquet_path = filtering_df.iloc[0]["input_file"][5:]
    try:
        if time.time() > int(os.getenv("SLURM_JOB_END_TIME", 0)) - total_time:
            logger.error("Not enough time")
            return 0

        if not os.path.exists(parquet_path):
            logger.info(f"Path doesn't exists: {parquet_path}")
            return 1

        filtered_parquet = pd.read_parquet(parquet_path,
                                           # columns=["uuid"],
                                           filters=[("uuid", "not in", filtering_df["uuid"])]
                                           )

        if time.time() > int(os.getenv("SLURM_JOB_END_TIME", 0)) - total_time:
            logger.error("Not enough time")
            return 0

        if len(filtered_parquet) == 0:
            logger.info(f"Fully filtered out: {parquet_path}")

        filtered_parquet.to_parquet(parquet_path, index=False, compression="zstd", compression_level=3)
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        return 0
    else:
        print(parquet_path, end="\n", file=verification_file)
        logger.debug(f"Completed filtering: {parquet_path} with {len(filtered_parquet)}")
        return 1


if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.rank

    schedule = get_schedule(f"{schedule_folder}/{filter_name}", rank)
    comm.Barrier()
    if len(schedule) == 0:
        logger.error(f"Schedule not found or empty for rank {rank}")
        exit(0)

    df = load_table(f"{csv_folder}/{filter_name}")
    df = df.merge(schedule,
                  how="right",
                  on=["input_file"])
    df = df[["uuid", "gbif_id", "input_file"]]

    df_grouped: pd.api.typing.DataFrameGroupBy = df.groupby(["input_file"], group_keys=True)

    verification_file_name = f"{schedule_folder}/{filter_name}/verification/{str(rank).zfill(4)}.csv"
    if not os.path.exists(verification_file_name):
        verification_file = open(verification_file_name, "w")
        print("input_file", file=verification_file)
    else:
        verification_file = open(verification_file_name, "a")

    result = df_grouped.apply(filter_parquet, verification_file)

    verification_file.close()
    logger.info(result.sum())
