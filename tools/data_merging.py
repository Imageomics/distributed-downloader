import glob
import os
import shutil
import time
from typing import TextIO, List

import pandas as pd
import mpi4py.MPI as MPI

from distributed_downloader.utils import init_logger, ensure_created

src_folder = "/fs/scratch/PAS2136/gbif/processed/leftovers/multimedia/downloaded_images"
dst_folder = "/fs/ess/PAS2136/TreeOfLife/source=gbif/data"
merge_table_folder = "/fs/scratch/PAS2136/gbif/processed/leftovers/multimedia/tools/hashsum"
verification_folder = f"{merge_table_folder}/verification"
os.makedirs(verification_folder, exist_ok=True)
schedule_path = f"{merge_table_folder}/schedule.csv"
logger = init_logger(__name__, logging_level="DEBUG")
total_time = 200
server_name_regex = rf'{src_folder}/ServerName=(.*)/partition_id=.*'


def correct_server_name(server_names: List[str]) -> List[str]:
    for i, server in enumerate(server_names):
        server_names[i] = server.replace("%3A", "_")

    return server_names


def ensure_all_servers_exists(all_files_df: pd.DataFrame, dst_path: str) -> None:
    server_names_series: pd.Series = all_files_df["src_path"].str.extract(server_name_regex, expand=False)
    server_names = server_names_series.drop_duplicates().reset_index(drop=True).to_list()
    server_names = correct_server_name(server_names)
    ensure_created([os.path.join(dst_path, f"server={server}") for server in server_names])


def load_table(folder: str, columns: List[str] = None) -> pd.DataFrame:
    all_files = glob.glob(os.path.join(folder, "*.csv"))
    if len(all_files) == 0:
        if columns is None:
            raise ValueError("No files found and columns are not defined")
        return pd.DataFrame(columns=columns)
    return pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)


def get_schedule(path: str, _rank: int) -> pd.DataFrame:
    schedule_df = pd.read_csv(path)
    schedule_df = schedule_df.query(f"rank == {_rank}")
    verification_df = load_table(verification_folder, ["src_path"])
    outer_join = schedule_df.merge(verification_df, how='outer', indicator=True, on=["src_path"])
    return outer_join[(outer_join["_merge"] == 'left_only')].drop('_merge', axis=1)


def transfer_data(row: pd.Series):
    if time.time() > int(os.getenv("SLURM_JOB_END_TIME", 0)) - total_time:
        raise TimeoutError("Not enough time")

    if not os.path.exists(row["dst_path"]):
        logger.debug(f"Fully copping file {row['src_path']} to {row['dst_path']}")
        shutil.copy(row["src_path"], row["dst_path"])
        return

    src_df = pd.read_parquet(row["src_path"],
                             # columns=["uuid"],
                             )
    if len(src_df) == 0:
        logger.debug(f"Empty src file {row['src_path']} skipping")
        return

    dst_df = pd.read_parquet(row["dst_path"],
                             # columns=["uuid"],
                             )

    if len(dst_df) == 0:
        logger.debug(f"Empty dst file {row['dst_path']}, replacing with {row['src_path']}")
        shutil.copy(row["src_path"], row["dst_path"])
        return

    merged_df = pd.concat([src_df, dst_df]).reset_index(drop=True)

    if time.time() > int(os.getenv("SLURM_JOB_END_TIME", 0)) - total_time:
        raise TimeoutError("Not enough time")

    # logger.debug(f"{len(src_df)},{len(dst_df)},{len(merged_df)}")
    merged_df.to_parquet(row["dst_path"], index=False, compression="zstd", compression_level=3)


def filter_parquet(row: pd.Series, verification_file: TextIO) -> int:
    try:
        transfer_data(row)
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        return 0
    else:
        print(row["src_path"], end="\n", file=verification_file)
        logger.debug(f"Completed coping: {row['src_path']} to {row['dst_path']}")
        return 1


if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.rank

    schedule = get_schedule(f"{schedule_path}", rank)
    comm.Barrier()
    if len(schedule) == 0:
        logger.error(f"Schedule not found or empty for rank {rank}")
        exit(0)

    ensure_all_servers_exists(schedule, dst_folder)

    verification_file_name = f"{verification_folder}/{str(rank).zfill(4)}.csv"
    if not os.path.exists(verification_file_name):
        verification_file = open(verification_file_name, "w")
        print("src_path", file=verification_file)
    else:
        verification_file = open(verification_file_name, "a")

    result = schedule.apply(filter_parquet, axis=1, args=(verification_file,))

    verification_file.close()
    logger.info(result.sum())
