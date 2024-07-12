# import concurrent.futures
import hashlib
import os
import shutil
import time
from typing import List, Tuple

import pandas as pd
from mpi4py.futures import MPIPoolExecutor

from distributed_downloader.utils import ensure_created, init_logger

src_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia_copy/downloaded_images_copy"
dst_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia_copy/source=gbif"
tools_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia_copy/tools/hashsum"
verification_csv = f"{tools_folder}/verification.csv"
name_table_path = f"{tools_folder}/name_table.csv"
BUF_SIZE = 131_072
total_time = 60

server_name_regex = rf'{src_folder}/ServerName=(.*)/partition_id=.*'


def is_enough_time() -> None:
    if time.time() > int(os.getenv("SLURM_JOB_END_TIME", 0)) - total_time:
        raise TimeoutError("Not enough time")


def compute_hashsum(file_path: str, hashsum_alg) -> str:
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            hashsum_alg.update(data)
    return hashsum_alg.hexdigest()


def get_all_paths(name_table_csv: str) -> pd.DataFrame:
    _name_table = pd.read_csv(name_table_csv)
    return _name_table


def correct_server_name(server_names: List[str]) -> List[str]:
    for i, server in enumerate(server_names):
        server_names[i] = server.replace("%3A", "_")

    return server_names


def ensure_all_servers_exists(all_files_df: pd.DataFrame, dst_path: str) -> None:
    server_names_series: pd.Series = all_files_df["src_path"].str.extract(server_name_regex, expand=False)
    server_names = server_names_series.drop_duplicates().reset_index(drop=True).to_list()
    server_names = correct_server_name(server_names)
    ensure_created([os.path.join(dst_path, f"server={server}") for server in server_names])


def filter_already_done(all_files_df: pd.DataFrame, verification_path: str) -> pd.DataFrame:
    if not os.path.exists(verification_path):
        verification_file = open(verification_path, "w")
        print("src_path,dst_path,hashsum_src,hashsum_dst", file=verification_file)
        verification_file.close()
        return all_files_df

    verification_df = pd.read_csv(verification_path)
    outer_join = all_files_df.merge(verification_df, how='outer', indicator=True, on=["src_path", "dst_path"])
    left_to_copy = outer_join[(outer_join["_merge"] == 'left_only')].drop('_merge', axis=1)[["src_path", "dst_path"]]
    return left_to_copy


def copy_file(row: Tuple[pd.Index, pd.Series]) -> Tuple[bool, str, str, str, str]:
    src_path = row[1]["src_path"]
    dst_path = row[1]["dst_path"]
    try:
        is_enough_time()

        hs_src_alg = hashlib.md5()
        hs_src_local = compute_hashsum(src_path, hs_src_alg)

        is_enough_time()

        shutil.copy(src_path, dst_path)

        is_enough_time()

        hs_dest_alg = hashlib.md5()
        hs_dest_local = compute_hashsum(dst_path, hs_dest_alg)
        return False, src_path, dst_path, hs_src_local, hs_dest_local
        # return False, src_path, dst_path, "", ""
    except Exception as e:
        return True, src_path, str(e), "", ""


if __name__ == "__main__":
    logger = init_logger(__name__)
    ensure_created([
        dst_folder,
        tools_folder
    ])

    name_table = get_all_paths(name_table_path)
    name_table = filter_already_done(name_table, verification_csv)
    ensure_all_servers_exists(name_table, dst_folder)

    if len(name_table) == 0:
        raise ValueError("Schedule is empty or doesn't exists")

    logger.info("Started copying")
    logger.info(f"{len(name_table)} files left to copy")
    with open(verification_csv, "a") as verification_file:
        with MPIPoolExecutor() as executor:
            for is_error, src, dst, hs_src, hs_dest in executor.map(copy_file, name_table.iterrows()):
                if is_error:
                    logger.error(f"Error {dst} for {src}")
                else:
                    print(src, dst, hs_src, hs_dest, sep=",", file=verification_file, flush=True)
                    logger.debug(f"Copied file {src} to {dst}")

    logger.info("Finished copying")
