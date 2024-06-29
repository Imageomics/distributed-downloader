import concurrent.futures
import glob
import hashlib
import os
import re
import shutil
import time
import uuid
from typing import List, Tuple, Sequence

import pandas as pd

from distributed_downloader.utils import ensure_created, init_logger

src_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/filtered_out/normal_images"
dst_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/filtered_out/normal_images_copy"
tools_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/tools/hashsum"
ensure_created([
    dst_folder,
    tools_folder
])
verification_csv = f"{tools_folder}/verification.csv"
src_data_name = "successes.parquet"
src_error_name = "errors.parquet"
dst_data_basename = "data_"
dst_errors_basename = "errors_"
BUF_SIZE = 65536
logger = init_logger(__name__)
total_time = 30

server_name_regex = rf'{src_folder}/ServerName=(.*)/partition_id=.*'
basename_regex = rf'{src_folder}/ServerName=.*/partition_id=.*/(.*)'


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


def get_all_paths(root_path: str, data_name: str, error_name: str) -> List[str]:
    glob_wildcards = [
        root_path + "/*/*/" + data_name,
        root_path + "/*/*/" + error_name
    ]

    result = []
    for wildcard in glob_wildcards:
        result.extend(glob.glob(wildcard))

    return result


def ensure_all_servers_exists(all_files: Sequence[str], dst_path: str) -> None:
    all_files_df = pd.DataFrame(all_files, columns=["src_path"])
    server_names_series: pd.Series = all_files_df["src_path"].str.extract(server_name_regex, expand=False)
    server_names = server_names_series.drop_duplicates().reset_index(drop=True).to_list()
    ensure_created([os.path.join(dst_path, f"server={server}") for server in server_names])


def get_server_name(path: str) -> str:
    match = re.match(server_name_regex, path)
    return match.group(1)


def get_basename(path: str) -> str:
    matched = re.match(basename_regex, path)
    basename = matched.group(1)
    if basename == src_data_name:
        return dst_data_basename
    elif basename == src_error_name:
        return dst_errors_basename
    else:
        raise ValueError(f"Unknown file name {basename} for path {path}")


def filter_already_done(all_files: List[str], verification_path: str) -> List[str]:
    if not os.path.exists(verification_path):
        verification_file = open(verification_path, "w")
        print("src_path,dst_path,hashsum_src,hashsum_dst", file=verification_file)
        verification_file.close()
        return all_files

    all_files_df = pd.DataFrame(all_files, columns=["src_path"])
    verification_df = pd.read_csv(verification_path)
    outer_join = all_files_df.merge(verification_df, how='outer', indicator=True, on=["src_path"])
    left_to_copy = outer_join[(outer_join["_merge"] == 'left_only')].drop('_merge', axis=1)["src_path"]
    return left_to_copy.to_list()


def copy_file(file_path: str) -> Tuple[bool, str, str, str, str]:
    try:
        is_enough_time()

        hs_src_alg = hashlib.md5()
        hs_src_local = compute_hashsum(file_path, hs_src_alg)
        new_path = f"{dst_folder}/server={get_server_name(file_path)}/{get_basename(file_path)}{str(uuid.uuid4())}.parquet"
        shutil.copy2(file_path, new_path)
        hs_dest_alg = hashlib.md5()
        hs_dest_local = compute_hashsum(new_path, hs_dest_alg)
        return False, file_path, new_path, hs_src_local, hs_dest_local
    except Exception as e:
        return True, file_path, str(e), "", ""


if __name__ == "__main__":
    paths_to_copy = get_all_paths(src_folder, src_data_name, src_error_name)
    paths_to_copy = filter_already_done(paths_to_copy, verification_csv)
    ensure_all_servers_exists(paths_to_copy, dst_folder)
    logger.info("Started copying")
    with open(verification_csv, "a") as verification_file:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for is_error, src, dst, hs_src, hs_dest in executor.map(copy_file, paths_to_copy):
                if is_error:
                    logger.error(f"Error {dst} for {src}")
                else:
                    print(f"{src},{dst},{hs_src},{hs_dest}", file=verification_file)
                    logger.debug(f"Copied file {src} to {dst}")

    logger.info("Finished copying")
