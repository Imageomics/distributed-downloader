import glob
import os
import uuid
from typing import List

import numpy as np
import pandas as pd

from distributed_downloader.utils import init_logger, ensure_created

src_folder = "/fs/scratch/PAS2136/gbif/processed/2024-05-01/multimedia_prep/downloaded_images"
dst_folder = "/fs/ess/PAS2136/TreeOfLife/source=gbif"
tools_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/tools/hashsum"
name_table_path = f"{tools_folder}/name_table.csv"
src_data_name = "successes.parquet"
src_error_name = "errors.parquet"
dst_data_basename = "data_"
dst_errors_basename = "errors_"
server_name_regex = rf'{src_folder}/ServerName=(.*)/partition_id=.*'
basename_regex = rf'{src_folder}/ServerName=.*/partition_id=.*/(.*)'


def get_all_paths(root_path: str, data_name: str, error_name: str) -> List[str]:
    glob_wildcard = root_path + "/*/*"
    result = []

    for path in glob.glob(glob_wildcard):
        if not os.path.exists(os.path.join(path, "completed")):
            continue
        result.append(os.path.join(path, data_name))
        result.append(os.path.join(path, error_name))

    return result


def generate_dst_paths(src_paths: List[str]) -> pd.DataFrame:
    src_paths_df = pd.DataFrame(src_paths, columns=["src_path"])
    src_paths_df["server_name"] = src_paths_df["src_path"].str.extract(server_name_regex)
    src_paths_df["corrected_server_name"] = src_paths_df["server_name"].str.replace("%3A", "_")
    src_paths_df["basename"] = src_paths_df["src_path"].str.extract(basename_regex)
    src_paths_df["dst_basename"] = np.where(src_paths_df["basename"] == src_data_name,
                                            dst_data_basename,
                                            dst_errors_basename)
    src_paths_df["uuid"] = src_paths_df.apply(lambda _: str(uuid.uuid4()), axis=1)
    src_paths_df["dst_path"] = (dst_folder
                                + "/server=" + src_paths_df['corrected_server_name']
                                + "/" + src_paths_df['dst_basename'] + src_paths_df['uuid'] + ".parquet")
    return src_paths_df[["src_path", "dst_path"]]


if __name__ == "__main__":
    logger = init_logger(__name__)
    ensure_created([
        dst_folder,
        tools_folder
    ])

    paths_to_copy = get_all_paths(src_folder, src_data_name, src_error_name)
    name_table = generate_dst_paths(paths_to_copy)
    name_table.to_csv(name_table_path, index=False, header=True)
