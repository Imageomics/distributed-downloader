import glob
import os
import uuid

import numpy as np
import pandas as pd

from distributed_downloader.utils import init_logger

# src_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia_copy/downloaded_images_copy"
# dst_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia_copy/source=gbif"
# prev_src_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia_copy/downloaded_images_copy"
# dst_name_table_path = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia_copy/tools/hashsum/name_table.csv"
# src_name_table_path = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia_copy/tools/hashsum_merging/name_table.csv"
src_folder = "/fs/scratch/PAS2136/gbif/processed/leftovers/multimedia/downloaded_images"
dst_folder = "/fs/ess/PAS2136/TreeOfLife/source=gbif/data"
prev_src_folder = "/fs/scratch/PAS2136/gbif/processed/2024-05-01/multimedia_prep/downloaded_images"
dst_name_table_path = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/tools/hashsum/name_table.csv"
src_name_table_path = "/fs/scratch/PAS2136/gbif/processed/leftovers/multimedia/tools/hashsum/name_table.csv"
src_data_name = "successes.parquet"
src_error_name = "errors.parquet"
dst_data_basename = "data_"
dst_errors_basename = "errors_"
server_name_regex = rf'{src_folder}/ServerName=(.*)/partition_id=.*'
partition_id_regex = rf"{src_folder}/ServerName=.*/partition_id=(.*)/.*"
basename_regex = rf'{src_folder}/ServerName=.*/partition_id=.*/(.*)'


def get_all_paths_to_merge(root_path: str, data_name: str, error_name: str) -> pd.DataFrame:
    glob_wildcard = root_path + "/*/*"
    result = []

    for path in glob.glob(glob_wildcard):
        if not os.path.exists(os.path.join(path, "completed")):
            continue
        result.append(os.path.join(path, data_name))
        result.append(os.path.join(path, error_name))

    return pd.DataFrame(result, columns=["src_path"])


def generate_dst_paths(src_paths_df: pd.DataFrame, prev_name_table_df: pd.DataFrame) -> pd.DataFrame:
    src_paths_df["server_name"] = src_paths_df["src_path"].str.extract(server_name_regex)
    src_paths_df["corrected_server_name"] = src_paths_df["server_name"].str.replace("%3A", "_")
    src_paths_df["partition_id"] = src_paths_df["src_path"].str.extract(partition_id_regex)
    src_paths_df["basename"] = src_paths_df["src_path"].str.extract(basename_regex)
    src_paths_df["dst_basename"] = np.where(src_paths_df["basename"] == src_data_name,
                                            dst_data_basename,
                                            dst_errors_basename)
    src_paths_df["uuid"] = src_paths_df.apply(lambda _: str(uuid.uuid4()), axis=1)
    src_paths_df["dst_proposed_path"] = (dst_folder
                                         + "/server=" + src_paths_df['corrected_server_name']
                                         + "/" + src_paths_df['dst_basename'] + src_paths_df['uuid'] + ".parquet")

    src_paths_df["prev_src_path"] = (prev_src_folder
                                     + "/ServerName=" + src_paths_df["server_name"]
                                     + "/partition_id=" + src_paths_df["partition_id"]
                                     + "/" + src_paths_df["basename"])
    src_paths_df = src_paths_df.merge(prev_name_table_df, how="left",
                                      left_on="prev_src_path",
                                      right_on="src_path",
                                      suffixes=("", "_y"),
                                      validate="1:1")
    # src_paths_df["is_new"] = np.where(src_paths_df["dst_path"].isna(), True, False)
    src_paths_df["dst_path"] = src_paths_df["dst_path"].fillna(value=src_paths_df["dst_proposed_path"])
    return src_paths_df[["src_path", "dst_path"]]


if __name__ == "__main__":
    logger = init_logger(__name__)

    paths_to_copy = get_all_paths_to_merge(src_folder, src_data_name, src_error_name)
    prev_name_table = pd.read_csv(dst_name_table_path)
    prev_name_table["dst_path"] = (prev_name_table["dst_path"]
                                   .str.replace("/fs/ess/PAS2136/TreeOfLife/source=gbif",
                                                "/fs/ess/PAS2136/TreeOfLife/source=gbif/data"))
    name_table = generate_dst_paths(paths_to_copy, prev_name_table)
    name_table.to_csv(src_name_table_path, index=False, header=True)
