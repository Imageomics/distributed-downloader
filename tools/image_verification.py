import glob
import hashlib
import os
import shutil
import time
from typing import TextIO, List

import cv2
import numpy as np
import pandas as pd
import mpi4py.MPI as MPI
from PIL import Image, UnidentifiedImageError

from distributed_downloader.utils import init_logger

schedule_folder = "/fs/scratch/PAS2136/gbif/processed/verification_test/multimedia/tools"
filter_name = "verification"
images_folder = "/fs/scratch/PAS2136/gbif/processed/2024-05-01/multimedia_prep/downloaded_images"
parquet_name = "successes.parquet"
verification_folder = f"{schedule_folder}/{filter_name}/verification"
os.makedirs(verification_folder, exist_ok=True)
corrupted_folder = f"{schedule_folder}/{filter_name}/corrupted"
os.makedirs(corrupted_folder, exist_ok=True)
logger = init_logger(__name__)
total_time = 150


def get_csv_writer(path: str, scheme: List[str]) -> TextIO:
    if not os.path.exists(path):
        file = open(path, "w")
        print(",".join(scheme), file=file)
    else:
        file = open(path, "a")
    return file


def is_enough_time() -> None:
    if time.time() > int(os.getenv("SLURM_JOB_END_TIME", 0)) - total_time:
        raise TimeoutError("Not enough time")


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
    verification_df = load_table(verification_folder, ["server_name", "partition_id"])
    outer_join = schedule_df.merge(verification_df, how='outer', indicator=True, on=["server_name", "partition_id"])
    return outer_join[(outer_join["_merge"] == 'left_only')].drop('_merge', axis=1)


def verify_image(row: pd.Series) -> pd.Series:
    # Feed in expected_dimensions and known_checksum from successes.parquet
    verified_image = pd.Series(data=(row["uuid"], True, ""),
                               index=("uuid", "is_verified", "error"))
    try:
        # Ensure no data-at-rest corruption from stray intergalactic cosmic rays ...
        image_bytes_checksum = hashlib.md5(row["image"]).hexdigest()  # Define elsewhere
        if image_bytes_checksum != row["hashsum_resized"]:
            raise ValueError("Checksum mismatch, image may be corrupted")

        # NumPy Array and Reshaping
        img_array = np.frombuffer(row["image"], dtype=np.uint8).reshape(
            (row["resized_size"][0], row["resized_size"][1], 3))

        # Convert BGR to RGB
        img_array = cv2.cvtColor(img_array, cv2.COLOR_BGR2RGB)

        # Convert the NumPy array to a PIL Image
        image = Image.fromarray(img_array)

        # Use PIL's verify method for a basic validation
        image.verify()

        # Validate range of pixel values
        if np.any(img_array > 255) or np.any(img_array < 0):
            raise ValueError("Pixel values are out of range")
    except (ValueError, UnidentifiedImageError, cv2.error) as e:
        # print(f"Data integrity issue detected: {e}")
        verified_image["is_verified"] = False
        verified_image["error"] = str(e)

    return verified_image


def verify_batch(df_local: pd.DataFrame,
                 verification_file: TextIO,
                 corrupted_file: TextIO) -> int:
    filtering_df = df_local.reset_index(drop=True)

    server_name = filtering_df.iloc[0]["server_name"]
    partition_id = filtering_df.iloc[0]["partition_id"]
    try:
        is_enough_time()

        parquet_path = os.path.join(
            images_folder,
            f"ServerName={server_name}",
            f"partition_id={partition_id}",
            parquet_name
        )

        if not os.path.exists(parquet_path):
            logger.info(f"Path doesn't exists: {server_name}/{partition_id}")
            return 1

        parquet_to_verify = pd.read_parquet(parquet_path)
        is_enough_time()

        if len(parquet_to_verify) != 0:
            verified_images = parquet_to_verify.apply(verify_image, axis=1)
            verified_parquet = parquet_to_verify.merge(verified_images, on="uuid", how="left", validate="1:1")
            corrupted_images: pd.DataFrame = verified_parquet.loc[~verified_parquet["is_verified"]]
            if len(corrupted_images) != 0:
                verified_parquet.loc[verified_parquet["is_verified"]].to_parquet(parquet_path,
                                                                                 index=False,
                                                                                 compression="zstd",
                                                                                 compression_level=3)
                corrupted_images = corrupted_images[["uuid", "error"]]
                corrupted_images["server_name"] = server_name
                corrupted_images["partition_id"] = partition_id
                corrupted_images.to_csv(corrupted_file, mode="a", header=True, index=False)
    except Exception as e:
        logger.error(f"Error occurred for {server_name}/{partition_id}: {e}")
        return 0
    else:
        print(f"{server_name},{partition_id}", end="\n", file=verification_file)
        logger.debug(f"Completed filtering: {server_name}/{partition_id}")
        return 1


if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.rank

    schedule = get_schedule(f"{schedule_folder}/{filter_name}", rank)
    comm.Barrier()
    if len(schedule) == 0:
        logger.error(f"Schedule not found or empty for rank {rank}")
        exit(0)

    df_grouped: pd.api.typing.DataFrameGroupBy = schedule.groupby(["server_name", "partition_id"], group_keys=True)

    verification_file = get_csv_writer(f"{verification_folder}/{str(rank).zfill(4)}.csv",
                                       ["server_name", "partition_id"])
    corrupted_file = get_csv_writer(f"{corrupted_folder}/{str(rank).zfill(4)}.csv",
                                    ["uuid", "server_name", "partition_id", "error"])

    result = df_grouped.apply(verify_batch, verification_file, corrupted_file)

    verification_file.close()
    logger.info(result.sum())
