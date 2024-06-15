import hashlib
import os
import time
from typing import Tuple

import mpi4py.MPI as MPI

import cv2
import numpy as np
import pandas as pd
import py7zr
from PIL import Image, UnidentifiedImageError
from py7zr import FILTER_ZSTD

comm = MPI.COMM_WORLD
rank = comm.rank
output_path = "/users/PAS2119/andreykopanev/distributed_downloader/data/verification_stat"
schedule_path = "/users/PAS2119/andreykopanev/distributed_downloader/data/schedule_full.csv"
base_path = "/fs/scratch/PAS2136/gbif/processed/2024-05-01/multimedia_prep/downloaded_images"
_new_size = 720
read_time = 150
write_time = 150
schedule_df = pd.read_csv(schedule_path)
schedule = schedule_df.query(f"Rank == {rank}").set_index("ServerName").to_dict("index")

if len(schedule) == 0:
    raise ValueError(f"Empty schedule for rank {rank}")


def read_parquets(base_path: str, filename: str) -> pd.DataFrame:
    empty_df = pd.DataFrame()

    for folder, content in schedule.items():
        ids = str(content["Ids"]).split()
        for _id in ids:
            if not os.path.exists(f"{base_path}/ServerName={folder}/partition_id={_id}/{filename}"):
                continue

            new_df = pd.read_parquet(f"{base_path}/ServerName={folder}/partition_id={_id}/{filename}")
            new_df["ServerName"] = folder
            new_df["partition_id"] = int(_id)

            empty_df = pd.concat([empty_df, new_df]).reset_index(drop=True)

    return empty_df


def image_resize(image: np.ndarray,
                 max_size=720) -> Tuple[np.ndarray[int, np.dtype[np.uint8]], np.ndarray[int, np.dtype[np.uint32]]]:
    h, w = image.shape[:2]
    if h > w:
        new_h = max_size
        new_w = int(w * (new_h / h))
    else:
        new_w = max_size
        new_h = int(h * (new_w / w))
    return cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_AREA), np.array([new_h, new_w])


def validate_image_data(img_bytes: bytes,
                        expected_dimensions: np.ndarray[int, np.dtype[np.int32]],
                        known_checksum: str = None) -> Tuple[bool, str]:
    # Feed in expected_dimensions and known_checksum from successes.parquet
    try:
        # Ensure no data-at-rest corruption from stray intergalactic cosmic rays ...
        if known_checksum:
            image_bytes_checksum = hashlib.md5(img_bytes).hexdigest()  # Define elsewhere
            if image_bytes_checksum != known_checksum:
                raise ValueError("Checksum mismatch, image may be corrupted")

        # NumPy Array and Reshaping
        img_array = np.frombuffer(img_bytes, dtype=np.uint8).reshape(
            (expected_dimensions[0], expected_dimensions[1], 3))

        # Convert BGR to RGB
        img_array = cv2.cvtColor(img_array, cv2.COLOR_BGR2RGB)

        # Convert the NumPy array to a PIL Image
        image = Image.fromarray(img_array)

        # Use PIL's verify method for a basic validation
        image.verify()

        # Validate range of pixel values
        if np.any(img_array > 255) or np.any(img_array < 0):
            raise ValueError("Pixel values are out of range")

        return True, ""
    except (ValueError, UnidentifiedImageError, cv2.error) as e:
        # print(f"Data integrity issue detected: {e}")
        return False, str(e)


def resize_partition(partition: pd.DataFrame) -> pd.DataFrame:
    server_name = partition['ServerName'].iloc[0]
    partition_id = partition['partition_id'].iloc[0]
    partition_path = f"{base_path}/ServerName={server_name}/partition_id={partition_id}"
    print(f"Starting {server_name} {partition_id}")

    if not os.path.exists(partition_path):
        return pd.DataFrame(columns=["uuid", "identifier", "ServerName", "verified", "verification_msg"])

    if time.time() > int(os.getenv("SLURM_JOB_END_TIME", 0)) - (write_time + read_time):
        print(
            f"Not enough time to resize {int(os.getenv('SLURM_JOB_END_TIME', 0)) - time.time()} left, {write_time + read_time} needed")
        return pd.DataFrame(columns=["uuid", "identifier", "ServerName", "verified", "verification_msg"])

    partition_dict = partition.to_dict("index")

    verification_dict = {}

    try:
        with py7zr.SevenZipFile(f"{partition_path}/images.7z", 'r', filters=[{'id': FILTER_ZSTD, 'level': 3}]) as f:
            names = f.getnames()
            for fname, bio in f.read(names).items():
                image_stream = bio.read()

                image_shape: np.ndarray[int, np.dtype[np.int32]] = partition_dict[fname]["resized_size"]
                image_original_np: np.ndarray = np.frombuffer(image_stream, dtype=np.uint8).reshape(
                    [image_shape[0], image_shape[1], 3])

                is_valid, error_msg = validate_image_data(image_stream, image_shape,
                                                          partition_dict[fname]["hashsum_resized"])
                verification_dict[fname] = {
                    "identifier": partition_dict[fname]["identifier"],
                    "ServerName": server_name,
                    "verified": is_valid,
                    "verification_msg": error_msg}

                if image_shape[0] > _new_size or image_shape[1] > _new_size:
                    image_original_np, image_shape = image_resize(image_original_np, _new_size)

                image_stream = image_original_np.tobytes()
                partition_dict[fname]["hashsum_resized"] = hashlib.md5(image_stream).hexdigest()
                partition_dict[fname]["resized_size"] = image_shape
                partition_dict[fname]["image"] = image_stream
    except Exception as e:
        corrupted = open(f"{partition_path}/_corrupted.txt", "w")
        print(str(e), file=corrupted)
        corrupted.close()
        print(f"Error: {server_name}: {e}", flush=True)
        return pd.DataFrame(columns=["uuid", "identifier", "ServerName", "verified", "verification_msg"])
    else:
        os.remove(f"{partition_path}/images.7z")
        (pd.DataFrame
         .from_dict(partition_dict, orient="index")
         .reset_index(names="uuid")
         .drop(columns=["ServerName", "partition_id"])
         .to_parquet(f"{partition_path}/successes.parquet", index=False, compression="zstd", compression_level=3))

        verification_df = pd.DataFrame.from_dict(verification_dict, orient="index").reset_index(names="uuid")

        verification_df.drop(columns=["identifier", "ServerName"]).to_parquet(f"{partition_path}/verification.parquet",
                                                                              index=False)

        return verification_df


if __name__ == "__main__":
    successes_df = read_parquets(base_path, "successes.parquet").set_index("uuid")
    successes_grouped = successes_df.groupby(["ServerName", "partition_id"])

    (successes_grouped
     .apply(resize_partition, include_groups=True)
     .reset_index(drop=True).drop(columns=["uuid"])
     .groupby(["ServerName", "verified"])
     .count()
     .reset_index(names=["ServerName", "verified"])
     .to_parquet(f"{output_path}/ver_{rank}.parquet", index=False))
