import glob
import hashlib
import os
import time
from typing import List, TextIO, Tuple

import cv2
import numpy as np
import pandas as pd
from PIL import UnidentifiedImageError, Image
from attr import define
import mpi4py.MPI as MPI

from tools.config import Config
from tools.tools_base import ToolsBase


@define
class RunnerToolBase(ToolsBase):
    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)

    def perform_filtering(self):
        raise NotImplementedError()


@define
class MPIRunnerTool(RunnerToolBase):
    filter_folder: str = None
    filter_table_folder: str = None
    verification_folder: str = None
    verification_IO: TextIO = None

    data_scheme: List[str] = None
    verification_scheme: List[str] = None

    mpi_rank: int = None
    mpi_comm: MPI.Intracomm = None
    total_time: int = None

    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)

    def __attrs_post_init__(self):
        self.mpi_comm = MPI.COMM_WORLD
        self.mpi_rank = self.mpi_comm.rank

    def is_enough_time(self):
        assert self.total_time is not None, ValueError("total_time is not set")
        if time.time() > int(os.getenv("SLURM_JOB_END_TIME", 0)) - self.total_time:
            raise TimeoutError("Not enough time")

    @staticmethod
    def load_table(folder: str, columns: List[str] = None) -> pd.DataFrame:
        all_files = glob.glob(os.path.join(folder, "*.csv"))
        if len(all_files) == 0:
            assert columns is not None, ValueError("No files found and columns are not defined")

            return pd.DataFrame(columns=columns)
        return pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

    @staticmethod
    def get_csv_writer(path: str, scheme: List[str]) -> TextIO:
        if not os.path.exists(path):
            file = open(path, "w")
            print(",".join(scheme), file=file)
        else:
            file = open(path, "a")
        return file

    def ensure_folders_created(self):
        assert self.filter_name is not None, ValueError("filter name is not set")
        assert self.verification_scheme is not None, ValueError("verification scheme is not set")

        self.filter_folder = os.path.join(self.tools_path, self.filter_name)
        self.filter_table_folder = os.path.join(self.filter_folder, "filter_table")
        self.verification_folder = os.path.join(self.tools_path, self.filter_name, "verification")

        os.makedirs(self.verification_folder, exist_ok=True)

        self.verification_IO = self.get_csv_writer(f"{self.verification_folder}/{str(self.mpi_rank).zfill(4)}.csv",
                                                   self.verification_scheme)

    def get_schedule(self):
        schedule_df = pd.read_csv(os.path.join(self.filter_folder, "schedule.csv"))
        schedule_df = schedule_df.query(f"rank == {self.mpi_rank}")
        verification_df = self.load_table(self.verification_folder, ["server_name", "partition_id"])
        outer_join = schedule_df.merge(verification_df, how='outer', indicator=True, on=["server_name", "partition_id"])
        return outer_join[(outer_join["_merge"] == 'left_only')].drop('_merge', axis=1)

    def get_remaining_table(self, schedule: pd.DataFrame) -> pd.api.typing.DataFrameGroupBy:
        assert self.data_scheme is not None, ValueError("data scheme is not set")

        df = self.load_table(self.filter_table_folder)
        df = df.merge(schedule,
                      how="right",
                      on=["server_name", "partition_id"])
        df = df[self.data_scheme]

        return df.groupby(["server_name", "partition_id"], group_keys=True)

    def apply_filter(self, filtering_df: pd.DataFrame, server_name: str, partition_id: str) -> int:
        raise NotImplementedError()

    def runner_fn(self, df_local: pd.DataFrame) -> int:
        filtering_df = df_local.reset_index(drop=True)
        server_name = filtering_df.iloc[0]["server_name"]
        partition_id = filtering_df.iloc[0]["partition_id"]
        try:
            filtered_parquet_length = self.apply_filter(filtering_df, server_name, partition_id)
        except NotImplementedError as not_impl:
            raise NotImplementedError("Filter function wasn't implemented")
        except Exception as e:
            self.logger.error(f"Error occurred: {e}")
            return 0
        else:
            print(f"{server_name},{partition_id}", end="\n", file=self.verification_IO)
            self.logger.debug(f"Completed filtering: {server_name}/{partition_id} with {filtered_parquet_length}")
            return 1

    def main(self):
        self.ensure_folders_created()

        schedule = self.get_schedule()
        self.mpi_comm.Barrier()
        if len(schedule) == 0:
            self.logger.error(f"Schedule not found or empty for rank {self.mpi_rank}")
            exit(0)

        remaining_table = self.get_remaining_table(schedule)

        remaining_table.apply(self.runner_fn)

    def __del__(self):
        self.verification_IO.close()


@define
class FilterRunnerTool(MPIRunnerTool):
    data_scheme: List[str] = ["uuid", "gbif_id", "server_name", "partition_id"]
    verification_scheme: List[str] = ["server_name", "partition_id"]
    total_time = 150

    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)

    def apply_filter(self, filtering_df: pd.DataFrame, server_name: str, partition_id: str) -> int:
        self.is_enough_time()

        parquet_path = os.path.join(
            self.downloaded_images_path,
            f"ServerName={server_name}",
            f"partition_id={partition_id}",
            "successes.parquet"
        )

        if not os.path.exists(parquet_path):
            self.logger.info(f"Path doesn't exists: {server_name}/{partition_id}")
            return 0

        filtered_parquet = pd.read_parquet(parquet_path,
                                           filters=[("uuid", "not in", filtering_df["uuid"])]
                                           )

        self.is_enough_time()

        if len(filtered_parquet) == 0:
            self.logger.info(f"Fully filtered out: {server_name}/{partition_id}")

        filtered_parquet.to_parquet(parquet_path, index=False, compression="zstd", compression_level=3)

        return len(filtered_parquet)


@define
class DuplicationFilterRunnerTool(FilterRunnerTool):
    filter_name = "duplication_based"

    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)


@define
class SizeBasedFilterRunnerTool(FilterRunnerTool):
    filter_name: str = "size_based"

    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)


@define
class ImageVerificationRunnerTool(MPIRunnerTool):
    filter_name: str = "image_verification"

    data_scheme: List[str] = ["server_name", "partition_id"]
    verification_scheme: List[str] = ["server_name", "partition_id"]
    corrupted_folder: str = None
    corrupted_scheme: List[str] = ["uuid", "gbif_id", "server_name", "partition_id"]
    corrupted_IO: TextIO = None
    total_time = 150

    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)

    def ensure_folders_created(self):
        assert self.filter_name is not None, ValueError("filter name is not set")
        assert self.verification_scheme is not None, ValueError("verification scheme is not set")
        assert self.corrupted_scheme is not None, ValueError("corrupted scheme is not set")

        self.filter_folder = os.path.join(self.tools_path, self.filter_name)
        self.filter_table_folder = os.path.join(self.filter_folder, "filter_table")
        self.verification_folder = os.path.join(self.tools_path, self.filter_name, "verification")
        self.corrupted_folder = os.path.join(self.tools_path, self.filter_name, "corrupted")

        os.makedirs(self.verification_folder, exist_ok=True)
        os.makedirs(self.corrupted_folder, exist_ok=True)

        self.verification_IO = self.get_csv_writer(f"{self.verification_folder}/{str(self.mpi_rank).zfill(4)}.csv",
                                                   self.verification_scheme)
        self.corrupted_IO = self.get_csv_writer(f"{self.corrupted_folder}/{str(self.mpi_rank).zfill(4)}.csv",
                                                self.corrupted_scheme)

    def apply_filter(self, filtering_df: pd.DataFrame, server_name: str, partition_id: str) -> int:
        self.is_enough_time()

        parquet_path = os.path.join(
            self.downloaded_images_path,
            f"ServerName={server_name}",
            f"partition_id={partition_id}",
            "successes.parquet"
        )

        if not os.path.exists(parquet_path):
            self.logger.info(f"Path doesn't exists: {server_name}/{partition_id}")
            return 0

        parquet_to_verify = pd.read_parquet(parquet_path)
        parquet_to_verify_length = len(parquet_to_verify)
        self.is_enough_time()

        if parquet_to_verify_length != 0:
            verified_images = parquet_to_verify.apply(self.verify_image, axis=1)
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
                corrupted_images.to_csv(self.corrupted_IO, mode="a", header=True, index=False)

            return parquet_to_verify_length - len(corrupted_images)
        return parquet_to_verify_length

    @staticmethod
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


@define
class ResizeRunnerTool(MPIRunnerTool):
    filter_name: str = "resize"

    data_scheme: List[str] = ["server_name", "partition_id"]
    verification_scheme: List[str] = ["server_name", "partition_id"]
    total_time = 300
    new_size: int = None

    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)

    def __attrs_post_init__(self):
        assert isinstance(self.config["tools_parameters"]["new_resize_size"], int), (
            ValueError("new size have to be Integer"))
        self.new_size = self.config["tools_parameters"]["new_resize_size"]

    def apply_filter(self, filtering_df: pd.DataFrame, server_name: str, partition_id: str) -> int:
        self.is_enough_time()

        parquet_path = os.path.join(
            self.downloaded_images_path,
            f"ServerName={server_name}",
            f"partition_id={partition_id}",
            "successes.parquet"
        )

        if not os.path.exists(parquet_path):
            self.logger.info(f"Path doesn't exists: {server_name}/{partition_id}")
            return 0

        parquet_to_resize = pd.read_parquet(parquet_path)
        initial_scheme = parquet_to_resize.columns

        self.is_enough_time()
        resized_parquet = parquet_to_resize.apply(self.resize_partition, axis=1)

        parquet_to_resize = parquet_to_resize.merge(resized_parquet,
                                                    on="uuid",
                                                    how="inner",
                                                    validate="1:1",
                                                    suffixes=("_x", ""))
        parquet_to_resize = parquet_to_resize[initial_scheme]

        self.is_enough_time()
        parquet_to_resize.to_parquet(parquet_path, index=False, compression="zstd", compression_level=3)

    def resize_partition(self, row: pd.Series) -> pd.Series:
        image_shape: np.ndarray[int, np.dtype[np.int32]] = row["resized_size"]
        image_original_np: np.ndarray = np.frombuffer(row["image"], dtype=np.uint8).reshape(
            [image_shape[0], image_shape[1], 3])

        if image_shape[0] > self.new_size or image_shape[1] > self.new_size:
            image_original_np, image_shape = self.image_resize(image_original_np)

        return pd.Series({"uuid": row["uuid"], "resized_size": image_shape, "image": image_original_np},
                         index=["uuid", "resized_size", "image"])

    def image_resize(self, image: np.ndarray) \
            -> Tuple[np.ndarray[int, np.dtype[np.uint8]], np.ndarray[int, np.dtype[np.uint32]]]:
        h, w = image.shape[:2]
        if h > w:
            new_h = self.new_size
            new_w = int(w * (new_h / h))
        else:
            new_w = self.new_size
            new_h = int(h * (new_w / w))
        return cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_AREA), np.array([new_h, new_w])