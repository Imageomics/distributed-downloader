import glob
import os

import pandas as pd
from attr import define

from tools.config import Config
from tools.tools_base import ToolsBase


@define
class SchedulerToolBase(ToolsBase):
    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)

    def create_schedule(self):
        raise NotImplementedError()


@define
class DefaultScheduler(SchedulerToolBase):
    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)

    def create_schedule(self):
        assert self.filter_name is not None, ValueError("filter name is not set")

        filter_folder = os.path.join(self.tools_path, self.filter_name)
        filter_table_folder = os.path.join(filter_folder, "filter_table")

        all_files = glob.glob(os.path.join(filter_table_folder, "*.csv"))
        df: pd.DataFrame = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
        df = df[["server_name", "partition_id"]]
        df = df.drop_duplicates(subset=["server_name", "partition_id"]).reset_index(drop=True)
        df["rank"] = df.index % self.total_workers

        df.to_csv(os.path.join(filter_folder, "schedule.csv"), header=True, index=False)


@define
class SizeBasedScheduler(DefaultScheduler):
    filter_name: str = "size_based"

    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)


@define
class DuplicatesBasedScheduler(DefaultScheduler):
    filter_name: str = "duplication_based"

    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)


@define
class ResizeToolScheduler(DefaultScheduler):
    filter_name: str = "resize"

    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)


@define
class ImageVerificationBasedScheduler(DefaultScheduler):
    filter_name: str = "image_verification"

    def __attrs_pre_init__(self, cfg: Config):
        super().__init__(cfg)
