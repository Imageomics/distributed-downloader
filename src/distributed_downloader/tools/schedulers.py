import glob
import os

import pandas as pd

from distributed_downloader.tools.config import Config
from distributed_downloader.tools.registry import ToolsBase, ToolsRegistryBase


class SchedulerToolBase(ToolsBase):

    def __init__(self, cfg: Config):
        super().__init__(cfg)

        self.filter_family = "scheduler"


class DefaultScheduler(SchedulerToolBase):

    def __init__(self, cfg: Config):
        super().__init__(cfg)

    def run(self):
        assert self.filter_name is not None, ValueError("filter name is not set")

        filter_folder = os.path.join(self.tools_path, self.filter_name)
        filter_table_folder = os.path.join(filter_folder, "filter_table")

        all_files = glob.glob(os.path.join(filter_table_folder, "*.csv"))
        df: pd.DataFrame = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
        df = df[["server_name", "partition_id"]]
        df = df.drop_duplicates(subset=["server_name", "partition_id"]).reset_index(drop=True)
        df["rank"] = df.index % self.total_workers

        df.to_csv(os.path.join(filter_folder, "schedule.csv"), header=True, index=False)


@ToolsRegistryBase.register("scheduler", "size_based")
class SizeBasedScheduler(DefaultScheduler):

    def __init__(self, cfg: Config):
        super().__init__(cfg)

        self.filter_name: str = "size_based"


@ToolsRegistryBase.register("scheduler", "duplication_based")
class DuplicatesBasedScheduler(DefaultScheduler):

    def __init__(self, cfg: Config):
        super().__init__(cfg)

        self.filter_name: str = "duplication_based"


@ToolsRegistryBase.register("scheduler", "resize")
class ResizeToolScheduler(DefaultScheduler):

    def __init__(self, cfg: Config):
        super().__init__(cfg)

        self.filter_name: str = "resize"


@ToolsRegistryBase.register("scheduler", "image_verification")
class ImageVerificationBasedScheduler(DefaultScheduler):

    def __init__(self, cfg: Config):
        super().__init__(cfg)

        self.filter_name: str = "image_verification"