import argparse
import csv
import os.path
from logging import Logger

from distributed_downloader.tools.checkpoint import Checkpoint
from distributed_downloader.tools.config import Config

from typing import Optional, Dict

try:
    from typing import LiteralString
except ImportError:
    from typing_extensions import LiteralString

from attr import define, field, Factory

from distributed_downloader.core.initialization import init_filestructure
from distributed_downloader.tools.utils import submit_job, preprocess_dep_ids, init_logger


@define
class DistributedDownloader:
    config: Config

    logger: Logger = field(default=Factory(lambda: init_logger(__name__)))

    urls_path: str = None
    inner_checkpoint_path: str = None
    profiles_path: str = None
    schedules_folder: str = None

    inner_checkpoint: Checkpoint = None
    _checkpoint_override: Optional[Dict[str, bool]] = None
    default_checkpoint_structure = {
        "batched": False,
        "profiled": False,
        "schedule_creation_scheduled": False,
    }

    @classmethod
    def from_path(cls, path: str,
                  checkpoint_override: Optional[Dict[str, bool]] = None) -> "DistributedDownloader":
        return cls(config=Config.from_path(path, "downloader"),
                   checkpoint_override=checkpoint_override)

    def __attrs_post_init__(self):
        self.urls_path = self.config.get_folder("urls_folder")
        self.inner_checkpoint_path = self.config.get_folder("inner_checkpoint_file")
        self.profiles_path = self.config.get_folder("profiles_table")
        self.schedules_folder = os.path.join(self.config.get_folder("schedules_folder"), "current")

        self.inner_checkpoint = Checkpoint.from_path(self.inner_checkpoint_path, self.default_checkpoint_structure)
        if self._checkpoint_override is not None:
            for key, value in self._checkpoint_override.items():
                if key not in self.default_checkpoint_structure.keys():
                    raise KeyError("Unknown key for override in checkpoint")

                self.inner_checkpoint[key] = value

    def __init_environment(self) -> None:
        os.environ["CONFIG_PATH"] = self.config.config_path

        os.environ["ACCOUNT"] = self.config["account"]
        os.environ["PATH_TO_INPUT"] = self.config["path_to_input"]

        os.environ["PATH_TO_OUTPUT"] = self.config["path_to_output_folder"]
        for output_folder, output_path in self.config.folder_structure.items():
            os.environ["OUTPUT_" + output_folder.upper()] = output_path

        for downloader_var, downloader_value in self.config["downloader_parameters"].items():
            os.environ["DOWNLOADER_" + downloader_var.upper()] = str(downloader_value)

        self.logger.info("Environment initialized")

    def __schedule_initialization(self) -> int:
        self.logger.info("Scheduling initialization script")

        init_filestructure(self.config.folder_structure)

        idx = submit_job(self.config.get_script("general_submitter"),
                         self.config.get_script("initialization_script"))

        self.logger.info(f"Submitted initialization script {idx}")
        self.inner_checkpoint["batched"] = True
        return idx

    def __schedule_profiling(self, prev_job_id: int = None) -> int:
        self.logger.info("Scheduling profiling script")
        idx = submit_job(self.config.get_script("general_submitter"),
                         self.config.get_script("profiling_script"),
                         *preprocess_dep_ids([prev_job_id]))
        self.logger.info(f"Submitted profiling script {idx}")
        self.inner_checkpoint["profiled"] = True
        return idx

    def __schedule_downloading(self, prev_job_id: int = None) -> None:
        self.logger.info("Scheduling downloading scripts")

        if self.__check_downloading():
            self.logger.info("All images already downloaded")
            return

        all_prev_ids = [prev_job_id]

        if os.path.exists(self.schedules_folder):
            for schedule in os.listdir(self.schedules_folder):
                if not os.path.exists(os.path.join(self.schedules_folder, schedule, "_jobs_ids.csv")):
                    continue
                with open(os.path.join(self.schedules_folder, schedule, "_jobs_ids.csv"), "r") as file:
                    all_prev_ids.append(int(list(csv.DictReader(file))[-1]["job_id"]))

        schedule_creation_id = submit_job(self.config.get_script("schedule_creator_submitter"),
                                          self.config.get_script("schedule_creation_script"),
                                          *preprocess_dep_ids(all_prev_ids))
        self.logger.info(f"Submitted schedule creation script {schedule_creation_id}")
        self.inner_checkpoint["schedule_creation_scheduled"] = True

    def __check_downloading(self) -> bool:
        if not os.path.exists(self.schedules_folder):
            return False

        done = True
        for schedule in os.listdir(self.schedules_folder):
            schedule_path = os.path.join(self.schedules_folder, schedule)
            if os.path.exists(f"{schedule_path}/_UNCHANGED"):
                self.logger.warning(f"Schedule {schedule} is unchanged")
                if not self.config.get("suppress_unchanged_error", False):
                    raise ValueError(f"Schedule {schedule} is unchanged, which can lead to infinite loop")
            done = done and os.path.exists(f"{schedule_path}/_DONE")

        return done

    def download_images(self) -> None:
        self.__init_environment()

        initialization_job_id = None
        if (
                not os.path.exists(self.urls_path)
                or not self.inner_checkpoint["batched"]
        ):
            initialization_job_id = self.__schedule_initialization()
        else:
            self.logger.info("Skipping initialization script: already batched")

        profiling_job_id = None
        if (
                not os.path.exists(self.profiles_path)
                or not self.inner_checkpoint["profiled"]
        ):
            profiling_job_id = self.__schedule_profiling(initialization_job_id)
        else:
            self.logger.info("Skipping profiling script: already profiled")

        if not self.inner_checkpoint["schedule_creation_scheduled"]:
            self.__schedule_downloading(profiling_job_id)
        else:
            self.logger.error("Schedule creation already scheduled")


def main() -> None:
    parser = argparse.ArgumentParser(description='Distributed downloader')
    parser.add_argument("config_path", metavar="config_path", type=str,
                        help="the name of the tool that is intended to be used")
    parser.add_argument("--reset_batched", action="store_true", help="Will reset filtering and scheduling steps")
    parser.add_argument("--reset_profiled", action="store_true", help="Will reset scheduling step")
    _args = parser.parse_args()

    config_path = _args.config_path
    state_override = None
    if _args.reset_batched:
        state_override = {
            "batched": False,
            "profiled": False,
            "schedule_creation_scheduled": False
        }
    elif _args.reset_profiled:
        state_override = {
            "profiled": False,
            "schedule_creation_scheduled": False
        }

    dd = DistributedDownloader.from_path(config_path, state_override)
    dd.download_images()


if __name__ == "__main__":
    main()
