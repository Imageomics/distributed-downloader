import os
from logging import Logger
from pprint import pprint
from typing import Literal, List, Dict, Optional, Any, TextIO, Tuple

import pandas as pd
import yaml
from attr import define, field, Factory

from distributed_downloader.utils import submit_job, init_logger, ensure_created, update_checkpoint, preprocess_dep_ids
from tools.config import Config
from tools.registry import ToolsRegistryBase

checkpoint_scheme = {
    "filtered": False,
    "schedule_created": False,
    "completed": False
}


@define
class Tools:
    config: Config
    tool_name: str

    logger: Logger = field(default=Factory(lambda: init_logger(__name__)))

    config_path: str = None
    tool_folder: str = None
    tool_job_history_path: str = None
    tool_checkpoint_path: str = None

    tool_checkpoint: Dict[str, bool] = None
    tool_job_history: List[int] = None
    tool_job_history_IO: TextIO = None

    @classmethod
    def from_path(cls, path: str, tool_name: str) -> "Tools":
        if tool_name not in ToolsRegistryBase.TOOLS_REGISTRY.keys():
            ValueError("unknown tool name")

        return cls(config=Config.from_path(path),
                   tool_name=tool_name,
                   config_path=path)

    def __attrs_post_init__(self):
        # noinspection PyTypeChecker
        self.tool_folder: str = os.path.join(self.config['path_to_output_folder'],
                                             self.config['output_structure']['tools_folder'],
                                             self.tool_name)
        self.tool_job_history_path: str = os.path.join(self.tool_folder, "job_history.csv")
        self.tool_checkpoint_path: str = os.path.join(self.tool_folder, "tool_checkpoint.yaml")

        self.__init_environment()
        self.__init_filestructure()

    def __init_environment(self) -> None:
        os.environ["CONFIG_PATH"] = self.config.config_path

        os.environ["ACCOUNT"] = self.config["account"]
        os.environ["PATH_TO_INPUT"] = self.config["path_to_input"]

        os.environ["PATH_TO_OUTPUT"] = self.config["path_to_output_folder"]
        for output_folder, output_path in self.config["output_structure"].items():
            os.environ["OUTPUT_" + output_folder.upper()] = os.path.join(self.config["path_to_output_folder"],
                                                                         output_path)
        os.environ["OUTPUT_TOOLS_LOGS_FOLDER"] = os.path.join(self.config["path_to_output_folder"],
                                                              self.config["output_structure"]["tools_folder"],
                                                              self.tool_name,
                                                              "logs")

        for downloader_var, downloader_value in self.config["tools_parameters"].items():
            os.environ["TOOLS_" + downloader_var.upper()] = str(downloader_value)

        self.logger.info("Environment initialized")

    def __init_filestructure(self):
        ensure_created([
            self.tool_folder,
            os.path.join(self.tool_folder, "filter_table"),
            os.path.join(self.tool_folder, "verification"),
            os.path.join(self.tool_folder, "logs")
        ])

        self.tool_checkpoint = self.__load_checkpoint()
        self.tool_job_history, self.tool_job_history_IO = self.__load_job_history()

    def __load_checkpoint(self) -> Dict[str, bool]:
        checkpoint = {}
        if os.path.exists(self.tool_checkpoint_path):
            with open(self.tool_checkpoint_path, "r") as file:
                checkpoint = yaml.full_load(file)

        for key, value in checkpoint_scheme.items():
            if key not in checkpoint.keys():
                checkpoint[key] = value

        update_checkpoint(self.tool_checkpoint_path, checkpoint)
        return checkpoint

    def __load_job_history(self) -> Tuple[List[int], TextIO]:
        job_ids = []

        if os.path.exists(self.tool_job_history_path):
            df = pd.read_csv(self.tool_job_history_path)
            job_ids = df["job_ids"].to_list()
        else:
            with open(self.tool_job_history_path, "w") as f:
                print("job_ids", file=f)

        job_io = open(self.tool_job_history_path, "a")

        return job_ids, job_io

    def __update_job_history(self, new_id: int) -> None:
        self.tool_job_history.append(new_id)
        print(new_id, file=self.tool_job_history_IO)

    def __schedule_filtering(self) -> None:
        self.logger.info("Scheduling filtering script")
        job_id = submit_job(self.config['scripts']['tools_submitter'],
                            self.config['scripts']['tools_filter_script'],
                            self.tool_name,
                            *preprocess_dep_ids([self.tool_job_history[-1] if len(self.tool_job_history) != 0 else None]),
                            "--spark")
        self.__update_job_history(job_id)
        self.tool_checkpoint["filtered"] = True
        update_checkpoint(self.tool_checkpoint_path, self.tool_checkpoint)
        self.logger.info("Scheduled filtering script")

    def __schedule_schedule_creation(self) -> None:
        self.logger.info("Scheduling schedule creation script")
        job_id = submit_job(self.config['scripts']['tools_submitter'],
                            self.config['scripts']['tools_scheduling_script'],
                            self.tool_name,
                            *preprocess_dep_ids([self.tool_job_history[-1]]))
        self.__update_job_history(job_id)
        self.tool_checkpoint["schedule_created"] = True
        update_checkpoint(self.tool_checkpoint_path, self.tool_checkpoint)
        self.logger.info("Scheduled schedule creation script")

    def __schedule_workers(self) -> None:
        self.logger.info("Scheduling workers script")

        for _ in range(self.config["tools_parameters"]["num_workers"]):
            job_id = submit_job(self.config['scripts']['tools_submitter'],
                                self.config['scripts']['tools_worker_script'],
                                self.tool_name,
                                *preprocess_dep_ids([self.tool_job_history[-1]]))
            self.__update_job_history(job_id)

        job_id = submit_job(self.config['scripts']['tools_submitter'],
                            self.config['scripts']['tools_verification_script'],
                            self.tool_name,
                            *preprocess_dep_ids([self.tool_job_history[-1]]))
        self.__update_job_history(job_id)

        self.logger.info("Scheduled workers script")

    def apply_tool(self):
        if not self.tool_checkpoint.get("filtered", False):
            self.__schedule_filtering()
        else:
            self.logger.info("Skipping filtering script: table already created")

        if not self.tool_checkpoint.get("schedule_created", False):
            self.__schedule_schedule_creation()
        else:
            self.logger.info("Skipping schedule creation script: schedule already created")

        if not self.tool_checkpoint.get("completed", False):
            self.__schedule_workers()
        else:
            self.logger.error("Tool completed its job")

    def __del__(self):
        if self.tool_job_history_IO is not None:
            self.tool_job_history_IO.close()


def main():
    config_path = "/users/PAS2119/andreykopanev/distributed-downloader/config/local_config.yaml"
    tool_name = "duplication_based"

    dd = Tools.from_path(config_path, tool_name)
    dd.apply_tool()


if __name__ == "__main__":
    main()
