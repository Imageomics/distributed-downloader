import csv
import os.path
from logging import Logger
from typing import Any, Dict, LiteralString, Optional

import yaml
from attr import define, field, Factory

from distributed_downloader.initialization import init_filestructure
from distributed_downloader.utils import update_checkpoint, load_config, submit_job


@define
class DistributedDownloader:
    config_path: LiteralString | str
    config: Dict[str, str | int | bool | Dict[str, Any]]

    logger: Logger = field(default=Factory(Logger))

    urls_path: LiteralString | str = None
    inner_checkpoint_path: LiteralString | str = None
    profiles_path: LiteralString | str = None
    schedules_folder: LiteralString | str = None

    inner_checkpoint: Optional[Dict[str, bool]] = None

    @classmethod
    def from_path(cls, path: str) -> "DistributedDownloader":
        return cls(config=load_config(path),
                   config_path=path)

    def __attrs_post_init__(self):
        self.urls_path = os.path.join(self.config['path_to_output_folder'],
                                      self.config['output_structure']['urls_folder'])
        self.inner_checkpoint_path = os.path.join(self.config['path_to_output_folder'],
                                                  self.config['output_structure']['inner_checkpoint_file'])
        self.profiles_path = os.path.join(self.config['path_to_output_folder'],
                                          self.config['output_structure']['profiles_table'])
        self.schedules_folder = os.path.join(self.config['path_to_output_folder'],
                                             self.config['output_structure']['schedules_folder'],
                                             "current")

        self.inner_checkpoint = self.__load_checkpoint()

    def __init_environment(self) -> None:
        os.environ["CONFIG_PATH"] = self.config_path

        os.environ["ACCOUNT"] = self.config["account"]
        os.environ["PATH_TO_INPUT"] = self.config["path_to_input"]

        os.environ["PATH_TO_OUTPUT"] = self.config["path_to_output_folder"]
        for output_folder, output_path in self.config["output_structure"].items():
            os.environ["OUTPUT_" + output_folder.upper()] = os.path.join(self.config["path_to_output_folder"],
                                                                         output_path)

        for downloader_var, downloader_value in self.config["downloader_parameters"].items():
            os.environ["DOWNLOADER_" + downloader_var.upper()] = str(downloader_value)

        self.logger.info("Environment initialized")

    def __schedule_initialization(self) -> int:
        self.logger.info("Scheduling initialization script")

        init_filestructure(self.config)

        idx = submit_job(self.config['scripts']['general_submitter'],
                         self.config['scripts']['initialization_script'])

        self.logger.info(f"Submitted initialization script {idx}")
        self.inner_checkpoint["batched"] = True
        update_checkpoint(self.inner_checkpoint_path, self.inner_checkpoint)
        return idx

    def __schedule_profiling(self, prev_job_id: int = None) -> int:
        self.logger.info("Scheduling profiling script")
        idx = submit_job(self.config['scripts']['general_submitter'],
                         self.config['scripts']['profiling_script'],
                         str(prev_job_id))
        self.logger.info(f"Submitted profiling script {idx}")
        self.inner_checkpoint["profiled"] = True
        update_checkpoint(self.inner_checkpoint_path, self.inner_checkpoint)
        return idx

    def __schedule_downloading(self, prev_job_id: int = None) -> None:
        self.logger.info("Scheduling downloading scripts")
        all_prev_ids = [prev_job_id] if prev_job_id is not None else []

        for schedule in os.listdir(self.schedules_folder):
            with open(os.path.join(self.schedules_folder, schedule, "_jobs_ids.csv"), "r") as file:
                all_prev_ids.append(int(list(csv.DictReader(file))[-1]["job_id"]))

        schedule_creation_id = submit_job(self.config['scripts']['schedule_creator_submitter'],
                                          self.config['scripts']['schedule_creation_script'],
                                          *all_prev_ids)
        self.logger.info(f"Submitted schedule creation script {schedule_creation_id}")
        self.inner_checkpoint["schedule_creation_scheduled"] = True
        update_checkpoint(self.inner_checkpoint_path, self.inner_checkpoint)

    #     for schedule in os.listdir(self.schedules_folder):
    #         submission_records = []
    #         offset = 0
    #         verifier_id = self.__submit_verifier(schedule,
    #                                              offset,
    #                                              schedule_creation_id)
    #         submission_records.append({
    #             "job_id": verifier_id,
    #             "is_verification": True
    #         })
    #         offset += 1
    #
    #         for _ in range(self.config["downloader_parameters"]["num_downloaders"]):
    #             download_id = self.__submit_downloader(schedule,
    #                                                    offset,
    #                                                    submission_records[-1]["job_id"], )
    #             submission_records.append({
    #                 "job_id": download_id,
    #                 "is_verification": False
    #             })
    #
    #             verifier_id = self.__submit_verifier(schedule,
    #                                                  offset,
    #                                                  download_id)
    #             submission_records.append({
    #                 "job_id": verifier_id,
    #                 "is_verification": True
    #             })
    #
    #             offset += 1
    #
    #         pd.DataFrame(submission_records).to_csv(os.path.join(self.schedules_folder, schedule, "_jobs_ids.csv"),
    #                                                 index=False,
    #                                                 header=True)
    #
    #     self.logger.info("All downloading scripts submitted")
    #
    # def __submit_downloader(self,
    #                         _schedule: str,
    #                         iteration_id: int,
    #                         dep_id: int) -> int:
    #     iteration = str(iteration_id).zfill(4)
    #     idx = submit_job(self.config['scripts']['mpi_submitter'],
    #                      self.config['scripts']['downloading_script'],
    #                      _schedule,
    #                      iteration,
    #                      str(dep_id))
    #     self.logger.info(f"Submitted downloader {idx} for {_schedule}")
    #     return idx
    #
    # def __submit_verifier(self,
    #                       _schedule: str,
    #                       iteration_id: int,
    #                       dep_id: int) -> int:
    #     iteration = str(iteration_id).zfill(4)
    #     idx = submit_job(self.config['scripts']['mpi_submitter'],
    #                      self.config['scripts']['verifying_script'],
    #                      _schedule,
    #                      iteration,
    #                      str(dep_id))
    #     self.logger.info(f"Submitted verifier {idx} for {_schedule}")
    #     return idx

    def __load_checkpoint(self) -> Dict[str, bool]:
        if not os.path.exists(self.inner_checkpoint_path):
            return {
                "batched": False,
                "profiled": False,
                "schedule_creation_scheduled": False,
            }

        with open(self.inner_checkpoint_path, "r") as file:
            return yaml.full_load(file)

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
    config_path = "/mnt/c/Users/24122/PycharmProjects/distributed_downloader/config/example_config.yaml"

    dd = DistributedDownloader.from_path(config_path)
    dd.download_images()


if __name__ == "__main__":
    main()
