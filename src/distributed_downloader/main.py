import os.path
from typing import Any, Dict, LiteralString

import yaml
from attr import define, field, Factory


@define
class DistributedDownloader:
    config: Dict[str, Any]

    urls_path: LiteralString | str | bytes = None
    inner_checkpoint_path: LiteralString | str | bytes = None
    profiles_path: LiteralString | str | bytes = None

    inner_checkpoint: Dict[str, bool] = None

    def __attrs_post_init__(self):
        self.urls_path = os.path.join(self.config['path_to_output_folder'], self.config['output_structure']['urls_folder'])
        self.inner_checkpoint_path = os.path.join(self.config['path_to_output_folder'], self.config['output_structure']['inner_checkpoint_file'])
        self.profiles_path = os.path.join(self.config['path_to_output_folder'], self.config['output_structure']['profiles_table'])

        self.inner_checkpoint = self.__load_checkpoint()

    def __init_environment(self) -> None:
        pass

    def __schedule_initialization(self) -> int:
        pass

    def __schedule_profiling(self, prev_job_id: int = None) -> int:
        pass

    def __schedule_downloading(self, prev_job_id: int = None) -> None:
        pass

    def __load_checkpoint(self) -> Dict[str, bool]:
        if not os.path.exists(self.inner_checkpoint_path):
            return {
                "batched": False,
                "profiled": False
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

        profiling_job_id = None
        if (
            not os.path.exists(self.profiles_path)
            or not self.inner_checkpoint["profiled"]
        ):
            profiling_job_id = self.__schedule_profiling(initialization_job_id)

        self.__schedule_downloading(profiling_job_id)


def main() -> None:
    config_path = "/mnt/c/Users/24122/PycharmProjects/distributed_downloader/config/example_config.yaml"
    with open(config_path, "r") as file:
        config = yaml.full_load(file)

    dd = DistributedDownloader(config)
    dd.download_images()


if __name__ == "__main__":
    main()
