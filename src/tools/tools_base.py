import os
from logging import Logger
from typing import LiteralString

from attr import define, field, Factory

from distributed_downloader.utils import init_logger
from tools.config import Config
from tools.registry import ToolsRegistryBase


@define
class ToolsBase(metaclass=ToolsRegistryBase):
    config: Config

    logger: Logger = field(default=Factory(lambda: init_logger(__name__)))

    urls_path: str = None
    downloaded_images_path: str = None
    tools_path: str = None
    total_workers: int = None

    filter_name: str = None

    # noinspection PyTypeChecker
    def __attrs_post_init__(self):
        self.urls_path = os.path.join(self.config['path_to_output_folder'],
                                      self.config['output_structure']['urls_folder'])
        self.downloaded_images_path = os.path.join(self.config['path_to_output_folder'],
                                                   self.config['output_structure']['images_folder'])
        self.tools_path = os.path.join(self.config['path_to_output_folder'],
                                       self.config['output_structure']['tools_folder'])
        self.total_workers = (self.config["tools_parameters"]["max_nodes"]
                              * self.config["tools_parameters"]["workers_per_node"])

        self.__init_environment()

    def __init_environment(self) -> None:
        os.environ["CONFIG_PATH"] = self.config.config_path

        os.environ["ACCOUNT"] = self.config["account"]
        os.environ["PATH_TO_INPUT"] = self.config["path_to_input"]

        os.environ["PATH_TO_OUTPUT"] = self.config["path_to_output_folder"]
        for output_folder, output_path in self.config["output_structure"].items():
            os.environ["OUTPUT_" + output_folder.upper()] = os.path.join(self.config["path_to_output_folder"],
                                                                         output_path)

        for downloader_var, downloader_value in self.config["tools_parameters"].items():
            os.environ["TOOLS_" + downloader_var.upper()] = str(downloader_value)

        self.logger.info("Environment initialized")

