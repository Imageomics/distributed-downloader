import os
from typing import Dict, Type

from distributed_downloader.utils import init_logger
from tools.config import Config


class ToolsRegistryBase(type):
    TOOLS_REGISTRY: Dict[str, Dict[str, Type["ToolsBase"]]] = {}

    @classmethod
    def get(cls, name):
        return cls.TOOLS_REGISTRY.get(name.lower())

    @classmethod
    def register(cls, filter_family: str, filter_name: str):
        def wrapper(model_cls):
            assert issubclass(model_cls, ToolsBase)
            assert (filter_name not in cls.TOOLS_REGISTRY.keys()
                    or filter_family not in cls.TOOLS_REGISTRY[filter_name].keys()), (
                ValueError(f"tool with the name {filter_name} already have family {filter_family}"))

            if filter_name not in cls.TOOLS_REGISTRY.keys():
                cls.TOOLS_REGISTRY[filter_name] = dict()

            cls.TOOLS_REGISTRY[filter_name][filter_family] = model_cls
            return model_cls
        return wrapper

    def __contains__(self, item):
        return item in self.TOOLS_REGISTRY

    def __iter__(self):
        return iter(self.TOOLS_REGISTRY)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.TOOLS_REGISTRY})"

    __str__ = __repr__


class ToolsBase(metaclass=ToolsRegistryBase):

    # noinspection PyTypeChecker
    def __init__(self, cfg: Config):
        self.config = cfg

        self.filter_name: str = None
        self.filter_family: str = None

        self.logger = init_logger(__name__)

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

    def run(self):
        raise NotImplementedError()
