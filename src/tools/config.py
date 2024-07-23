import os.path
from typing import Any, Dict

import yaml
from attr import define


@define
class Config:
    config_path: str
    _cfg_dict: Dict[str, str | int | bool | Dict[str, Any]]
    scripts: Dict[str, str]
    folder_structure: Dict[str, str]

    @classmethod
    def from_path(cls, path: str) -> "Config":
        cfg = cls.__load_config(path)

        assert cls.__check_config(cfg), "Config is not valid"

        # print(cfg)
        return cls(config_path=path,
                   cfg_dict=cfg)

    @staticmethod
    def __check_config(cfg: Dict[str, str | int | bool | Dict[str, Any]]) -> bool:
        return True

    @staticmethod
    def __load_config(path: str) -> Dict[str, str | int | bool | Dict[str, Any]]:
        with open(path, "r") as file:
            return yaml.full_load(file)

    @staticmethod
    def __load_scripts(cfg: Dict[str, str | int | bool | Dict[str, Any]]) -> Dict[str, str]:
        return cfg["scripts"]

    @staticmethod
    def __load_folder_structure(cfg: Dict[str, str | int | bool | Dict[str, Any]]) -> Dict[str, str]:
        file_structure: Dict[str, str] = {"path_to_output_folder": cfg["path_to_output_folder"]}
        file_structure.update({key: os.path.join(cfg["path_to_output_folder"], value) for key, value in cfg["output_structure"].items()})
        return file_structure

    def get_script(self, script_name: str) -> str:
        return self.scripts[script_name]

    def get_folder(self, folder_name: str) -> str:
        return self.folder_structure[folder_name]

    def get(self, key: str, default: Any = None) -> str | int | bool | Dict[str, Any]:
        return self._cfg_dict.get(key, default)

    def __getitem__(self, key: str) -> str | int | bool | Dict[str, Any]:
        return self._cfg_dict[key]
