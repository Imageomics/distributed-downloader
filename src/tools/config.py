from typing import Any, Dict, LiteralString

from attr import define

from distributed_downloader.utils import load_config


@define
class Config:
    config_path: str
    config: Dict[str, Any]

    @classmethod
    def from_path(cls, path: str) -> "Config":
        return cls(config_path=path,
                   config=load_config(path))

    def __getitem__(self, item):
        return self.config[item]

    def __setitem__(self, key, value):
        self.config[key] = value
