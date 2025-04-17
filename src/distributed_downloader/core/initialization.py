import os.path
from typing import Dict, Type

from distributed_downloader.core.initializers.base_initializer import BaseInitializer
from distributed_downloader.core.initializers.eol_initializer import EoLInitializer
from distributed_downloader.core.initializers.fathom_net_initializer import (
    FathomNetInitializer,
)
from distributed_downloader.core.initializers.gbif_initializer import GBIFInitializer
from distributed_downloader.core.initializers.lila_initializer import LilaInitializer
from distributed_downloader.tools import Config
from distributed_downloader.tools.utils import (
    truncate_paths,
)

__initializers: Dict[str, Type[BaseInitializer]] = {
    "gbif": GBIFInitializer,
    "fathom_net": FathomNetInitializer,
    "lila": LilaInitializer,
    "eol": EoLInitializer,
}


def init_filestructure(file_structure: Dict[str, str]) -> None:
    filtered_fs = [
        value
        for key, value in file_structure.items()
        if key not in ["inner_checkpoint_file", "ignored_table"]
    ]
    truncate_paths(filtered_fs)


if __name__ == "__main__":
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")
    config = Config.from_path(config_path, "downloader")
    assert (
            config["initializer_type"] in __initializers.keys()
    ), "Unknown initialization type, aborting"

    initializer = __initializers[config["initializer_type"]](config)
    initializer.run()
