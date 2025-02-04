from logging import Logger
from typing import Optional

from .checkpoint import Checkpoint
from .config import Config
from .filters import *
from .main import Tools
from .registry import ToolsRegistryBase
from .runners import *
from .schedulers import *
from .utils import (
    ensure_created,
    get_id,
    init_logger,
    load_dataframe,
    preprocess_dep_ids,
    submit_job,
    truncate_paths,
)


def apply_tools(config_path: str, tool_name: str, logger: Optional[Logger] = None) -> None:
    """
    Applies a tool to the images downloaded by the DistributedDownloader.

    This function creates an instance of `DistributedDownloader` using a configuration file path,
    optionally sets a logger for the downloader, and then applies the specified tool to the downloaded images.

    Parameters:
    - config_path (str): The file path to the configuration file required to initialize the downloader.
    - tool_name (str): The name of the tool to be applied to the downloaded images.
    - logger (Logger, optional): An instance of a logger to be used by the downloader. Defaults to None.

    Returns:
    - None
    """
    dd_tools = Tools.from_path(config_path, tool_name)
    if logger is not None:
        dd_tools.logger = logger
    dd_tools.apply_tool()
