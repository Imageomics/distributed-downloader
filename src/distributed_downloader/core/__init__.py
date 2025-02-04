from logging import Logger
from typing import Optional

from .main import DistributedDownloader


def download_images(config_path: str, logger: Optional[Logger] = None) -> None:
    """
    Initiates the download of images based on a given configuration.

    This function creates an instance of `DistributedDownloader` using a configuration file path,
    optionally sets a logger for the downloader, and then starts the image downloading process.

    Parameters:
    - config_path (str): The file path to the configuration file required to initialize the downloader.
    - logger (Logger, optional): An instance of a logger to be used by the downloader. Defaults to None.

    Returns:
    - None
    """
    dd = DistributedDownloader.from_path(config_path)
    if logger is not None:
        dd.logger = logger
    dd.download_images()
