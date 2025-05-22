from logging import Logger
from typing import Optional

from distributed_downloader.core.main import DistributedDownloader
from distributed_downloader.core.fake_profiler import main as fake_profiler_main
from distributed_downloader.core.initialization import *
from distributed_downloader.core.MPI_download_prep import main as mpi_download_prep_main
from distributed_downloader.core.MPI_downloader_verifier import verify_batches
from distributed_downloader.core.MPI_multimedia_downloader import download_schedule
from distributed_downloader.core.MPI_multimedia_downloader_controller import (
    main as mpi_multimedia_downloader_controller,
)


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
