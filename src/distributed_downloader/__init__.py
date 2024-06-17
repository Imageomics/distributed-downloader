from logging import Logger

from main import DistributedDownloader


def download_images(config_path: str, logger: Logger = None) -> None:
    """
    Download images using the distributed downloader.
    Args:
        config_path:
        logger: (Default value = None)

    Returns:

    """
    dd = DistributedDownloader.from_path(config_path)
    if logger is not None:
        dd.logger = logger
    dd.download_images()
