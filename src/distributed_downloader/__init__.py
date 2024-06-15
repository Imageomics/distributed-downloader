from typing import Dict, Any
from main import DistributedDownloader


def download_images(config: Dict[str, Any]) -> None:
    """
    Download images using the distributed downloader.
    Args:
        config:

    Returns:

    """
    dd = DistributedDownloader(config)
    dd.download_images()
