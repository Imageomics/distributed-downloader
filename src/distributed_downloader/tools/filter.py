import argparse
import os

from distributed_downloader.tools.utils import init_logger
from distributed_downloader.tools.config import Config
from distributed_downloader.tools.registry import ToolsRegistryBase

if __name__ == "__main__":
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = Config.from_path(config_path, "tools")
    logger = init_logger(__name__)

    parser = argparse.ArgumentParser(description='Filtering step of the Tool')
    parser.add_argument("filter_name", metavar="filter_name", type=str,
                        help="the name of the tool that is intended to be used")
    _args = parser.parse_args()
    tool_name = _args.filter_name

    assert tool_name in ToolsRegistryBase.TOOLS_REGISTRY.keys(), ValueError("unknown filter")

    tool_filter = ToolsRegistryBase.TOOLS_REGISTRY[tool_name]["filter"](config)

    logger.info("Starting filter")
    tool_filter.run()

    logger.info("completed filtering")

    tool_filter = None
