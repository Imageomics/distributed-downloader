import argparse
import os

from tools.utils import init_logger
from tools.config import Config
from tools.registry import ToolsRegistryBase

if __name__ == "__main__":
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = Config.from_path(config_path, "tools")
    logger = init_logger(__name__)

    parser = argparse.ArgumentParser(description='Running step of the Tool')
    parser.add_argument("runner_name", metavar="runner_name", type=str,
                        help="the name of the tool that is intended to be used")
    _args = parser.parse_args()
    tool_name = _args.runner_name

    assert tool_name in ToolsRegistryBase.TOOLS_REGISTRY.keys(), ValueError("unknown runner")

    tool_filter = ToolsRegistryBase.TOOLS_REGISTRY[tool_name]["runner"](config)

    logger.info("Starting runner")
    tool_filter.run()

    logger.info("completed runner")
