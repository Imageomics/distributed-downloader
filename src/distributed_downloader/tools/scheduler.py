import argparse
import os

from distributed_downloader.tools import Checkpoint
from distributed_downloader.tools.utils import init_logger
from distributed_downloader.tools.config import Config
from distributed_downloader.tools.registry import ToolsRegistryBase

if __name__ == "__main__":
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    parser = argparse.ArgumentParser(description="Running step of the Tool")
    parser.add_argument(
        "scheduler_name",
        metavar="scheduler_name",
        type=str,
        help="the name of the tool that is intended to be used",
    )
    _args = parser.parse_args()
    tool_name = _args.scheduler_name

    assert tool_name in ToolsRegistryBase.TOOLS_REGISTRY.keys(), ValueError(
        "unknown scheduler"
    )

    config = Config.from_path(config_path, "tools")
    checkpoint = Checkpoint.from_path(
        os.path.join(
            config.get_folder("tools_folder"), tool_name, "tool_checkpoint.yaml"
        ),
        {
            "filtering_completed": False,
            "scheduling_schedule": True,
            "scheduling_completed": False,
        },
    )
    logger = init_logger(__name__)
    checkpoint["scheduling_schedule"] = False

    if not checkpoint.get("filtering_completed", False):
        logger.error("Filtering wasn't complete, can't create schedule")
        exit(1)

    tool_scheduler = ToolsRegistryBase.TOOLS_REGISTRY[tool_name]["scheduler"](config)

    if not checkpoint.get("scheduling_completed", False):
        logger.info("Starting scheduler")
        tool_scheduler.run()

        logger.info("completed scheduler")

        checkpoint["scheduling_completed"] = True
    else:
        logger.info("Scheduling was already completed")
