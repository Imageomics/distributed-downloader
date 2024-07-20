import argparse
import os

import pandas as pd
import yaml

from distributed_downloader.utils import init_logger, update_checkpoint
from tools.config import Config
from tools.registry import ToolsRegistryBase
from tools.runners import MPIRunnerTool

if __name__ == "__main__":
    config_path = os.environ.get("CONFIG_PATH")
    if config_path is None:
        raise ValueError("CONFIG_PATH not set")

    config = Config.from_path(config_path)
    logger = init_logger(__name__)

    parser = argparse.ArgumentParser(description='Running step of the Tool')
    parser.add_argument("runner_name", metavar="runner_name", type=str,
                        help="the name of the tool that is intended to be used")
    _args = parser.parse_args()
    tool_name = _args.runner_name

    assert tool_name in ToolsRegistryBase.TOOLS_REGISTRY.keys(), ValueError("unknown runner")

    tool_folder = os.path.join(config["path_to_output_folder"], config["output_structure"]["tools_folder"], tool_name)
    with open(os.path.join(tool_folder, "tool_checkpoint.yaml"), "r") as file:
        checkpoint = yaml.full_load(file)
    schedule_df = pd.read_csv(os.path.join(tool_folder, "schedule.csv"))
    verification_df = MPIRunnerTool.load_table(os.path.join(tool_folder, "verification"))

    outer_join = schedule_df.merge(verification_df, how='outer', indicator=True, on=["server_name", "partition_id"])
    left = outer_join[(outer_join["_merge"] == 'left_only')].drop('_merge', axis=1)

    if len(left) == 0:
        checkpoint["completed"] = True
        update_checkpoint(os.path.join(tool_folder, "tool_checkpoint.yaml"), checkpoint)

        logger.info("Tool completed its job")
    else:
        logger.info(f"Tool needs more time, left to complete: {len(left)}")
