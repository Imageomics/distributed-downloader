from typing import Literal

from tools.config import Config
from tools.registry import ToolsRegistryBase


def apply_tool(cfg_path: str, tool_name: Literal["size_based", "duplication_based", "resize", "image_verification"]):
    config = Config.from_path(cfg_path)
    if tool_name not in ToolsRegistryBase.TOOLS_REGISTRY.keys():
        ValueError("unknown tool name")



if __name__ == "__main__":
    pass
