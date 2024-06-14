from typing import Any, Dict

import yaml


def download_images(config: Dict[str, Any]) -> None:
    pass


def main() -> None:
    config_path = "/mnt/c/Users/24122/PycharmProjects/distributed-downloader/config/example_config.yaml"
    with open(config_path, "r") as file:
        config = yaml.full_load(file)

    download_images(config)


if __name__ == "__main__":
    main()
