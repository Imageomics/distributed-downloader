from typing import List, Dict, Any, Iterator

import pandas as pd
import re


def load_batch(
        path_to_parquet: str,
        server_name: str,
        batches_to_download: List[int],
) -> Iterator[List[Dict[str, Any]]]:
    for batch_id in batches_to_download:
        server_df = pd.read_parquet(
            f"{path_to_parquet}/ServerName={re.sub(':', '%3A', server_name)}/partition_id={batch_id}")
        yield server_df.to_dict("records")


def load_one_batch(input_path: str) -> List[Dict[str, Any]]:
    return pd.read_parquet(input_path).to_dict("records")
