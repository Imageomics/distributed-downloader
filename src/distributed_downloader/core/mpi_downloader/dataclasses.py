from __future__ import annotations

import multiprocessing
import queue
import threading
import uuid
import math
from typing import List, Dict, Any

import numpy as np
from attr import define, field
from pandas import DataFrame

_NOT_PROVIDED = "Not provided"


@define
class DownloadedImage:
    retry_count: int
    error_code: int
    error_msg: str

    unique_name: str
    source_id: int
    identifier: str
    is_license_full: bool
    license: str
    source: str
    title: str

    hashsum_original: str = ""
    hashsum_resized: str = ""
    # image: np.ndarray = np.ndarray(0)
    image: bytes = bytes()
    original_size: np.ndarray[np.uint32] = np.ndarray([0, 0], dtype=np.uint32)
    resized_size: np.ndarray[np.uint32] = np.ndarray([0, 0], dtype=np.uint32)

    start_time: float = 0
    end_time: float = 0

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> DownloadedImage:
        if "EOL content ID" in row.keys() and 'EOL page ID' in row.keys():
            source_id = row["EOL content ID"] + "_" + row['EOL page ID']
        else:
            source_id = "None"

        return cls(
            retry_count=0,
            error_code=0,
            error_msg="",
            unique_name=row.get("uuid", uuid.uuid4().hex),
            source_id=row.get("source_id", source_id),
            identifier=row.get("identifier", ""),
            is_license_full=all([row.get("license", None), row.get("source", None), row.get("title", None)]),
            license=row.get("license", _NOT_PROVIDED) or _NOT_PROVIDED,
            source=row.get("source", _NOT_PROVIDED) or _NOT_PROVIDED,
            title=row.get("title", _NOT_PROVIDED) or _NOT_PROVIDED,
        )


def init_downloaded_image_entry(image_entry: np.ndarray, row: Dict[str, Any]) -> np.ndarray:
    image_entry["is_downloaded"] = False
    image_entry["retry_count"] = 0
    image_entry["error_code"] = 0
    image_entry["error_msg"] = ""
    image_entry["uuid"] = row.get("UUID", uuid.uuid4().hex)
    image_entry["source_id"] = row.get("source_id", 0)
    image_entry["identifier"] = row.get("identifier", "")
    image_entry["is_license_full"] = all([row.get("license", None), row.get("source", None), row.get("title", None)])
    image_entry["license"] = row.get("license", _NOT_PROVIDED) or _NOT_PROVIDED
    image_entry["source"] = row.get("source", _NOT_PROVIDED) or _NOT_PROVIDED
    image_entry["title"] = row.get("title", _NOT_PROVIDED) or _NOT_PROVIDED

    return image_entry


@define
class SuccessEntry:
    uuid: str
    source_id: int
    identifier: str
    is_license_full: bool
    license: str
    source: str
    title: str
    hashsum_original: str
    hashsum_resized: str
    original_size: np.ndarray[np.uint32]
    resized_size: np.ndarray[np.uint32]
    image: bytes

    def __success_dtype(self, img_size: int):
        return np.dtype([
            ("uuid", "S32"),
            ("source_id", "i4"),
            ("identifier", "S256"),
            ("is_license_full", "bool"),
            ("license", "S256"),
            ("source", "S256"),
            ("title", "S256"),
            ("original_size", "(2,)u4"),
            ("resized_size", "(2,)u4"),
            ("hashsum_original", "S32"),
            ("hashsum_resized", "S32"),
            ("image", f"({img_size},{img_size},3)uint8")
        ])

    @staticmethod
    def get_success_spark_scheme():
        from pyspark.sql.types import StructType
        from pyspark.sql.types import StringType
        from pyspark.sql.types import LongType
        from pyspark.sql.types import StructField
        from pyspark.sql.types import BooleanType
        from pyspark.sql.types import ArrayType
        from pyspark.sql.types import BinaryType

        return StructType([
            StructField("uuid", StringType(), False),
            StructField("source_id", LongType(), False),
            StructField("identifier", StringType(), False),
            StructField("is_license_full", BooleanType(), False),
            StructField("license", StringType(), True),
            StructField("source", StringType(), True),
            StructField("title", StringType(), True),
            StructField("original_size", ArrayType(LongType(), False), False),
            StructField("resized_size", ArrayType(LongType(), False), False),
            StructField("hashsum_original", StringType(), False),
            StructField("hashsum_resized", StringType(), False),
            StructField("image", BinaryType(), False)
        ])

    @classmethod
    def from_downloaded(cls, downloaded: DownloadedImage) -> SuccessEntry:
        return cls(
            uuid=downloaded.unique_name,
            source_id=downloaded.source_id,
            identifier=downloaded.identifier,
            is_license_full=downloaded.is_license_full,
            license=downloaded.license,
            source=downloaded.source,
            title=downloaded.title,
            hashsum_original=downloaded.hashsum_original,
            hashsum_resized=downloaded.hashsum_resized,
            original_size=downloaded.original_size,
            resized_size=downloaded.resized_size,
            image=downloaded.image
        )

    @staticmethod
    def to_list_download(downloaded: DownloadedImage) -> List:
        return [
            downloaded.unique_name,
            downloaded.source_id,
            downloaded.identifier,
            downloaded.is_license_full,
            downloaded.license,
            downloaded.source,
            downloaded.title,
            downloaded.original_size,
            downloaded.resized_size,
            downloaded.hashsum_original,
            downloaded.hashsum_resized,
            downloaded.image
        ]

    @staticmethod
    def get_names() -> List[str]:
        return [
            "uuid",
            "source_id",
            "identifier",
            "is_license_full",
            "license",
            "source",
            "title",
            "original_size",
            "resized_size",
            "hashsum_original",
            "hashsum_resized",
            "image"
        ]

    def to_list(self) -> List:
        return [
            self.uuid,
            self.source_id,
            self.identifier,
            self.is_license_full,
            self.license,
            self.source,
            self.title,
            self.original_size,
            self.resized_size,
            self.hashsum_original,
            self.hashsum_resized,
            self.image
        ]

    def to_np(self) -> np.ndarray:
        np_structure = np.array(
            [
                (self.uuid,
                 self.source_id,
                 self.identifier,
                 self.is_license_full,
                 self.license,
                 self.source,
                 self.title,
                 self.original_size,
                 self.resized_size,
                 self.hashsum_original,
                 self.hashsum_resized,
                 self.image)
            ],
            dtype=self.__success_dtype(np.max(self.resized_size)))

        return np_structure


@define
class ErrorEntry:
    uuid: str
    identifier: str
    retry_count: int
    error_code: int
    error_msg: str

    _error_dtype = np.dtype([
        ("uuid", "S32"),
        ("identifier", "S256"),
        ("retry_count", "i4"),
        ("error_code", "i4"),
        ("error_msg", "S256")
    ])

    @classmethod
    def from_downloaded(cls, downloaded: DownloadedImage) -> ErrorEntry:
        return cls(
            uuid=downloaded.unique_name,
            identifier=downloaded.identifier,
            retry_count=downloaded.retry_count,
            error_code=downloaded.error_code,
            error_msg=downloaded.error_msg
        )

    @staticmethod
    def to_list_download(downloaded: DownloadedImage) -> List:
        return [
            downloaded.unique_name,
            downloaded.identifier,
            downloaded.retry_count,
            downloaded.error_code,
            downloaded.error_msg
        ]

    def to_list(self) -> List:
        return [
            self.uuid,
            self.identifier,
            self.retry_count,
            self.error_code,
            self.error_msg
        ]

    def to_np(self) -> np.ndarray:
        np_structure = np.array(
            [
                (self.uuid,
                 self.identifier,
                 self.retry_count,
                 self.error_code,
                 self.error_msg)
            ],
            dtype=self._error_dtype)

        return np_structure

    @staticmethod
    def get_names() -> List[str]:
        return [
            "uuid",
            "identifier",
            "retry_count",
            "error_code",
            "error_msg"
        ]


@define
class ImageBatchesByServerToRequest:
    server_name: str
    lock: threading.Lock
    writer_notification: threading.Event
    urls: queue.Queue[List[Dict[str, Any]]]
    max_rate: int
    total_batches: int

    @classmethod
    def from_pandas(cls,
                    server_name: str,
                    manager: multiprocessing.Manager,
                    urls: List[DataFrame],
                    max_rate: int = 50) -> ImageBatchesByServerToRequest:
        urls_queue: queue.Queue[List[Dict[str, Any]]] = queue.Queue()
        for url_batch in urls:
            urls_queue.put(url_batch.to_dict("records"))

        return cls(
            server_name=server_name,
            lock=manager.Lock(),
            writer_notification=manager.Event(),
            urls=urls_queue,
            max_rate=max_rate,
            total_batches=len(urls)
        )


@define
class CompletedBatch:
    success_queue: queue.Queue[DownloadedImage]
    error_queue: queue.Queue[DownloadedImage]
    batch_id: int = -1
    offset: int = 0


@define
class WriterServer:
    server_name: str
    download_complete: threading.Event
    competed_queue: queue.Queue[CompletedBatch]
    total_batches: int
    done_batches: int = 0


@define
class RateLimit:
    initial_rate = field(init=True, type=float)
    _multiplier = field(init=True, type=float, default=0.5, validator=lambda _, __, value: 0 < value)
    lower_bound = field(init=False, type=int, converter=math.floor, default=0)
    upper_bound = field(init=False, type=int, converter=math.floor, default=0)

    def __attrs_post_init__(self):
        self.lower_bound = max(self.initial_rate * (1 - self._multiplier), 1)
        self.upper_bound = self.initial_rate * (1 + self._multiplier)

    def change_rate(self, new_rate: float):
        self.initial_rate = new_rate
        self.lower_bound = max(self.initial_rate * (1 - self._multiplier), 1)
        self.upper_bound = self.initial_rate * (1 + self._multiplier)


profile_dtype = np.dtype([
    ("server_name", "S256"),
    ("total_batches", "i4"),
    ("success_count", "i4"),
    ("error_count", "i4"),
    ("rate_limit", "i4")
])
