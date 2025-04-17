"""
Data structures for the distributed downloader system.

This module defines the core data structures used throughout the downloader:
- DownloadedImage: Represents an image being downloaded with metadata
- SuccessEntry/ErrorEntry: Models for successful and failed downloads
- CompletedBatch: Collection of download results
- RateLimit: Dynamic rate limiting configuration
- Other supporting classes for batch processing and scheduling
"""

from __future__ import annotations

import math
import multiprocessing
import queue
import threading
import uuid
from typing import Any, Dict, List

import numpy as np
from attr import define, field
from pandas import DataFrame

_NOT_PROVIDED = "Not provided"


@define
class DownloadedImage:
    """
    Represents an image being downloaded with its metadata and processing state.
    
    This class tracks the state of an image throughout the download process,
    including retry attempts, error conditions, and image processing results.
    
    Attributes:
        retry_count: Number of retry attempts made
        error_code: Error code if download failed (0 for success)
        error_msg: Error message if download failed
        unique_name: Unique identifier for the image
        source_id: Source identifier from the original dataset
        identifier: URL or other identifier for the image
        is_license_full: Whether complete license information is available (license, source, and title)
        license: License information
        source: Source information
        title: Title or caption information
        hashsum_original: MD5 checksum of the original image
        hashsum_resized: MD5 checksum of the resized image
        image: Actual image data as bytes
        original_size: Original dimensions [height, width]
        resized_size: Resized dimensions [height, width]
        start_time: Time when download started
        end_time: Time when download completed
    """
    
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
        """
        Create a DownloadedImage instance from a dictionary row.
        
        Args:
            row: Dictionary containing image metadata
            
        Returns:
            DownloadedImage: A new initialized instance
        """
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
    """
    Initialize a numpy array entry with image metadata.
    
    Args:
        image_entry: numpy array to be initialized
        row: Dictionary containing image metadata
        
    Returns:
        np.ndarray: Initialized array entry
    """
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
    """
    Represents a successfully downloaded image with all required metadata.
    
    This class stores all information about a successfully downloaded and processed image,
    including image data, checksums, and metadata from the original source.
    
    Attributes:
        uuid: Unique identifier for the image
        source_id: Source identifier from the original dataset
        identifier: URL or other identifier for the image
        is_license_full: Whether complete license information is available (license, source, and title)
        license: License information
        source: Source information
        title: Title or caption information
        hashsum_original: MD5 checksum of the original image
        hashsum_resized: MD5 checksum of the resized image
        original_size: Original dimensions [height, width]
        resized_size: Resized dimensions [height, width]
        image: Actual image data as bytes
    """
    
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
        """
        Define the NumPy dtype for storing success entries.
        
        Args:
            img_size: Size of the image dimension
            
        Returns:
            np.dtype: NumPy data type definition
        """
        return np.dtype([
            ("uuid", "S32"),
            ("source_id", "S32"),
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
        """
        Define the PySpark schema for success entries.
        
        Returns:
            StructType: PySpark schema definition
        """
        from pyspark.sql.types import (
            ArrayType,
            BinaryType,
            BooleanType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        return StructType([
            StructField("uuid", StringType(), False),
            StructField("source_id", StringType(), False),
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
        """
        Create a SuccessEntry from a DownloadedImage.
        
        Args:
            downloaded: The DownloadedImage to convert
            
        Returns:
            SuccessEntry: A new success entry instance
        """
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
        """
        Convert a DownloadedImage to a list format for storage.
        
        Args:
            downloaded: DownloadedImage to convert
            
        Returns:
            List: List of values in the correct order for storage
        """
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
        """
        Get the column names for success entries.
        
        Returns:
            List[str]: List of column names
        """
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
        """
        Convert this SuccessEntry to a list format.
        
        Returns:
            List: List representation of this entry
        """
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
        """
        Convert this SuccessEntry to a NumPy array.
        
        Returns:
            np.ndarray: NumPy array representation of this entry
        """
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
    """
    Represents a failed download attempt with error information.
    
    This class stores information about failed downloads, including
    the error code, error message, and retry count.
    
    Attributes:
        uuid: Unique identifier for the download attempt
        identifier: URL or other identifier for the image
        retry_count: Number of retry attempts made
        error_code: Error code from the download attempt
        error_msg: Error message describing the failure
    """
    
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
        """
        Create an ErrorEntry from a DownloadedImage.
        
        Args:
            downloaded: The DownloadedImage that failed
            
        Returns:
            ErrorEntry: A new error entry instance
        """
        return cls(
            uuid=downloaded.unique_name,
            identifier=downloaded.identifier,
            retry_count=downloaded.retry_count,
            error_code=downloaded.error_code,
            error_msg=downloaded.error_msg
        )

    @staticmethod
    def to_list_download(downloaded: DownloadedImage) -> List:
        """
        Convert a DownloadedImage to a list format for error storage.
        
        Args:
            downloaded: DownloadedImage to convert
            
        Returns:
            List: List of error values in the correct order for storage
        """
        return [
            downloaded.unique_name,
            downloaded.identifier,
            downloaded.retry_count,
            downloaded.error_code,
            downloaded.error_msg
        ]

    def to_list(self) -> List:
        """
        Convert this ErrorEntry to a list format.
        
        Returns:
            List: List representation of this entry
        """
        return [
            self.uuid,
            self.identifier,
            self.retry_count,
            self.error_code,
            self.error_msg
        ]

    def to_np(self) -> np.ndarray:
        """
        Convert this ErrorEntry to a NumPy array.
        
        Returns:
            np.ndarray: NumPy array representation of this entry
        """
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
        """
        Get the column names for error entries.
        
        Returns:
            List[str]: List of column names
        """
        return [
            "uuid",
            "identifier",
            "retry_count",
            "error_code",
            "error_msg"
        ]


@define
class ImageBatchesByServerToRequest:
    """
    Container for batches of images to be requested from a specific server.
    
    This class manages queue of URL batches for a single server, along with
    synchronization primitives for coordinating access.
    
    Attributes:
        server_name: Name of the server
        lock: Lock for synchronized access to this container
        writer_notification: Event to notify when writing is completed
        urls: Queue of URL batches to process
        max_rate: Maximum request rate for this server
        total_batches: Total number of batches to process
    """
    
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
        """
        Create an instance from pandas DataFrames.
        
        Args:
            server_name: Name of the server
            manager: Multiprocessing manager for creating synchronized objects
            urls: List of DataFrame batches containing URLs
            max_rate: Maximum request rate for this server
            
        Returns:
            ImageBatchesByServerToRequest: A new instance
        """
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
    """
    Container for completed download results.
    
    This class holds queues of successful and failed downloads
    from a batch processing operation.
    
    Attributes:
        success_queue: Queue of successfully downloaded images
        error_queue: Queue of failed download attempts
        batch_id: Identifier for this batch
        offset: Offset within the processing sequence
    """
    
    success_queue: queue.Queue[DownloadedImage]
    error_queue: queue.Queue[DownloadedImage]
    batch_id: int = -1
    offset: int = 0


@define
class WriterServer:
    """
    Coordinates writing results for a specific server.
    
    This class tracks the completion status of downloads for a server
    and manages the queue of completed batches.
    
    Attributes:
        server_name: Name of the server
        download_complete: Event signaling when downloads are complete
        completed_queue: Queue of completed batches
        total_batches: Total number of batches to process
        done_batches: Number of batches that have been processed
    """
    
    server_name: str
    download_complete: threading.Event
    completed_queue: queue.Queue[CompletedBatch]
    total_batches: int
    done_batches: int = 0


@define
class RateLimit:
    """
    Dynamic rate limiting configuration.
    
    This class manages the rate limits for download requests,
    providing upper and lower bounds for adaptive rate control.
    
    Attributes:
        initial_rate: Starting rate limit
        _multiplier: Multiplier for determining bounds
        lower_bound: Minimum allowed rate
        upper_bound: Maximum allowed rate
    """
    
    initial_rate = field(init=True, type=float)
    _multiplier = field(init=True, type=float, default=0.5, validator=lambda _, __, value: 0 < value)
    lower_bound = field(init=False, type=int, converter=math.floor, default=0)
    upper_bound = field(init=False, type=int, converter=math.floor, default=0)

    def __attrs_post_init__(self):
        """
        Post-initialization setup to calculate rate limits.
        """
        self.lower_bound = max(self.initial_rate * (1 - self._multiplier), 1)
        self.upper_bound = self.initial_rate * (1 + self._multiplier)

    def change_rate(self, new_rate: float):
        """
        Update rate limits based on a new rate.
        
        Args:
            new_rate: New rate to base limits on
        """
        self.initial_rate = new_rate
        self.lower_bound = max(self.initial_rate * (1 - self._multiplier), 1)
        self.upper_bound = self.initial_rate * (1 + self._multiplier)


# NumPy dtype for server profile data
profile_dtype = np.dtype([
    ("server_name", "S256"),
    ("total_batches", "i4"),
    ("success_count", "i4"),
    ("error_count", "i4"),
    ("rate_limit", "i4")
])
