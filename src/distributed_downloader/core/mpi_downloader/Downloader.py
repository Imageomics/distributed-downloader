"""
Core downloader module for retrieving and processing images.

This module implements the Downloader class which is responsible for:
- Downloading images from URLs with rate limiting and error handling
- Processing and resizing downloaded images
- Managing parallel downloads with thread pooling
- Adapting download rates based on server responses

The downloader uses concurrent threads with semaphores to control the
rate of requests to each server.
"""

import concurrent.futures
import hashlib
import logging
import queue
import threading
import time
from typing import Any, Dict, List, Tuple

import cv2
import numpy as np
import requests

from .dataclasses import CompletedBatch, DownloadedImage, RateLimit

_MAX_RETRIES = 5
_TIMEOUT = 5

_RETRY_ERRORS = [429, 500, 501, 502, 503, 504]


class Downloader:
    """
    Image downloader with concurrent processing, rate limiting, and error handling.
    
    This class manages the downloading and processing of images from URLs, controlling
    request rates to avoid server throttling while maximizing throughput. It handles
    retries for transient failures and processes images for storage.
    
    Attributes:
        header: HTTP headers to use for requests
        job_end_time: UNIX timestamp when the job should end
        logger: Logger instance for output messages
        session: HTTP session for making requests
        rate_limit: Current download rate limit (requests per second)
        img_size: Maximum size for image resize
        upper_limit: Maximum allowed download rate
        bottom_limit: Minimum allowed download rate
        semaphore: Controls concurrent access to resources
        convert_image: Whether to process and resize images after download
    """
    
    def __init__(self,
                 header: dict,
                 session: requests.Session,
                 rate_limit: RateLimit,
                 img_size: int = 1024,
                 job_end_time: int = 0,
                 convert_image: bool = True,
                 logger=logging.getLogger()):
        """
        Initialize a new Downloader instance.
        
        Args:
            header: HTTP headers to use for requests
            session: Prepared requests.Session for HTTP connections
            rate_limit: Rate limit object with initial, min, and max rates
            img_size: Maximum size for image resize
            job_end_time: UNIX timestamp when the job should end
            convert_image: Whether to process and resize images
            logger: Logger instance for output messages
        """
        self.header = header
        self.job_end_time = job_end_time
        self.logger = logger
        self.session: requests.Session = session
        self.rate_limit: float = rate_limit.initial_rate
        self.img_size = img_size
        self.upper_limit: int = rate_limit.upper_bound
        self.bottom_limit: int = rate_limit.lower_bound
        self.semaphore: threading.Semaphore = threading.Semaphore(self.upper_limit)
        self._condition: threading.Condition = threading.Condition()
        self.convert_image: bool = convert_image

        self._success_count = 0
        self._error_count = 0
        self.success_queue: queue.Queue[DownloadedImage] = queue.Queue()
        self.error_queue: queue.Queue[DownloadedImage] = queue.Queue()

        self._exit: bool = False

    def get_images(self,
                   images_requested: List[Dict[str, Any]],
                   new_rate_limit: RateLimit = None) \
            -> Tuple[CompletedBatch, float]:
        """
        Download and process a batch of images.
        
        This method handles the concurrent downloading of multiple images,
        managing rate limits and collecting results.
        
        Args:
            images_requested: List of dictionaries containing image metadata and URLs
            new_rate_limit: Optional new rate limit to apply
            
        Returns:
            Tuple[CompletedBatch, float]: A completed batch containing successful and
                failed downloads, and the final download rate
        """
        if new_rate_limit is not None:
            self.rate_limit = new_rate_limit.initial_rate
            self.upper_limit = new_rate_limit.upper_bound
            self.bottom_limit = new_rate_limit.lower_bound
            self.semaphore = threading.Semaphore(self.upper_limit)

        self._success_count = 0
        self._error_count = 0
        self.success_queue.queue.clear()
        self.error_queue.queue.clear()
        images_length = len(images_requested)

        self.logger.debug(f"Starting to download {images_length}")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for idx, image in enumerate(images_requested):
                prepared_image = DownloadedImage.from_row(image)

                time.sleep(1 / self.rate_limit)

                (executor.submit(self.load_url, prepared_image, _TIMEOUT, True)
                 .add_done_callback(
                    self._callback_builder(executor, prepared_image)
                ))
            with self._condition:
                self._condition.wait_for(
                    lambda: self._success_count + self._error_count >= images_length or self._exit
                )

        self.logger.debug(f"Finished downloading {self._success_count} images")

        if self._exit:
            return CompletedBatch(queue.Queue(), queue.Queue()), 0

        return CompletedBatch(self.success_queue, self.error_queue), self.rate_limit

    def load_url(self, url: DownloadedImage, timeout: int = 2, was_delayed=True) -> bytes:
        """
        Download an image from a URL with rate limiting.
        
        This method handles the actual HTTP request to download an image,
        respecting rate limits and checking for job timeouts.
        
        Args:
            url: DownloadedImage object containing the URL and metadata
            timeout: HTTP request timeout in seconds
            was_delayed: Whether a delay was already applied before calling
            
        Returns:
            bytes: Raw image content
            
        Raises:
            TimeoutError: If the job end time is approaching
            requests.HTTPError: For HTTP-related errors
        """
        with self.semaphore:
            if self.job_end_time - time.time() < 0:
                raise TimeoutError("Not enough time")

            if not was_delayed:
                time.sleep(1 / self.rate_limit)

            self.logger.debug(f"Downloading {url.identifier}")

            url.start_time = time.perf_counter()

            response = self.session.get(url.identifier,
                                        headers=self.header,
                                        stream=True,
                                        allow_redirects=True,
                                        timeout=timeout)
            response.raise_for_status()
            response.raw.decode_content = True

            return response.content

    def _callback_builder(self, executor: concurrent.futures.ThreadPoolExecutor, prep_img: DownloadedImage):
        """
        Build a callback function for handling download completion or failure.
        
        This method creates a callback function that will be called when a download
        completes or fails, handling retry logic and rate limit adjustments.
        
        Args:
            executor: Thread executor for submitting retry tasks
            prep_img: DownloadedImage object being processed
            
        Returns:
            function: Callback function to handle download results
        """
        def _done_callback(future: concurrent.futures.Future):
            with self._condition:
                try:
                    self.process_image(prep_img, future.result())
                except Exception as e:
                    is_retry = self.process_error(prep_img, e)

                    if self._exit:
                        self._condition.notify()
                        return

                    if is_retry:
                        self.rate_limit = max(self.rate_limit - 1, self.bottom_limit)
                        executor.submit(self.load_url, prep_img, _TIMEOUT, False).add_done_callback(
                            self._callback_builder(executor, prep_img))
                    else:
                        self._error_count += 1
                        self.error_queue.put(prep_img)
                else:
                    diff = prep_img.end_time - prep_img.start_time
                    delay = 1 / self.rate_limit
                    rate_limit_adjustment = delay - diff
                    self.rate_limit = min(max(self.rate_limit + rate_limit_adjustment, self.bottom_limit),
                                          self.upper_limit)

                    self._success_count += 1
                    self.success_queue.put(prep_img)

                self.logger.debug(f"Downloaded {prep_img.identifier}")

                self._condition.notify()

        return _done_callback

    def process_image(self, return_entry: DownloadedImage, raw_image_bytes: bytes) -> None:
        """
        Process a downloaded image by resizing and computing checksums.
        
        This method processes raw image data by:
        1. Converting to a NumPy array
        2. Computing checksums for integrity verification
        3. Resizing if larger than the target dimensions
        4. Storing the processed image and metadata
        
        Args:
            return_entry: DownloadedImage object to update with processed data
            raw_image_bytes: Raw image bytes from the download
            
        Raises:
            ValueError: If the image is corrupted or invalid
        """
        return_entry.end_time = time.perf_counter()

        self.logger.debug(f"Processing {return_entry.identifier}")

        if not self.convert_image:
            return_entry.image = raw_image_bytes
            self.logger.debug(f"No image conversion for {return_entry.identifier}")
            return

        np_image = np.asarray(bytearray(raw_image_bytes), dtype="uint8")
        original_image = cv2.imdecode(np_image, cv2.IMREAD_COLOR)

        if original_image is None:
            raise ValueError("Corrupted Image")

        original_hashsum = hashlib.md5(raw_image_bytes).hexdigest()

        img_size = self.img_size
        resized_image = original_image
        resized_size = original_image.shape[:2]
        if original_image.shape[0] > img_size or original_image.shape[1] > img_size:
            resized_image, resized_size = self.image_resize(original_image, img_size)

        resized_image = resized_image.tobytes()

        resized_hashsum = hashlib.md5(resized_image).hexdigest()

        return_entry.image = resized_image
        return_entry.original_size = original_image.shape[:2]
        return_entry.resized_size = resized_size
        return_entry.hashsum_original = original_hashsum
        return_entry.hashsum_resized = resized_hashsum

        self.logger.debug(f"Processed {return_entry.identifier}")

    def process_error(self, return_entry: DownloadedImage, error: Exception) -> bool:
        """
        Process download errors and determine if retry is appropriate.
        
        This method handles various error types that can occur during download
        and decides if a retry should be attempted based on the error type and
        retry count.
        
        Args:
            return_entry: DownloadedImage object that encountered an error
            error: Exception that occurred during download or processing
            
        Returns:
            bool: True if a retry should be attempted, False otherwise
        """
        if isinstance(error, TimeoutError):
            self.logger.info("Timout, trying to exit")

            self._exit = True
            self._condition.notify()
            return False
        elif isinstance(error, requests.HTTPError):
            self.logger.warning(f"HTTP error: {error.response.status_code} for {return_entry.identifier}")

            return_entry.error_code = error.response.status_code
            return_entry.error_msg = str(error)

            if error.response.status_code in _RETRY_ERRORS:
                return_entry.retry_count += 1

                return return_entry.retry_count < _MAX_RETRIES

            return False
        elif isinstance(error, requests.RequestException):
            self.logger.warning(f"Request error: {error} for {return_entry.identifier}")

            return_entry.error_code = -1
            return_entry.error_msg = str(error)
            return_entry.retry_count += 1

            return return_entry.retry_count < _MAX_RETRIES
        else:
            self.logger.error(f"Error: {error} for {return_entry.identifier}")

            return_entry.error_code = -2
            return_entry.error_msg = str(error)

        return False

    @staticmethod
    def image_resize(image: np.ndarray, max_size=1024) -> tuple[np.ndarray[int, np.dtype[np.uint8]], np.ndarray[int, np.dtype[np.uint32]]]:
        """
        Resize an image while preserving aspect ratio.
        
        Args:
            image: NumPy array containing the image data
            max_size: Maximum dimension for the resized image
            
        Returns:
            tuple: (resized_image, new_dimensions) where dimensions are [height, width]
        """
        h, w = image.shape[:2]
        if h > w:
            new_h = max_size
            new_w = int(w * (new_h / h))
        else:
            new_w = max_size
            new_h = int(h * (new_w / w))
        return cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_AREA), np.array([new_h, new_w])
