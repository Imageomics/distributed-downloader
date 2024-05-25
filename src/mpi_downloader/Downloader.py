import concurrent.futures
import hashlib
import logging
import queue
import threading
import time
from typing import List, Dict, Any, Tuple

import cv2
import numpy as np
import requests

from mpi_downloader.dataclasses import DownloadedImage, CompletedBatch, RateLimit

_MAX_RETRIES = 5
_TIMEOUT = 5

_RETRY_ERRORS = [429, 500, 501, 502, 503, 504]


class Downloader:
    def __init__(self,
                 header: dict,
                 session: requests.Session,
                 rate_limit: RateLimit,
                 img_size: int = 1024,
                 job_end_time: int = 0,
                 convert_image: bool = True,
                 logger=logging.getLogger()):
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
        return_entry.end_time = time.perf_counter()

        self.logger.debug(f"Processing {return_entry.identifier}")

        if not self.convert_image:
            return_entry.image = raw_image_bytes
            self.logger.debug(f"No image conversion for {return_entry.identifier}")
            return

        np_image = np.asarray(bytearray(raw_image_bytes), dtype="uint8")
        original_image = cv2.imdecode(np_image, cv2.IMREAD_COLOR)
        # original_image = cv2.cvtColor(cv_image, cv2.COLOR_BGR2RGB)

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
        if isinstance(error, TimeoutError):
            self.logger.info(f"Timout, trying to exit")

            self._exit = True
            self._condition.notify()
            raise False
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
        h, w = image.shape[:2]
        if h > w:
            new_h = max_size
            new_w = int(w * (new_h / h))
        else:
            new_w = max_size
            new_h = int(h * (new_w / w))
        return cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_AREA), np.array([new_h, new_w])
