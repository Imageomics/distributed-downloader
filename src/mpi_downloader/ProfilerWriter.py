import cv2
import h5py
import numpy as np

from mpi_downloader import CompletedBatch
from mpi_downloader.dataclasses import error_entry, error_dtype, profile_dtype

sample_length = 5

def write_batch(
        profiles_hdf: h5py.Dataset,
        errors_hdf: h5py.Dataset,
        completed_batch: CompletedBatch,
        rate_limit: float,
        rank: int,
        offset: int,
        batch_size: int,
        server_name: str,
        total_batches: int,
        output_path: str
):
    # os.makedirs(f"{output_path}/samples", exist_ok=True)

    successes_number = completed_batch.success_queue.qsize()
    errors_number = completed_batch.error_queue.qsize()

    errors_list = []

    for _ in range(errors_number):
        error_download = completed_batch.error_queue.get()
        errors_list.append(error_entry.from_downloaded(error_download).to_np())

    for idx in range(successes_number):
        success_download = completed_batch.success_queue.get()
        try:
            np_image = np.asarray(bytearray(success_download.image), dtype="uint8")
            original_image = cv2.imdecode(np_image, cv2.IMREAD_COLOR)
            # original_image = cv2.cvtColor(cv_image, cv2.COLOR_BGR2RGB)

            if original_image is None:
                raise ValueError("Corrupted Image")

            resized_image = cv2.resize(original_image, (1024, 1024), interpolation=cv2.INTER_LINEAR)

            if idx < sample_length:
                cv2.imwrite(f"{output_path}/samples/{server_name}_{idx}.jpg", resized_image)

        except Exception as e:
            errors_list.append(error_entry(
                uuid=success_download.UUID,
                identifier=success_download.identifier,
                retry_count=0,
                error_code=-3,
                error_msg=str(e)
            ).to_np())

            errors_number += 1
            successes_number -= 1

    print(f"Rank {rank} writing to HDF5 {successes_number} successes and {errors_number} errors")

    errors_np = np.array(errors_list, dtype=error_dtype).reshape((-1,))
    server_profile_np = np.array(
            [
                (
                    server_name,
                    total_batches,
                    successes_number,
                    errors_number,
                    rate_limit
                )
            ],
            dtype=profile_dtype)

    profiles_hdf[offset] = server_profile_np
    errors_hdf[offset * batch_size:offset * batch_size + errors_number] = errors_np
