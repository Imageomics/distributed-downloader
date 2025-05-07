#!/bin/bash
#SBATCH --job-name schedule_creation
#SBATCH --time=00:05:00

set -e

module load miniconda3/23.3.1-py310
source "${REPO_ROOT}/.venv/bin/activate"
export PYARROW_IGNORE_TIMEZONE=1

python \
  "${DISTRIBUTED_DOWNLOADER_PATH}/core/MPI_download_prep.py" \
  > "${logs_dir}/MPI_downloader_prep.log"

mv "${logs_dir}/${LOGS_BASE_FILENAME}.out" "${logs_dir}/current"
mv "${logs_dir}/${LOGS_BASE_FILENAME}.err" "${logs_dir}/current"
mv "${logs_dir}/MPI_downloader_prep.log" "${logs_dir}/current"
