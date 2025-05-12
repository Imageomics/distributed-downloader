#!/bin/bash
#SBATCH --job-name schedule_creation
#SBATCH --time=00:05:00

set -e

module load miniconda3/23.3.1-py310
source "${REPO_ROOT}/.venv/bin/activate"
export PYARROW_IGNORE_TIMEZONE=1

python \
  "${DISTRIBUTED_DOWNLOADER_PATH}/core/MPI_download_prep.py" \
  > "${OUTPUT_LOGS_FOLDER}/MPI_downloader_prep.log"

mv "${OUTPUT_LOGS_FOLDER}/${LOGS_BASE_FILENAME}.out" "${OUTPUT_LOGS_FOLDER}/current"
mv "${OUTPUT_LOGS_FOLDER}/${LOGS_BASE_FILENAME}.err" "${OUTPUT_LOGS_FOLDER}/current"
mv "${OUTPUT_LOGS_FOLDER}/MPI_downloader_prep.log" "${OUTPUT_LOGS_FOLDER}/current"
