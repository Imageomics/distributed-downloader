#!/bin/bash
#SBATCH --job-name profiling
#SBATCH --nodes=1
#SBATCH --cpus-per-task=3
#SBATCH --time=00:05:00

set -e

module load miniconda3/23.3.1-py310
source "${REPO_ROOT}/.venv/bin/activate"
export PYARROW_IGNORE_TIMEZONE=1

python \
  "${REPO_ROOT}/src/distributed_downloader/core/fake_profiler.py" \
  > "${OUTPUT_LOGS_FOLDER}/profiler.log"
