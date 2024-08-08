#!/bin/bash
#SBATCH --job-name initialization
#SBATCH --nodes=4
#SBATCH --time=00:30:00

set -e

driver_memory="110G"
executor_memory="64G"

module load spark/3.4.1

pbs-spark-submit \
    --driver-memory $driver_memory \
    --executor-memory $executor_memory \
    "${REPO_ROOT}/src/distributed_downloader/core/initialization.py" \
    > "${OUTPUT_LOGS_FOLDER}/initialization.log"
