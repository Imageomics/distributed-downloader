#!/bin/bash
#SBATCH --job-name initialization
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=40
#SBATCH --cpus-per-task=1
#SBATCH --time=00:30:00

driver_memory="110G"
executor_memory="64G"

module load spark/3.4.1

pbs-spark-submit \
    --driver-memory $driver_memory \
    --executor-memory $executor_memory \
    "${REPO_ROOT}/src/distributed_downloader/initialization.py" \
    > "${REPO_ROOT}/logs/initialization.log"