#!/bin/bash
#SBATCH --job-name initialization
#SBATCH --time=00:30:00

driver_memory="110G"
executor_memory="64G"

module load spark/3.4.1

pbs-spark-submit \
    --driver-memory $driver_memory \
    --executor-memory $executor_memory \
    "${REPO_ROOT}/src/initialization.py" \
    > "${REPO_ROOT}/logs/initialization.log"