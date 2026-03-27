#!/bin/bash
#SBATCH --job-name tool_filter
#SBATCH --mem=0
#SBATCH --time=01:00:00

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 tool_name"
    exit 1
fi

tool_name=$1

logs_dir="${OUTPUT_TOOLS_LOGS_FOLDER}"
mkdir -p "$logs_dir"

driver_memory="110G"
executor_memory="64G"

module load spark/3.4.1
# OSC Cardinal requires explicit version — check `module spider miniconda3` if this fails
module load miniconda3/24.1.2-py310
source "${REPO_ROOT}/.venv/bin/activate"
export PYTHONPATH=${PYTHONPATH}:"${REPO_ROOT}/src":"${REPO_ROOT}/distributed-downloader"

pbs-spark-submit \
    --driver-memory $driver_memory \
    --executor-memory $executor_memory \
    "${REPO_ROOT}/src/distributed_downloader/tools/filter.py" \
    "${tool_name}" \
    > "${logs_dir}/tool_filter.log"
