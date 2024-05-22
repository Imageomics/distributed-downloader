#!/bin/bash

SCRIPTS_DIR=$(dirname "$(realpath "$0")")
REPO_ROOT=$(dirname "$(realpath "${SCRIPTS_DIR}")")
export REPO_ROOT

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 script1 schedule# iteration_number [dependency]"
    exit 1
fi

script=$1
schedule=$2
iteration_number=$3

LOGS_DIR="${REPO_ROOT}/logs/${schedule}/${iteration_number}"
mkdir -p "${LOGS_DIR}"

if [ "$4" != "" ] && [ "$4" != "--recheck" ]; then
    dependency=$4
    filename=$(basename "$script")
    ext="${filename##*.}"
    base_filename=$(basename "${filename}" ."${ext}")

    # Submit the script to Slurm
    sbatch --output="${LOGS_DIR}/${base_filename}.out" --error="${LOGS_DIR}/${base_filename}.err" --dependency=afterany:"${dependency}" "${script}" "${schedule}" "${iteration_number}" "$5" --nodes="${DOWNLOADER_MAX_NODES}" --ntasks-per-node="${DOWNLOADER_WORKERS_PER_NODE}" --cpus-per-task="${DOWNLOADER_CPU_PER_TASK}" --account="${ACCOUNT}"
    exit 0
else
    filename=$(basename "$script")
    ext="${filename##*.}"
    base_filename=$(basename "${filename}" ."${ext}")

    # Submit the script to Slurm
    sbatch --output="${LOGS_DIR}/${base_filename}.out" --error="${LOGS_DIR}/${base_filename}.err" "${script}" "${schedule}" "${iteration_number}" "$4" --nodes="${DOWNLOADER_MAX_NODES}" --ntasks-per-node="${DOWNLOADER_WORKERS_PER_NODE}" --cpus-per-task="${DOWNLOADER_CPU_PER_TASK}" --account="${ACCOUNT}"
    exit 0
fi

