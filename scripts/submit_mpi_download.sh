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

logs_dir="${OUTPUT_LOGS_FOLDER}/current/${schedule}/${iteration_number}"
mkdir -p "${logs_dir}"

filename=$(basename "$script")
ext="${filename##*.}"
base_filename=$(basename "${filename}" ."${ext}")

if [ "$4" != "" ] && [ "$4" != "--recheck" ]; then
    dependency=$4

    # Submit the script to Slurm
    sbatch \
        --output="${logs_dir}/${base_filename}.out" \
        --error="${logs_dir}/${base_filename}.err" \
        --dependency=afterany:"${dependency}" \
        --nodes="${DOWNLOADER_MAX_NODES}" \
        --ntasks-per-node="${DOWNLOADER_WORKERS_PER_NODE}" \
        --cpus-per-task="${DOWNLOADER_CPU_PER_WORKER}" \
        --account="${ACCOUNT}" \
        "${script}" "${schedule}" "${iteration_number}" "$5"
    exit 0
else
    # Submit the script to Slurm
    sbatch \
        --output="${logs_dir}/${base_filename}.out" \
        --error="${logs_dir}/${base_filename}.err" \
        --nodes="${DOWNLOADER_MAX_NODES}" \
        --ntasks-per-node="${DOWNLOADER_WORKERS_PER_NODE}" \
        --cpus-per-task="${DOWNLOADER_CPU_PER_WORKER}" \
        --account="${ACCOUNT}" \
        "${script}" "${schedule}" "${iteration_number}" "$4"
    exit 0
fi

