#!/bin/bash

set -e

SCRIPTS_DIR=$(dirname "$(realpath "$0")")
REPO_ROOT=$(dirname "$(realpath "${SCRIPTS_DIR}")")
export REPO_ROOT
logs_dir="${OUTPUT_LOGS_FOLDER}"
mkdir -p "${logs_dir}"

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 script1 [dependencies...]"
    exit 1
fi

script=$1
if [ ! -f "$script" ]; then
  echo "Error: File '$script' not found"
fi

filename=$(basename "$script")
ext="${filename##*.}"
base_filename=$(basename "${filename}" ."${ext}")
export LOGS_BASE_FILENAME=$base_filename
dependencies=$(IFS=,; echo "${*:2}")

# Submit the script to Slurm
if [ -z "${dependencies}" ]; then
  sbatch \
    --output="${logs_dir}/${base_filename}.out" \
    --error="${logs_dir}/${base_filename}.err" \
    --nodes="${DOWNLOADER_MAX_NODES}" \
    --ntasks-per-node="${DOWNLOADER_WORKERS_PER_NODE}" \
    --cpus-per-task="${DOWNLOADER_CPU_PER_WORKER}" \
    --account="${ACCOUNT}" \
    "${script}"
  exit 0
fi

sbatch \
  --output="${logs_dir}/${base_filename}.out" \
  --error="${logs_dir}/${base_filename}.err" \
  --dependency=afterany:"${dependencies}" \
  --nodes="${DOWNLOADER_MAX_NODES}" \
  --ntasks-per-node="${DOWNLOADER_WORKERS_PER_NODE}" \
  --cpus-per-task="${DOWNLOADER_CPU_PER_WORKER}" \
  --account="${ACCOUNT}" \
  "${script}"
