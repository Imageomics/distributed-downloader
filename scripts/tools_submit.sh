#!/bin/bash

set -e

SCRIPTS_DIR=$(dirname "$(realpath "$0")")
REPO_ROOT=$(dirname "$(realpath "${SCRIPTS_DIR}")")
export REPO_ROOT
logs_dir="${OUTPUT_TOOLS_LOGS_FOLDER}"
mkdir -p "${logs_dir}"

# Check if any arguments were passed
if [ "$#" -eq 0 ]; then
    echo "Usage: $0 script1 tool_name dependency"
    exit 1
fi

script=$1
if [ ! -f "$script" ]; then
  echo "Error: File '$script' not found"
fi

filename=$(basename "$script")
ext="${filename##*.}"
base_filename=$(basename "${filename}" ."${ext}")
tool_name=$2
dependency=$3

# Submit the script to Slurm
if [ -z "${dependency}" ]; then
  sbatch \
    --output="${logs_dir}/${base_filename}.out" \
    --error="${logs_dir}/${base_filename}.err" \
    --nodes="${TOOLS_MAX_NODES}" \
    --ntasks-per-node="${TOOLS_WORKERS_PER_NODE}" \
    --cpus-per-task="${TOOLS_CPU_PER_WORKER}" \
    --account="${ACCOUNT}" \
    "${script}" \
    "${tool_name}"
  exit 0
fi

sbatch \
  --output="${logs_dir}/${base_filename}.out" \
  --error="${logs_dir}/${base_filename}.err" \
  --nodes="${TOOLS_MAX_NODES}" \
  --ntasks-per-node="${TOOLS_WORKERS_PER_NODE}" \
  --cpus-per-task="${TOOLS_CPU_PER_WORKER}" \
  --dependency=afterany:"${dependency}" \
  --account="${ACCOUNT}" \
  "${script}" \
  "${tool_name}"
