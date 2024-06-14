#!/bin/bash
#SBATCH --job-name server_profiling_data_prep
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=48
#SBATCH --time=00:30:00
#SBATCH --account=pas2136

# ============================ PARAMETERS ======================================
# file to be converted to parquets
filename="multimedia.txt"

# configure spark
driver_memory="64G"
executor_memory="64G"

# ================== DO NOT TOUCH BELOW THIS LINE ==============================
source "${REPO_ROOT}/config/osc.env"
module load spark/3.4.1 # try a newer version of spark

TIME_STAMP=2024-05-01

ext="${filename##*.}"
base_filename=$(basename "${filename}" ."${ext}")

images_urls_path="${GBIF_CACHE_ROOT}/${TIME_STAMP}/${filename}"
output_folder_path="${PROCESSED_DATA_ROOT}/${TIME_STAMP}/${base_filename}_prep"

# pbs-spark-submit is a wrapper script that sets up the environment for Spark
# memory limit per node: 177G
pbs-spark-submit \
    --driver-memory $driver_memory \
    --executor-memory $executor_memory \
    "${REPO_ROOT}/src/server_prep.py" \
    "$images_urls_path" \
    "$output_folder_path" \
    > "${REPO_ROOT}/logs/server_profiling_data_prep.log"