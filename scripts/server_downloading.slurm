#!/bin/bash
#SBATCH --job-name mpi_downloader
#SBATCH --mem=0
#SBATCH --time=02:00:00

# ============================ PARAMETERS ======================================

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 schedule# iteration_number"
    exit 1
fi

schedule=$1
iteration_number=$2

# ================== DO NOT TOUCH BELOW THIS LINE ==============================
source "${REPO_ROOT}/config/hpc.env"

input_path="${PROCESSED_DATA_ROOT}/${TIME_STAMP}/${DOWNLOAD_DIR}"

schedule_path="${input_path}/${DOWNLOADER_SCHEDULES_FOLDER}/${schedule}"
logs_dir="${input_path}/${DOWNLOADER_LOGS_FOLDER}/${schedule}/${iteration_number}"
mkdir -p "${logs_dir}"

module load intel/2021.10.0
module load intelmpi/2021.10
module load miniconda3/23.3.1-py310
source "${REPO_ROOT}/.venv/bin/activate"
export PYARROW_IGNORE_TIMEZONE=1

export I_MPI_JOB_RESPECT_PROCESS_PLACEMENT=0

# memory limit per node: 177G
{
  srun --mpi=pmi2 --nodes=1 --ntasks-per-node=1 --cpus-per-task=1 --output="${logs_dir}/MPI_multimedia_downloader.log" \
    python \
    "${REPO_ROOT}/src/distributed-downloader/MPI_multimedia_downloader_controller.py" \
    "$input_path" \
    "$schedule_path" \
    "$DOWNLOADER_MAX_NODES" \
    "$DOWNLOADER_WORKERS_PER_NODE"
} && {
  srun \
  --mpi=pmi2 \
  --nodes="$DOWNLOADER_MAX_NODES" \
  --ntasks-per-node="$DOWNLOADER_WORKERS_PER_NODE" \
  --cpus-per-task="$DOWNLOADER_CPU_PER_TASK" \
  --mem=0 \
  --output="${logs_dir}/MPI_multimedia_downloader-%2t.log" \
  python "${REPO_ROOT}/src/distributed-downloader/MPI_multimedia_downloader.py" \
  "$input_path" \
  "$schedule_path" \
  --header "$HEADER" \
  --img-size "$IMAGE_SIZE" \
  --logging-level "$LOGGER_LEVEL"
}
