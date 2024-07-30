#!/bin/bash
#SBATCH --job-name mpi_verifier
#SBATCH --time=00:02:00

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 schedule# iteration_number"
    exit 1
fi

schedule=$1
iteration_number=$2

schedule_path="${OUTPUT_SCHEDULES_FOLDER}/current/${schedule}"
logs_dir="${OUTPUT_LOGS_FOLDER}/current/${schedule}/${iteration_number}"
mkdir -p "${logs_dir}"

module load intel/2021.10.0
module load intelmpi/2021.10
module load miniconda3/23.3.1-py310
source "${REPO_ROOT}/.venv/bin/activate"
export PYARROW_IGNORE_TIMEZONE=1
export I_MPI_JOB_RESPECT_PROCESS_PLACEMENT=0

srun \
  --mpi=pmi2 \
  --nodes=1 \
  --ntasks-per-node=1 \
  --cpus-per-task=1 \
  --output="${logs_dir}/MPI_downloader_verifier.log" \
  python \
  "${REPO_ROOT}/src/distributed_downloader/core/MPI_downloader_verifier.py" \
  "$schedule_path"
