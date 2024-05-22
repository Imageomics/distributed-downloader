#!/bin/bash
#SBATCH --job-name test
#SBATCH --nodes=1
#SBATCH --cpus-per-task=3
#SBATCH --time=00:05:00
#SBATCH --account=pas2136

# ============================ PARAMETERS ======================================
set -e

batch_size=10000
max_nodes=10
max_worker_per_node=4

# ================== DO NOT TOUCH BELOW THIS LINE ==============================
source "${REPO_ROOT}/config/osc.env"

input_path="${PROCESSED_DATA_ROOT}/${TIME_STAMP}/multimedia_prep"

module load intel/2021.10.0
module load intelmpi/2021.10
module load hdf5/1.12.2
module load miniconda3/23.3.1-py310
source .venv/gbif_venv/bin/activate
export PYARROW_IGNORE_TIMEZONE=1

# export I_MPI_DEBUG=100
# unset I_MPI_PMI_LIBRARY
# unset I_MPI_HYDRA_BOOTSTRAP
export I_MPI_JOB_RESPECT_PROCESS_PLACEMENT=0

# memory limit per node: 177G
srun --mpi=pmi2 --nodes=1 --ntasks-per-node=1 --cpus-per-task=3 \
    python \
    "${REPO_ROOT}/src/MPI_server_profiler_controller.py" \
    "$input_path" \
    "$max_nodes" \
    "$max_worker_per_node" \
    "$batch_size" \
    > "${input_path}/logs/MPI_server_profiler_controller.log"

# srun \
# --mpi=pmi2 \
#  --nodes=$max_nodes \
#  --ntasks-per-node=$max_worker_per_node \
#  --cpus-per-task=10 \
#  --output="${input_path}/logs/MPI_server_profiler-%2t.log" \
#  python src/MPI_server_profiler.py \
#  "$input_path" \
#  "$batch_size"
