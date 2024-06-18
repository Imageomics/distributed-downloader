#!/bin/bash
#SBATCH --job-name resize_mpi
#SBATCH --nodes=10
#SBATCH --ntasks-per-node=3
#SBATCH --cpus-per-task=13
#SBATCH --time=02:00:00
#SBATCH --mem=0
#SBATCH --account=pas2136


module load intel/2021.10.0
module load intelmpi/2021.10
module load miniconda3/23.3.1-py310
source "${REPO_ROOT}/.venv/bin/activate"
export PYARROW_IGNORE_TIMEZONE=1

export I_MPI_JOB_RESPECT_PROCESS_PLACEMENT=0


srun \
  --mpi=pmi2 \
  --nodes=10 \
  --ntasks-per-node=3 \
  --cpus-per-task=13 \
  --mem=0 \
  --output="${REPO_ROOT}/logs/resize/resize-%2t.log" \
  python \
  "${REPO_ROOT}/tools/resize_mpi.py"
