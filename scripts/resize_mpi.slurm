#!/bin/bash
#SBATCH --job-name resize_mpi
#SBATCH --nodes=20
#SBATCH --ntasks-per-node=5
#SBATCH --cpus-per-task=8
#SBATCH --time=10:00:00
#SBATCH --mem=0
#SBATCH --account=pas2136

source "${REPO_ROOT}/config/hpc.env"

module load intel/2021.10.0
module load intelmpi/2021.10
module load miniconda3/23.3.1-py310
source .venv/gbif_venv/bin/activate
export PYARROW_IGNORE_TIMEZONE=1

export I_MPI_JOB_RESPECT_PROCESS_PLACEMENT=0


srun \
  --mpi=pmi2 \
  --nodes=20 \
  --ntasks-per-node=5 \
  --cpus-per-task=8 \
  --mem=0 \
  --output="${REPO_ROOT}/logs/resize/resize-%2t.log" \
  python \
  "${REPO_ROOT}/src/resize_mpi.py"
