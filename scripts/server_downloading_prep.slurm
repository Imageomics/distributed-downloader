#!/bin/bash
#SBATCH --job-name download_prep
#SBATCH --time=00:05:00
#SBATCH --account=pas2136

source "${REPO_ROOT}/config/osc.env"

input_path="${PROCESSED_DATA_ROOT}/${TIME_STAMP}/multimedia_prep"

logs_dir="${input_path}/${DOWNLOADER_LOGS_FOLDER}"
mkdir -p "${logs_dir}"

module load intel/2021.10.0
module load intelmpi/2021.10
module load hdf5/1.12.2
module load miniconda3/23.3.1-py310
source .venv/gbif_venv/bin/activate
export PYARROW_IGNORE_TIMEZONE=1

# memory limit per node: 177G
srun \
  --mpi=pmi2 \
  --nodes=1 \
  --ntasks-per-node=1 \
  --cpus-per-task=1 \
  --output="${logs_dir}/MPI_downloader_prep.log" \
  python \
  "${REPO_ROOT}/src/MPI_download_prep.py" \
  "$input_path" \
  "$DOWNLOADER_MAX_NODES" \
  "$DOWNLOADER_WORKERS_PER_NODE"
