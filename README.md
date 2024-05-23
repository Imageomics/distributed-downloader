# Distributed Downloader
MPI-based distributed downloading tool for retrieving data from diverse domains.

## Background

This MPI-based distributed downloader was initially designed for the purpose of downloading all images from the monthly [GBIF occurrence snapshot](https://www.gbif.org/occurrence-snapshots). The overall setup is general enough that it could be transformed into a functional tool beyond just our use; it should work on any list of URLs. We chose to build this tool instead of using something like [img2dataset](https://github.com/rom1504/img2dataset) to better avoid overloading source servers (GBIF documents approximately 200M images) and have more control over the final dataset construction and metadata management (e.g., using `HDF5` as discussed in [issue #1](https://github.com/Imageomics/distributed-downloader/issues/1)). 


## How to Use

`distributed-downloader` utilizes multiple nodes on a High Performance Computing (HPC) system (specifically, an HPC with `slurm` workload manager) to download a collection of images specified in a given CSV or TXT file. There are three manual steps to get the downloader running as designed; the first two function as a preprocessing step (to be done once with the initial file), and the third initiates the download (this step may be run multiple times for pre-established periods and each will pick up where the last left off).

1. The first step is to run the file through `src/server_prep.py`. This includes partitioning the dataset by server to generate batches of 10K URLs per server. Servers are determined by the URL in the input file. Additionally, it adds a UUID to each entry in the file to ensure preservation of provenance throughout the download and processing and beyond. This processing is still GBIF occurrence snapshot-specific in that it includes filters on the input file by media type (`StillImage`), checks for missing `gbifID`, and checks that the record indeed contains an image (through the `format` column).

2. After the partitioning and filtering, `MPI_download_prep.py` must be run to establish the rate limits (by server) for the download. An "average" rate limit is established and then scaled based on the number of batches/simultaneous downloads per server (to avoid overloading a server while running simultaneous downloads). After the download is initialized, manual adjustments were made based on results. Additionally, if a server returns any retry error (`429, 500, 501, 502, 503, 504`), the request rate for that server is reduced.

3. Finally, `submitter.py` is run with the path to the `.env` file for various download settings and paths. This can be run for set periods of time and will restart where it has left off on the next run. Timing for batches is set in the `slurm` scripts passed through the `.env`.


### Running on other systems

The parameters for step 3 can all be set in the configuration file. This includes information about your HPC account and paths to various files, as well as distribution of work and download settings; be sure to fill in your information. 
The configuration file (`config/hpc.env`) should be in this location relative to the root of the directory from which these files are being run. 

The provided `slurm` scripts for running steps 1 and 2 (`scripts/server_downloading_prep.slurm` and `scripts/server_profiling.slurm`, which are only done once at the start of the project) must have the account info changed at the top of their files (`#SBATCH --account=<your account here>`).


## Note on files

`resize_mpi` (`py` and `slurm`) and `resizer_scheduler.py` are scripts intended to resize the images after download. For instance, in the case that the initial download size is set higher than intended, these can be used to adjust the size within the given structure and repackage it. They have not been generalized to fit in with the remaining package infrastructure and are simply extra tools that we used; they may be generalized in the future.
