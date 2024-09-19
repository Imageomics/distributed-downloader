# Distributed Downloader

MPI-based distributed downloading tool for retrieving data from diverse domains.

## Background

This MPI-based distributed downloader was initially designed for the purpose of downloading all images from the
monthly [GBIF occurrence snapshot](https://www.gbif.org/occurrence-snapshots). The overall setup is general enough that
it could be transformed into a functional tool beyond just our use; it should work on any list of URLs. We chose to
build this tool instead of using something like [img2dataset](https://github.com/rom1504/img2dataset) to better avoid
overloading source servers (GBIF documents approximately 200M images across 545 servers) and have more control over the
final dataset construction and metadata management (e.g., using `HDF5` as discussed
in [issue #1](https://github.com/Imageomics/distributed-downloader/issues/1)).

## Installation Instructions

### Conda installation

1. Install [Miniconda](https://docs.conda.io/en/latest/miniconda.html)
2. Create a new conda environment:
    ```commandline
    conda env create -f environment.yaml --solver=libmamba -y
    ```

### Pip installation

1. Install Python 3.10 or higher
2. Install MPI, any MPI should work, tested with OpenMPI and IntelMPI. Installation instructions can be found on
   official websites:
    - [OpenMPI](https://docs.open-mpi.org/en/v5.0.x/installing-open-mpi/quickstart.html)
    - [IntelMPI](https://www.intel.com/content/www/us/en/docs/mpi-library/developer-guide-linux/2021-6/installation.html)
3. Install required package:
    - For general use:
      ```commandline
      pip install git+https://github.com/Imageomics/distributed-downloader
      ```
    - For development:
      ```commandline
      pip install .[dev]
      ```

## How to Use

`distributed-downloader` utilizes multiple nodes on a High Performance Computing (HPC) system (specifically, an HPC
with `slurm` workload manager) to download a collection of images specified in a given tab-delimited text file.

### Main script

There are one manual step to get the downloader running as designed:
You need to call function `download_images` from package `distributed_downloader` with the `config_path` as an argument.
This will initialize filestructure in the output folder, partition the input file, profile the servers for their
possible download speed, and start downloading images. If downloading didn't finish, you can call the same function with
the same `config_path` argument to continue downloading.

Downloader has two logging profiles:

- `INFO` - logs only the most important information, for example when a batch is started and finished. It also logs out
  any error that occurred during download, image decoding, or writing batch to the filesystem
- `DEBUG` - logs all information, for example logging start and finish of each downloaded image.

### Tools script

After downloading is finished, you can use the `tools` package perform various operations on them.
To do this, you need to call the function `apply_tools` from package `distributed_downloader` with the `config_path`
and `tool_name` as an argument.
Following tools are available:

- `resize` - resizes images to a new size
- `image_verification` - verifies images by checking if they are corrupted
- `duplication_based` - removes duplicate images
- `size_based` - removes images that are too small

You can also add your own tool, the instructions are in the section below.

### Creating a new tool

You can also add your own tool by creating 3 classes and registering them with respective decorators.

- Each tool's output will be saved in separate folder in `{config.output_structure.tools_folder}/{tool_name}`
- There are 3 steps in the tool pipeline: `filter`, `scheduler` and `runner`.
    - `filter` - filters the images that should be processed by the tool and creates csv files with them
    - `scheduler` - creates a schedule for processing the images for MPI
    - `runner` - processes the images using MPI
- Each step should be implemented in a separate class.
- Tool name should be the same across all classes.
- Each tool should inherit from `ToolsBase` class.
- Each tool should have a `run` method that will be called by the main script.
- Each tool should be registered with a decorator from a respective package (`FilterRegister` from `filters` etc.)

## Rules for scripts:

All scripts can expect to have the following custom environment variables, specific variables are only initialized
when respective tool is called:

- General parameters
    - `CONFIG_PATH`
    - `ACCOUNT`
    - `PATH_TO_INPUT`
    - `PATH_TO_OUTPUT`
    - `OUTPUT_URLS_FOLDER`
    - `OUTPUT_LOGS_FOLDER`
    - `OUTPUT_IMAGES_FOLDER`
    - `OUTPUT_SCHEDULES_FOLDER`
    - `OUTPUT_PROFILES_TABLE`
    - `OUTPUT_IGNORED_TABLE`
    - `OUTPUT_INNER_CHECKPOINT_FILE`
    - `OUTPUT_TOOLS_FOLDER`
- Specific for downloader
    - `DOWNLOADER_NUM_DOWNLOADS`
    - `DOWNLOADER_MAX_NODES`
    - `DOWNLOADER_WORKERS_PER_NODE`
    - `DOWNLOADER_CPU_PER_WORKER`
    - `DOWNLOADER_HEADER`
    - `DOWNLOADER_IMAGE_SIZE`
    - `DOWNLOADER_LOGGER_LEVEL`
    - `DOWNLOADER_BATCH_SIZE`
    - `DOWNLOADER_RATE_MULTIPLIER`
    - `DOWNLOADER_DEFAULT_RATE_LIMIT`
- Specific for tools
    - `TOOLS_NUM_WORKERS`
    - `TOOLS_MAX_NODES`
    - `TOOLS_WORKERS_PER_NODE`
    - `TOOLS_CPU_PER_WORKER`
    - `TOOLS_THRESHOLD_SIZE`
    - `TOOLS_NEW_RESIZE_SIZE`

## Working with downloaded data

Downloaded data is stored in `images_folder` (configured in config file),
partitioned by `server_name` and `partition_id`, in two parquet files with following schemes:

- `successes.parquet`:
    - uuid: string
    - source_id: string
    - identifier: string
    - is_license_full: boolean
    - license: string
    - source: string
    - title: string
    - hashsum_original: string
    - hashsum_resized: string
    - original_size: [height, width]
    - resized_size: [height, width]
    - image: bytes
- `errors.parquet`:
    - uuid: string
    - identifier: string
    - retry_count: integer
    - error_code: integer
    - error_msg: string

For general operations (that do not involve access to `image` column, e.g. count the total number of entries, create
size distribution etc.) it is recommended to use Spark or similar applications. For any operation that does involve
`image` column, it is recommended to use Pandas or similar library to access each parquet file separately.
