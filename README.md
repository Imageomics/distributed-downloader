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

1. Install Python 3.10 or 3.11
2. Install MPI, any MPI should work, tested with OpenMPI and IntelMPI. Installation instructions can be found on
   official websites:
    - [OpenMPI](https://docs.open-mpi.org/en/v5.0.x/installing-open-mpi/quickstart.html)
    - [IntelMPI](https://www.intel.com/content/www/us/en/docs/mpi-library/developer-guide-linux/2021-6/installation.html)
3. Install the required package:
    - For general use:
      ```commandline
      pip install distributed-downloader
      ```
    - For development:
      ```commandline
      pip install -e .[dev]
      ```

## How to Use

`distributed-downloader` utilizes multiple nodes on a High Performance Computing (HPC) system (specifically, an HPC
with `slurm` workload manager) to download a collection of images specified in a given tab-delimited text file.

### Configuration

The downloader is configured using a YAML configuration file. Here's an example configuration:

```yaml
# Example configuration file
path_to_input: "/path/to/input/urls.csv"
path_to_output: "/path/to/output"

output_structure:
  urls_folder: "urls"
  logs_folder: "logs"
  images_folder: "images"
  schedules_folder: "schedules"
  profiles_table: "profiles.csv"
  ignored_table: "ignored.csv"
  inner_checkpoint_file: "checkpoint.json"
  tools_folder: "tools"

downloader_parameters:
  num_downloads: 1
  max_nodes: 20
  workers_per_node: 20
  cpu_per_worker: 1
  header: true
  image_size: 224
  logger_level: "INFO"
  batch_size: 10000
  rate_multiplier: 0.5
  default_rate_limit: 3

tools_parameters:
  num_workers: 1
  max_nodes: 10
  workers_per_node: 20
  cpu_per_worker: 1
  threshold_size: 10000
  new_resize_size: 224
```

### Main script

There is one manual step to get the downloader running as designed:
You need to call function `download_images` from package `distributed_downloader` with the `config_path` as an argument.
This will initialize filestructure in the output folder, partition the input file, profile the servers for their
possible download speed, and start downloading images. If downloading didn't finish, you can call the same function with
the same `config_path` argument to continue downloading.

```python
from distributed_downloader import download_images

# Start or continue downloading
download_images("/path/to/config.yaml")
```

Downloader has two logging profiles:

- `INFO` - logs only the most important information, for example, when a batch is started and finished. It also logs out
  any error that occurred during download, image decoding, or writing batch to the filesystem
- `DEBUG` - logs all information, for example, logging start and finish of each downloaded image.

### Tools script

After downloading is finished, you can use the `tools` package to perform various operations on the downloaded images.
To do this, you need to call the function `apply_tools` from package `distributed_downloader` with the `config_path`
and `tool_name` as an argument.

```python
from distributed_downloader import apply_tools

# Apply a specific tool
apply_tools("/path/to/config.yaml", "resize")
```

The following tools are available:

- `resize` - resizes images to a new size (specified in config)
- `image_verification` - verifies images by checking if they are corrupted
- `duplication_based` - removes duplicate images using MD5 hashing
- `size_based` - removes images that are too small (threshold specified in config)

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

Example of creating a custom tool:

```python
from distributed_downloader.tools import FilterRegister, SchedulerRegister, RunnerRegister, ToolsBase


@FilterRegister("my_custom_tool")
class MyCustomToolFilter(ToolsBase):
  def run(self):
    # Implementation of filter step
    pass


@SchedulerRegister("my_custom_tool")
class MyCustomToolScheduler(ToolsBase):
  def run(self):
    # Implementation of scheduler step
    pass


@RunnerRegister("my_custom_tool")
class MyCustomToolRunner(ToolsBase):
  def run(self):
    # Implementation of runner step
    pass
```

## Environment Variables

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
  - uuid: string - downloaded dataset internal unique identifier (created to distinguish between all component datasets downloaded with this package)
  - source_id: string - id of the entry provided by its source (e.g., `gbifID`)
  - identifier: string - source URL of the image
  - is_license_full: boolean - True indicates that `license`, `source`, and `title` all have non-null values for that
    particular entry.
    - license: string
    - source: string
    - title: string
  - hashsum_original: string - MD5 hash of the original image data
  - hashsum_resized: string - MD5 hash of the resized image data
  - original_size: [height, width] - dimensions of original image
  - resized_size: [height, width] - dimensions after resizing
  - image: bytes - binary image data

- `errors.parquet`:
  - uuid: string - downloaded dataset internal unique identifier (created to distinguish between all component datasets downloaded with this package)
  - identifier: string - URL of the image
  - retry_count: integer - number of download attempts
  - error_code: integer - HTTP or other error code
  - error_msg: string - detailed error message

For general operations (that do not involve access to `image` column, e.g. count the total number of entries, create
size distribution etc.) it is recommended to use Spark or similar applications. For any operation that does involve
`image` column, it is recommended to use Pandas or similar library to access each parquet file separately.

## Supported Image Formats

The downloader supports most common image formats, including:

- JPEG/JPG
- PNG
- GIF (first frame only)
- BMP
- TIFF

## Error Handling and Troubleshooting

Common issues and solutions:

1. **Rate limiting errors**: If you see many errors with code 429, adjust the `default_rate_limit` in your config to a
   lower value.

2. **Memory issues**: If the process is killed due to memory constraints, try reducing `batch_size` or
   `workers_per_node` in your config.

3. **Corrupt images**: Images that cannot be decoded are logged in the errors parquet file with appropriate error codes.

4. **Resuming failed downloads**: The downloader creates checkpoints automatically. Simply run the same command again to
   resume from the last checkpoint.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
