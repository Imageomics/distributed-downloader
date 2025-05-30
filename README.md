# Distributed Downloader

A high-performance, MPI-based distributed downloading tool for retrieving large-scale image datasets from diverse web
sources.

## Overview

The Distributed Downloader was initially developed to handle the massive scale of downloading all images from the
monthly [GBIF occurrence snapshot](https://www.gbif.org/occurrence-snapshots), which contains approximately 200 million
images distributed across 545 servers. The tool is designed with general-purpose capabilities and can efficiently
process any collection of URLs.

### Why Build This Tool?

We chose to develop this custom solution instead of using existing tools
like [img2dataset](https://github.com/rom1504/img2dataset) for several key reasons:

- **Server-friendly operation**: Implements sophisticated rate limiting to avoid overloading source servers
- **Enhanced control**: Provides fine-grained control over dataset construction and metadata management
- **Scalability**: Handles massive datasets that exceed the capabilities of single-machine solutions
- **Fault tolerance**: Robust checkpoint and recovery system for long-running downloads
- **Flexibility**: Supports diverse output formats and custom processing pipelines

## Installation

### Prerequisites

- Python 3.10 or 3.11
- MPI implementation (OpenMPI or Intel MPI)
- High-performance computing environment with Slurm (recommended)

### Conda Installation (Recommended)

1. Install [Miniconda](https://docs.conda.io/en/latest/miniconda.html)
2. Create the environment:
   
   ```bash
   conda env create -f environment.yaml --solver=libmamba -y
   ```

### Pip Installation

1. Install Python 3.10 or 3.11
2. Install an MPI implementation:
    - [OpenMPI](https://docs.open-mpi.org/en/v5.0.x/installing-open-mpi/quickstart.html)
   - [Intel MPI](https://www.intel.com/content/www/us/en/docs/mpi-library/developer-guide-linux/2021-6/installation.html)
3. Install the package:
   
   ```bash
   # For general use
   pip install distributed-downloader

   # For development
   pip install -e .[dev]
   ```

### Script Configuration

After installation, create the necessary Slurm scripts for your environment. See
the [Scripts Documentation](./docs/scripts_README.md) for detailed instructions.

## Configuration

The downloader uses YAML configuration files to specify all operational parameters:

```yaml
# Core paths
path_to_input: "/path/to/input/urls.csv"
path_to_output: "/path/to/output"

# Output structure
output_structure:
  urls_folder: "urls"
  logs_folder: "logs"
  images_folder: "images"
  schedules_folder: "schedules"
  profiles_table: "profiles.csv"
  ignored_table: "ignored.csv"
  inner_checkpoint_file: "checkpoint.json"
  tools_folder: "tools"

# Downloader parameters
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

# Tools parameters
tools_parameters:
  num_workers: 1
  max_nodes: 10
  workers_per_node: 20
  cpu_per_worker: 1
  threshold_size: 10000
  new_resize_size: 224
```

## Usage

### Primary Downloading Interface

#### Python API

```python
from distributed_downloader import download_images

# Start or continue downloading process
download_images("/path/to/config.yaml")
```

#### Command-Line Interface

```bash
# Continue from current state
distributed_downloader /path/to/config.yaml

# Reset and restart from initialization
distributed_downloader /path/to/config.yaml --reset_batched

# Restart from profiling step
distributed_downloader /path/to/config.yaml --reset_profiled
```

**CLI Options**:

- No flags: Resume from current checkpoint
- `--reset_batched`: Restart completely, including file initialization and partitioning
- `--reset_profiled`: Keep partitioned files but redo server profiling

### Tools Pipeline

After completing the download process, use the tools pipeline to perform post-processing operations on downloaded
images.

#### Python API

```python
from distributed_downloader import apply_tools

# Apply a specific tool
apply_tools("/path/to/config.yaml", "resize")
```

#### Command-Line Interface

```bash
# Continue tool pipeline from current state
distributed_downloader_tools /path/to/config.yaml resize

# Reset pipeline stages
distributed_downloader_tools /path/to/config.yaml resize --reset_filtering
distributed_downloader_tools /path/to/config.yaml resize --reset_scheduling
distributed_downloader_tools /path/to/config.yaml resize --reset_runners

# Use custom tools not in registry
distributed_downloader_tools /path/to/config.yaml my_custom_tool --tool_name_override
```

**CLI Options**:

- No flags: Continue from current tool state
- `--reset_filtering`: Restart entire tool pipeline
- `--reset_scheduling`: Keep filtered data, redo scheduling
- `--reset_runners`: Keep scheduling, restart runner jobs
- `--tool_name_override`: Allow unregistered custom tools

### Available Tools

The following built-in tools are available:

- **`resize`**: Resizes images to specified dimensions
- **`image_verification`**: Validates image integrity and identifies corruption
- **`duplication_based`**: Removes duplicate images using MD5 hash comparison
- **`size_based`**: Filters out images below specified size thresholds

### Custom Tool Development

Create custom tools by implementing three pipeline stages:

```python
from distributed_downloader.tools import (FilterRegister, SchedulerRegister, RunnerRegister, PythonFilterToolBase,
                                          MPIRunnerTool, DefaultScheduler)


@FilterRegister("my_custom_tool")
class MyCustomToolFilter(PythonFilterToolBase):
  def run(self):
    # Filter implementation
    pass


@SchedulerRegister("my_custom_tool")
class MyCustomToolScheduler(DefaultScheduler):
  def run(self):
    # Scheduling implementation
    pass


@RunnerRegister("my_custom_tool")
class MyCustomToolRunner(MPIRunnerTool):
  def run(self):
    # Processing implementation
    pass
```

## Data Format and Storage

### Input Requirements

Input files must be tab-delimited or CSV format containing URLs with the following required columns:

- `uuid`: Unique internal identifier
- `identifier`: Image URL
- `source_id`: Source-specific identifier

Optional columns:

- `license`: License URL
- `source`: Source attribution
- `title`: Image title

### Output Structure

Downloaded data is stored in the configured `images_folder`, partitioned by server name and partition ID:

#### Success Records (`successes.parquet`)

- `uuid`: Dataset internal identifier
- `source_id`: Source-provided identifier
- `identifier`: Original image URL
- `is_license_full`: Boolean indicating complete license information
- `license`, `source`, `title`: Attribution information
- `hashsum_original`, `hashsum_resized`: MD5 hashes
- `original_size`, `resized_size`: Image dimensions
- `image`: Binary image data

#### Error Records (`errors.parquet`)

- `uuid`: Dataset internal identifier
- `identifier`: Failed image URL
- `retry_count`: Number of download attempts
- `error_code`: HTTP or internal error code
- `error_msg`: Detailed error description

## Supported Image Formats

The downloader supports common web image formats:

- JPEG/JPG
- PNG
- GIF (first frame extraction)
- BMP
- TIFF

## Logging and Monitoring

### Logging Levels

- **`INFO`**: Essential information including batch progress and errors
- **`DEBUG`**: Detailed information including individual download events

### Log Organization

Logs are organized hierarchically by:

- Pipeline stage (initialization, profiling, downloading)
- Batch number and iteration
- Worker process ID

See [Structure Documentation](./docs/structure_README.md) for detailed log organization.

## Performance and Troubleshooting

### Common Performance Issues

1. **Rate limiting errors (429, 403)**:

- Reduce `default_rate_limit` in configuration
- Increase `rate_multiplier` for longer delays

2. **Memory constraints**:

- Reduce `batch_size` or `workers_per_node`
- Monitor system memory usage

3. **Network timeouts**:

- Check connectivity to source servers
- Review firewall and proxy settings

### Error Recovery

The system automatically resumes from checkpoints. For manual intervention:

- Review error distributions in parquet files
- Check server-specific error patterns
- Use ignored server list for problematic hosts

See [Troubleshooting Guide](./docs/troubleshooting_README.md) for comprehensive error resolution.

## Environment Variables

The system exports numerous environment variables for script coordination:

**General Parameters**:

- `CONFIG_PATH`, `PATH_TO_INPUT`, `PATH_TO_OUTPUT`
- `OUTPUT_*_FOLDER` variables for each output component

**Downloader-Specific**:

- `DOWNLOADER_MAX_NODES`, `DOWNLOADER_WORKERS_PER_NODE`
- `DOWNLOADER_BATCH_SIZE`, `DOWNLOADER_RATE_MULTIPLIER`

**Tools-Specific**:

- `TOOLS_MAX_NODES`, `TOOLS_WORKERS_PER_NODE`
- `TOOLS_THRESHOLD_SIZE`, `TOOLS_NEW_RESIZE_SIZE`

## System Requirements

### Minimum Requirements

- Multi-node HPC cluster with Slurm
- High-bandwidth network connectivity
- Substantial storage capacity for downloaded datasets
- MPI-capable compute environment

### Recommended Configuration

- 20+ compute nodes with 20+ cores each
- High-speed interconnect (InfiniBand recommended)
- Parallel file system (Lustre, GPFS)
- Dedicated network bandwidth for external downloads

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Documentation

- [Process Overview](./docs/process_README.md) — High-level workflow description
- [Output Structure](./docs/structure_README.md) — Detailed output organization
- [Scripts Documentation](./docs/scripts_README.md) — Slurm script configuration
- [Troubleshooting Guide](./docs/troubleshooting_README.md) — Common issues and solutions

## Contributing

We welcome contributions! Please see our contributing guidelines and ensure all tests pass before submitting pull
requests.
