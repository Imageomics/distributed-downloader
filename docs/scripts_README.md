# Distributed Downloader - Scripts Documentation

This document provides detailed explanations of the scripts in the `scripts` folder of the distributed-downloader
package. These scripts are used to submit jobs to Slurm and execute various tasks of the distributed downloader.

## Submission Scripts

## general_submitter (general_submit.sh)

### Purpose

`general_submit.sh` is designed to submit general batch jobs to Slurm without specifying node count or task
distribution. It's primarily used for initialization tasks, configuration setup, or any job that manages its own
resource allocation.

### Usage

```bash
./general_submit.sh script1 [dependencies...]
```

### Arguments

1. `script1`: The script file to submit to Slurm
2. `dependencies...`: (Optional) One or more Slurm job IDs that this job depends on

### Features

- Sets up the repository root environment variable (`REPO_ROOT`)
- Creates the logs directory automatically
- Handles job dependencies (if provided)
- Uses the `afterok` dependency type, which only runs if the dependency completed successfully
- Submits the script to Slurm with appropriate output and error file paths

### Environment Variables Used

- `OUTPUT_LOGS_FOLDER`: Directory to store log files
- `ACCOUNT`: Slurm account to charge the job to

### Example

```bash
# Submit a script with no dependencies
./general_submit.sh /path/to/script.sh

# Submit a script that depends on job 12345 completing successfully
./general_submit.sh /path/to/script.sh 12345

# Submit a script that depends on multiple jobs completing successfully
./general_submit.sh /path/to/script.sh 12345 67890
```

### Implementation Details

- When no dependencies are provided, the script is submitted directly without dependency constraints
- When dependencies are provided, the `--dependency=afterok:` flag is added with the job IDs
- The script creates output and error files named after the base name of the submitted script
- The repository root is automatically determined and exported as an environment variable

### Notes

- Unlike other submission scripts, this one doesn't specify resource requirements like node count or CPUs per task
- This makes it suitable for scripts that handle their own resource allocation or have minimal resource needs
- The `afterok` dependency ensures the job only runs if all dependencies completed successfully (unlike `afterany` which
  runs regardless of the exit status of the dependencies)

## schedule_creator_submitter (submit_schedule_creator.sh)

### Purpose

`submit_schedule_creator.sh` is specifically designed for submitting the schedule creation job to Slurm. While similar
to `general_submit.sh`, it explicitly configures the resource allocation to match the downloader's requirements and adds
additional environment variables needed for schedule creation.

### Usage

```bash
./submit_schedule_creator.sh script1 [dependencies...]
```

### Arguments

1. `script1`: The script file to submit to Slurm (typically schedule_creation.slurm)
2. `dependencies...`: (Optional) One or more Slurm job IDs that this job depends on

### Features

- Sets up the repository root environment variable (`REPO_ROOT`)
- Creates the logs directory automatically
- Handles job dependencies (if provided)
- Uses the `afterany` dependency type, which runs after the dependencies complete regardless of their exit status
- Submits the script to Slurm with appropriate node count, tasks per node, and CPUs per task
- Exports the `LOGS_BASE_FILENAME` environment variable for use by the schedule creation script

### Environment Variables Used

- `OUTPUT_LOGS_FOLDER`: Directory to store log files
- `DOWNLOADER_MAX_NODES`: Number of nodes to allocate
- `DOWNLOADER_WORKERS_PER_NODE`: Number of tasks per node
- `DOWNLOADER_CPU_PER_WORKER`: Number of CPUs per task
- `ACCOUNT`: Slurm account to charge the job to

### Implementation Details

- Unlike `general_submit.sh`, this script explicitly specifies resource allocation parameters from the downloader
  configuration
- It uses `--nodes`, `--ntasks-per-node`, and `--cpus-per-task` flags to match the downloader's requirements
- It exports the script's base filename as `LOGS_BASE_FILENAME` which is needed by the schedule creation script
- It uses `afterany` dependency type instead of `afterok` used by `general_submit.sh`

### Notes

- This script was created due to specific requirements on the Ohio Supercomputer Center (OSC) where the number of nodes
  must match when launching new jobs from within a job
- It's specifically optimized for the schedule creation workflow, which may need to spawn additional jobs
- Unlike `general_submit.sh`, this script always specifies resource requirements to ensure proper operation of the
  schedule creation process

## mpi_submitter (submit_mpi_download.sh)

### Purpose

`submit_mpi_download.sh` is specifically designed for submitting the MPI-based downloader jobs to Slurm. It handles the
special requirements of the downloader process, including passing schedule identifiers and iteration numbers to the
underlying scripts.

### Usage

```bash
./submit_mpi_download.sh script schedule# iteration_number [dependency] [--recheck]
```

### Arguments

1. `script`: The script file to submit to Slurm
2. `schedule#`: The general schedule identifier to process
3. `iteration_number`: The iteration number for the current download batch
4. `dependency`: (Optional) The job ID that this job depends on
5. `--recheck`: (Optional) Flag indicating this is a recheck job (for server_verifying.slurm)

### Features

- Sets up the repository root environment variable (`REPO_ROOT`)
- Creates a nested log directory structure based on schedule# and iteration_number
- Handles job dependencies (if provided)
- Passes all arguments through to the submitted script
- Uses the `afterany` dependency type, which runs after dependencies complete regardless of exit status
- Sets up appropriate node allocation based on downloader configuration
- Handles the special `--recheck` flag for `server_verifying.slurm` jobs

### Environment Variables Used

- `OUTPUT_LOGS_FOLDER`: Base directory for storing log files
- `DOWNLOADER_MAX_NODES`: Number of nodes to allocate
- `DOWNLOADER_WORKERS_PER_NODE`: Number of tasks per node
- `DOWNLOADER_CPU_PER_WORKER`: Number of CPUs per task
- `ACCOUNT`: Slurm account to charge the job to

### Example

```bash
# Submit a download job for schedule 3, iteration 1, with no dependency
./submit_mpi_download.sh /path/to/downloader.slurm 3 1

# Submit a download job for schedule 3, iteration 1, that depends on job 12345
./submit_mpi_download.sh /path/to/downloader.slurm 3 1 12345

# Submit a recheck job for schedule 3, iteration 1
./submit_mpi_download.sh /path/to/downloader.slurm 3 1 --recheck
```

### Implementation Details

- The script creates a nested directory structure for logs: `logs_dir/current/{schedule}/{iteration_number}`
- It handles both dependency-based submissions and direct submissions with a conditional
- The dependency flag is set to `afterany` meaning it will run after the dependent job completes, regardless of its exit
  status
- It passes all the remaining arguments directly to the submitted script
- The `--recheck` flag is treated specially, not as a dependency ID

### Notes

- This script is specifically intended for the MPI downloader jobs, not for general purpose submissions
- It configures resource allocation according to the downloader's requirements
- The nested log directory structure helps organize output by schedule and iteration number
- Unlike `general_submit.sh`, this script expects specific arguments for the downloader process

## tools_submitter (tools_submit.sh)

### Purpose

`tools_submit.sh` is designed for submitting tool-related jobs to Slurm with specific resource requirements for tooling
operations. It supports both regular and Spark-based tool submissions and handles job dependencies.

### Usage

```bash
./tools_submit.sh script tool_name [dependency] [--spark]
```

### Arguments

1. `script`: The script file to submit to Slurm
2. `tool_name`: The name of the tool to be run
3. `dependency`: (Optional) The job ID that this job depends on
4. `--spark`: (Optional) Flag indicating this is a Spark-based job

### Features

- Sets up the repository root environment variable (`REPO_ROOT`)
- Creates the logs directory automatically
- Handles job dependencies (if provided)
- Special handling for Spark jobs, which have different resource requirements
- For non-Spark jobs, applies tool-specific resource configurations

### Environment Variables Used

- `OUTPUT_TOOLS_LOGS_FOLDER`: Directory to store tool log files
- `TOOLS_MAX_NODES`: Maximum number of nodes for tools
- `TOOLS_WORKERS_PER_NODE`: Number of tool workers per node
- `TOOLS_CPU_PER_WORKER`: Number of CPUs per tool worker
- `ACCOUNT`: Slurm account to charge the job to

### Example

```bash
# Submit a regular tool job
./tools_submit.sh /path/to/tool_script.slurm resize

# Submit a tool job with a dependency
./tools_submit.sh /path/to/tool_script.slurm resize 12345

# Submit a Spark-based tool job
./tools_submit.sh /path/to/spark_tool_script.slurm resize --spark

# Submit a Spark-based tool job with a dependency
./tools_submit.sh /path/to/spark_tool_script.slurm resize 12345 --spark
```

## Distributed Downloader Slurm Script Architecture

The distributed-downloader package uses several Slurm scripts to handle different aspects of the download process. These
scripts are environment-specific and are currently configured for the Ohio Supercomputer Center (OSC).

> [!IMPORTANT]
>
> ### Job Output Format Requirement
>
> All Slurm scripts must output the job ID of the submitted job in a specific format. The job ID must be the last item
> on the line and separated by a space:
>
> ```
> {anything} {id}
> ```
>
> For example:
>
> ```
> Submitted batch job 12345
> ```
>
> This format is essential as the submission scripts parse this output to extract the job ID for dependency tracking.

### initialization_script (initialization.slurm)

**Purpose**: Sets up the initial environment and partitions the input file using Spark.

**Key Components**:

- Uses Spark's distributed processing to handle large input files
- Calls `core/initialization.py` which partitions the input data
- Typical run time is 30 minutes
- Requires 4 nodes for optimal performance

### profiling_script (profiling.slurm)

**Purpose**: Profiles the servers to determine download rates and capabilities.

**Key Components**:

- Runs on a single node with 3 CPUs
- Calls `core/fake_profiler.py` which tests servers and creates a profile table
- Typical run time is 5 minutes
- Creates a server profiles CSV file

### schedule_creation_script (schedule_creation.slurm)

**Purpose**: Creates download schedules based on server profiles.

**Key Components**:

- Runs on configured number of nodes
- Calls `core/MPI_download_prep.py` which creates schedules for downloads
- Typical run time is 5 minutes
- Moves logs to a structured directory after completion

### download_script (server_downloading.slurm)

**Purpose**: Executes the actual downloading process using MPI.

**Key Components**:

- Runs three sequential MPI tasks:
    1. `core/MPI_downloader_verifier.py` to verify the schedule
    2. `core/MPI_multimedia_downloader_controller.py` to set up the schedules for the next downloader run
    3. `core/MPI_multimedia_downloader.py` to perform downloads in parallel
- Uses all allocated nodes for maximum parallelism
- Configures memory settings for optimal performance
- Typical run time is 3 hours

### verify_script (server_verifying.slurm)

**Purpose**: Verifies the completion status of downloads.

**Key Components**:

- Runs on a single node
- Calls `core/MPI_downloader_verifier.py` to check completion status
- Typical run time is 2 minutes
- Creates verification logs for each schedule
- Haves a special `--recheck` flag to indicate rechecking of completed batches

### Environment Notes

These scripts are specifically configured for the OSC environment, which uses:

- Intel MPI 2021.10
- Miniconda3 23.3.1 with Python 3.10
- IntelMPI job placement settings

When adapting for other environments, you'll need to modify:

- Module load commands
- MPI implementation settings
- Memory allocation parameters
- Time limits

The scripts assume the existence of a virtual environment at `${REPO_ROOT}/.venv/`.

## Tools Slurm Script Architecture

The distributed-downloader package includes several specialized Slurm scripts to handle different aspects of the tools
pipeline. These scripts are designed to work with the tools framework which processes downloaded images in various ways.

> [!IMPORTANT]
>
> ### Job Output Format Requirement
>
> Like all other scripts in the system, the tools scripts must output the job ID of any submitted jobs in the specific
> format with the ID as the last item on a line separated by a space:
>
> ```
> {anything} {id}
> ```
>
> This format is essential for job dependency tracking.

### tools_filter_script (tools_filter.slurm)

**Purpose**: Performs the first step in the tool pipeline, filtering images based on specific criteria.

**Key Components**:

- Uses Spark for distributed processing of large dataset files
- Calls `tools/filter.py` with the specified tool name
- Creates CSV files containing references to images that match the filtering criteria
- Typical run time is 1 hour
- Requires significant memory for driver (110GB) and executors (64GB)

**Example**:
For a size-based filter tool, this script would identify all images smaller than a threshold size and write their UUIDs,
server names, and partition IDs to CSV files.

### tools_scheduling_script (tools_scheduler.slurm)

**Purpose**: Creates execution schedules for the tool workers based on filtered data.

**Key Components**:

- Runs on a single node
- Calls `tools/scheduler.py` with the specified tool name
- Processes the CSV files produced by the filter step
- Assigns images to different worker processes to balance the load
- Typical run time is 5 minutes
- Creates schedule files that map partitions to worker ranks

**Example**:
For a size-based filter tool, the scheduler might group images by server name and partition ID (which corresponds to a
single parquet file) and assign these groups to different MPI ranks (e.g., worker 1 processes partitions 1,2,3,4).

### tools_worker_script (tools_worker.slurm)

**Purpose**: Executes the actual tool processing using MPI parallelism.

**Key Components**:

- Runs on the configured number of nodes with specified worker distribution
- Calls `tools/runner.py` with the specified tool name
- Reads the schedule created by the scheduler and processes assigned partitions
- Uses all allocated nodes for maximum parallelism
- Configures memory settings for optimal performance
- Typical run time is 3 hours
- Creates output files specific to the tool (e.g., resized images)

**Example**:
For an image resizing tool, each worker would load the images assigned to it from the schedule, resize them to the
specified dimensions, and save the results to the output location.

### tools_verification_script (tools_verifier.slurm)

**Purpose**: Verifies the completion of the tool processing and updates status flags.

**Key Components**:

- Runs on a single node
- Calls `tools/verification.py` with the specified tool name
- Checks if all scheduled tasks have been completed
- Updates the completion status in the tool's checkpoint file
- Typical run time is 5 minutes
- Sets the "completed" flag when all processing is done

**Example**:
For any tool, the verifier checks if all scheduled tasks have been processed successfully and marks the overall
operation as complete when verified.

### Environment Configuration

All tools scripts share similar environment configuration:

- They rely on Intel MPI 2021.10 for communication
- They use Miniconda3 23.3.1 with Python 3.10
- They set PYTHONPATH to include the repository source directories
- They disable the Intel MPI job placement restriction with `I_MPI_JOB_RESPECT_PROCESS_PLACEMENT=0`

### Unique Aspects

- **tools_filter.slurm**: Uses Spark instead of MPI for distributed processing
- **tools_worker.slurm**: Uses the full configured node count and workers per node
- **tools_scheduler.slurm** and **tools_verifier.slurm**: Run on a single node as they perform organizational tasks

### Resource Management

The tools scripts use environment variables to determine resource allocation:

- `TOOLS_MAX_NODES`: Number of nodes for tool workers
- `TOOLS_WORKERS_PER_NODE`: Number of tool workers per node
- `TOOLS_CPU_PER_WORKER`: Number of CPUs per tool worker

These values are set from the configuration file and passed through the environment by the submission scripts.
