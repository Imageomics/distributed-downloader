# Distributed Downloader - Scripts Documentation

This document provides comprehensive explanations of the scripts in the `scripts` folder of the distributed-downloader
package. These scripts handle job submission to Slurm and execute various tasks within the distributed downloader
pipeline.

## Submission Scripts Overview

All submission scripts follow a consistent pattern for job dependency management and environment setup. They create
appropriate log directories, handle Slurm job dependencies, and export necessary environment variables for the submitted
jobs.

## Submission Script Details

### general_submitter (general_submit.sh)

**Purpose**: Submits general batch jobs to Slurm without specifying node count or task distribution, primarily used for
initialization tasks, configuration setup, or jobs that manage their own resource allocation.

**Usage**:

```bash
./general_submit.sh script1 [dependencies...]
```

**Arguments**:

1. `script1`: The script file to submit to Slurm
2. `dependencies...`: (Optional) One or more Slurm job IDs that this job depends on

**Key Features**:

- Sets up the repository root environment variable (`REPO_ROOT`)
- Creates the logs directory automatically
- Handles job dependencies using the `afterok` dependency type
- Submits jobs with appropriate output and error file paths

**Environment Variables**:

- `OUTPUT_LOGS_FOLDER`: Directory for log file storage
- `ACCOUNT`: Slurm account for job charging

**Examples**:

```bash
# Submit a script with no dependencies
./general_submit.sh /path/to/script.sh

# Submit a script dependent on job 12345
./general_submit.sh /path/to/script.sh 12345

# Submit a script dependent on multiple jobs
./general_submit.sh /path/to/script.sh 12345 67890
```

**Implementation Notes**:

- Uses `afterok` dependency, ensuring jobs only run if dependencies complete successfully
- Automatically determines and exports repository root as an environment variable
- Creates output and error files named after the submitted script's base name

### schedule_creator_submitter (submit_schedule_creator.sh)

**Purpose**: Submits the schedule creation job to Slurm with explicit resource configuration matching the downloader's
requirements and additional environment variables for schedule creation.

**Usage**:

```bash
./submit_schedule_creator.sh script1 [dependencies...]
```

**Arguments**:

1. `script1`: The script file to submit (typically schedule_creation.slurm)
2. `dependencies...`: (Optional) One or more Slurm job IDs that this job depends on

**Key Features**:

- Configures explicit resource allocation parameters
- Uses `afterany` dependency type (runs regardless of dependency exit status)
- Exports `LOGS_BASE_FILENAME` environment variable for schedule creation scripts

**Environment Variables**:

- `OUTPUT_LOGS_FOLDER`: Directory for log file storage
- `DOWNLOADER_MAX_NODES`: Number of nodes to allocate
- `DOWNLOADER_WORKERS_PER_NODE`: Number of tasks per node
- `DOWNLOADER_CPU_PER_WORKER`: Number of CPUs per task
- `ACCOUNT`: Slurm account for job charging

**Implementation Notes**:

- Explicitly specifies `--nodes`, `--ntasks-per-node`, and `--cpus-per-task` flags
- Required for environments like Ohio Supercomputer Center (OSC) where node count must match when launching jobs from
  within a job
- Optimized specifically for the schedule creation workflow

### mpi_submitter (submit_mpi_download.sh)

**Purpose**: Submits MPI-based downloader jobs to Slurm, handling special requirements including schedule identifiers
and iteration numbers.

**Usage**:

```bash
./submit_mpi_download.sh script schedule# iteration_number [dependency] [--recheck]
```

**Arguments**:

1. `script`: The script file to submit to Slurm
2. `schedule#`: The general schedule identifier to process
3. `iteration_number`: The iteration number for the current download batch
4. `dependency`: (Optional) The job ID that this job depends on
5. `--recheck`: (Optional) Flag for recheck jobs (server_verifying.slurm)

**Key Features**:

- Creates nested log directory structure based on schedule and iteration
- Handles the special `--recheck` flag for verification jobs
- Uses `afterany` dependency type
- Configures node allocation based on downloader configuration

**Environment Variables**:

- `OUTPUT_LOGS_FOLDER`: Base directory for log file storage
- `DOWNLOADER_MAX_NODES`: Number of nodes to allocate
- `DOWNLOADER_WORKERS_PER_NODE`: Number of tasks per node
- `DOWNLOADER_CPU_PER_WORKER`: Number of CPUs per task
- `ACCOUNT`: Slurm account for job charging

**Examples**:

```bash
# Submit download job for schedule 3, iteration 1
./submit_mpi_download.sh /path/to/downloader.slurm 3 1

# Submit with dependency on job 12345
./submit_mpi_download.sh /path/to/downloader.slurm 3 1 12345

# Submit recheck job
./submit_mpi_download.sh /path/to/downloader.slurm 3 1 --recheck
```

**Implementation Notes**:

- Creates nested log directory: `logs_dir/current/{schedule}/{iteration_number}`
- Passes all arguments directly to the submitted script
- Specifically designed for MPI downloader jobs with their unique requirements

### tools_submitter (tools_submit.sh)

**Purpose**: Submits tool-related jobs to Slurm with specific resource requirements for tooling operations, supporting
both regular and Spark-based tools.

**Usage**:

```bash
./tools_submit.sh script tool_name [dependency] [--spark]
```

**Arguments**:

1. `script`: The script file to submit to Slurm
2. `tool_name`: The name of the tool to be executed
3. `dependency`: (Optional) The job ID that this job depends on
4. `--spark`: (Optional) Flag indicating a Spark-based job

**Key Features**:

- Special handling for Spark jobs with different resource requirements
- Tool-specific resource configurations for non-Spark jobs
- Automatic log directory creation

**Environment Variables**:

- `OUTPUT_TOOLS_LOGS_FOLDER`: Directory for tool log files
- `TOOLS_MAX_NODES`: Maximum number of nodes for tools
- `TOOLS_WORKERS_PER_NODE`: Number of tool workers per node
- `TOOLS_CPU_PER_WORKER`: Number of CPUs per tool worker
- `ACCOUNT`: Slurm account for job charging

**Examples**:

```bash
# Submit regular tool job
./tools_submit.sh /path/to/tool_script.slurm resize

# Submit with dependency
./tools_submit.sh /path/to/tool_script.slurm resize 12345

# Submit Spark-based tool job
./tools_submit.sh /path/to/spark_tool_script.slurm resize --spark
```

## Slurm Script Architecture

The distributed-downloader package uses several Slurm scripts to handle different aspects of the download process. These
scripts are environment-specific and currently configured for the Ohio Supercomputer Center (OSC).

> [!IMPORTANT]
> **Job Output Format Requirement**
>
> All Slurm scripts must output the job ID in a specific format with the ID as the last item on a line, separated by a
> space:
>
> ```
> {anything} {id}
> ```
>
> Example: `Submitted batch job 12345`
>
> This format is essential for submission scripts to parse job IDs for dependency tracking.

### Core Pipeline Scripts

#### initialization_script (initialization.slurm)

**Purpose**: Sets up the initial environment and partitions the input file using Spark.

**Key Components**:

- Uses Spark's distributed processing for large input files
- Calls `core/initialization.py` for data partitioning
- Typical runtime: 30 minutes
- Requires: 4 nodes for optimal performance

#### profiling_script (profiling.slurm)

**Purpose**: Profiles servers to determine download rates and capabilities.

**Key Components**:

- Single node execution with 3 CPUs
- Calls `core/fake_profiler.py` for server testing and profile creation
- Typical runtime: 5 minutes
- Output: Server profiles CSV file

#### schedule_creation_script (schedule_creation.slurm)

**Purpose**: Creates download schedules based on server profiles.

**Key Components**:

- Runs on configured number of nodes
- Calls `core/MPI_download_prep.py` for schedule creation
- Typical runtime: 5 minutes
- Post-execution: Moves logs to structured directory

#### download_script (server_downloading.slurm)

**Purpose**: Executes the actual downloading process using MPI.

**Key Components**:

- Sequential execution of three MPI tasks:
  1. `core/MPI_downloader_verifier.py` — Schedule verification
  2. `core/MPI_multimedia_downloader_controller.py` — Next run schedule setup
  3. `core/MPI_multimedia_downloader.py` — Parallel download execution
- Uses all allocated nodes for maximum parallelism
- Optimized memory settings
- Typical runtime: 3 hours

#### verify_script (server_verifying.slurm)

**Purpose**: Verifies download completion status.

**Key Components**:

- Single node execution
- Calls `core/MPI_downloader_verifier.py` for completion verification
- Typical runtime: 2 minutes
- Special `--recheck` flag for batch rechecking
- Output: Verification logs for each schedule

### Tools Pipeline Scripts

The tools pipeline includes specialized scripts for processing downloaded images through various operations.

> [!IMPORTANT]
> **Job Output Format Requirement**
>
> Tools scripts must also follow the job ID output format for proper dependency tracking.

#### tools_filter_script (tools_filter.slurm)

**Purpose**: Performs initial filtering of images based on specific criteria.

**Key Components**:

- Uses Spark for distributed processing of large datasets
- Calls `tools/filter.py` with specified tool name
- Creates CSV files with filtered image references
- Typical runtime: 1 hour
- Memory requirements: 110GB driver, 64GB executors

**Example**: For size-based filtering, identifies images below threshold size and writes UUIDs, server names, and
partition IDs to CSV files.

#### tools_scheduling_script (tools_scheduler.slurm)

**Purpose**: Creates execution schedules for tool workers based on filtered data.

**Key Components**:

- Single node execution
- Calls `tools/scheduler.py` with specified tool name
- Processes CSV files from filter step
- Load balancing through worker assignment
- Typical runtime: 5 minutes
- Output: Schedule files mapping partitions to worker ranks

**Example**: Groups images by server name and partition ID, assigning groups to different MPI ranks for balanced
processing.

#### tools_worker_script (tools_worker.slurm)

**Purpose**: Executes actual tool processing using MPI parallelism.

**Key Components**:

- Runs on configured number of nodes with specified worker distribution
- Calls `tools/runner.py` with specified tool name
- Reads scheduler-created schedules and processes assigned partitions
- Optimized memory settings and maximum parallelism
- Typical runtime: 3 hours
- Output: Tool-specific results (e.g., resized images)

**Example**: For image resizing, each worker loads assigned images, resizes them to specified dimensions, and saves
results.

#### tools_verification_script (tools_verifier.slurm)

**Purpose**: Verifies tool processing completion and updates status flags.

**Key Components**:

- Single node execution
- Calls `tools/verification.py` with specified tool name
- Checks completion status of all scheduled tasks
- Updates checkpoint file completion status
- Typical runtime: 5 minutes
- Sets "completed" flag when all processing finishes

### Environment Configuration

All scripts share common environment configuration:

- Intel MPI 2021.10 for communication
- Miniconda3 23.3.1 with Python 3.10
- PYTHONPATH includes repository source directories
- Intel MPI job placement restriction disabled: `I_MPI_JOB_RESPECT_PROCESS_PLACEMENT=0`

### Resource Management

Scripts use environment variables for resource allocation:

- **Downloader scripts**: `DOWNLOADER_MAX_NODES`, `DOWNLOADER_WORKERS_PER_NODE`, `DOWNLOADER_CPU_PER_WORKER`
- **Tools scripts**: `TOOLS_MAX_NODES`, `TOOLS_WORKERS_PER_NODE`, `TOOLS_CPU_PER_WORKER`

### Architecture Distinctions

**Downloader Scripts**:

- Focus on downloading and verification workflows
- Use `afterok` and `afterany` dependencies appropriately
- Handle complex scheduling and verification logic

**Tools Scripts**:

- **tools_filter.slurm**: Uses Spark instead of MPI
- **tools_worker.slurm**: Uses full configured resources
- **tools_scheduler.slurm** and **tools_verifier.slurm**: Single node organizational tasks

### Adaptation Notes

When adapting for different environments, modify:

- Module load commands
- MPI implementation settings
- Memory allocation parameters
- Time limits
- Virtual environment paths (currently assumes `${REPO_ROOT}/.venv/`)
