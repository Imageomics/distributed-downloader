# Distributed Downloader Process Overview

This document provides a high-level overview of how the distributed downloader operates, breaking down the process into
distinct steps and explaining the workflow architecture.

## Process Flow Summary

The distributed downloader follows a structured pipeline approach with four main stages:

1. **Initialization** — Data preparation and partitioning
2. **Profiling** — Server capability assessment
3. **Downloading** — Parallel image retrieval
4. **Verification** — Completion validation

## Detailed Process Steps

### Step 1: Initialization

**Purpose**: Transform and partition the input dataset into a format suitable for distributed downloading.

**Implementation**:

- Uses Apache Spark for scalable data processing
- Partitions input URLs by server name (extracted via `urlparse(url).netloc`)
- Groups URLs into batches of configurable size (`batch_size` parameter)
- Stores partitioned data as Parquet files for efficient access

**Output**: Structured URL batches organized by server in the `urls_folder`

### Step 2: Profiling

**Purpose**: Assess server capabilities and determine appropriate download rates.

**Current Implementation**:

- Executes "fake" profiling that assigns uniform download rates
- Creates a profile table with server information and rate limits
- Future versions may implement actual server response time testing

**Output**: Server profiles CSV file containing rate limiting information

### Step 3: Downloading

The downloading step consists of multiple sub-processes to ensure efficient and reliable parallel downloading.

#### Step 3.1: Big Schedule Creation

**Purpose**: Organize remaining downloads into manageable "big batches" for worker groups.

**Process**:

- Determines optimal batch sizes based on cluster configuration
- Calculates batch size as: `max_nodes × workers_per_node × 50`
- Ensures sufficient work distribution across all available workers
- Schedules worker groups for each big batch
- Includes verification jobs at the end of each batch

**Implementation Details**:

- Each big batch is designed to fully utilize cluster resources
- The multiplier (50) ensures workers have adequate workload
- Verification jobs ensure all downloads completed successfully

#### Step 3.2: Downloader Job Execution

Each downloader job consists of three sequential MPI tasks:

##### Step 3.2.1: Verification

**Purpose**: Determine which batches have already been completed.

**Process**:

- Scans batch directories for completion markers
- Identifies successfully downloaded batches (marked by `completed` file)
- Creates list of remaining work for the current execution

##### Step 3.2.2: Local Scheduling

**Purpose**: Create detailed work assignments for MPI processes.

**Process**:

- Assigns specific image batches to individual MPI processes
- Balances workload across available workers
- Creates process-specific download queues
- Ensures optimal resource utilization

##### Step 3.2.3: Parallel Downloading

**Purpose**: Execute actual image downloads across multiple workers.

**Process**:

- Each MPI process downloads its assigned image batches
- Implements rate limiting to respect server capabilities
- Handles errors and retries according to configuration
- Stores successful downloads and error information in Parquet format

#### Step 3.3: Final Verification

**Purpose**: Confirm all scheduled downloads completed successfully.

**Process**:

- Reviews completion status of all batches in the big batch
- Identifies any failed or incomplete downloads
- Updates verification logs with final status
- Prepares for next big batch if work remains

## Architecture Design Principles

### Fault Tolerance

- Checkpoint system allows resuming from any interruption
- Individual batch completion tracking prevents duplicate work
- Error logging enables identification and resolution of systemic issues

### Scalability

- MPI-based parallelism scales across cluster nodes
- Spark-based initialization handles large input datasets
- Modular design supports different cluster configurations

### Server-Friendly Operation

- Configurable rate limiting prevents server overload
- Server-specific profiling allows tailored request rates
- Ignored server list enables problematic server exclusion

### Resource Optimization

- Big batch system maximizes cluster utilization
- Dynamic scheduling adapts to varying batch sizes
- Memory-efficient Parquet storage format

## Configuration Impact

The process behavior is highly configurable through the YAML configuration file:

- **Batch sizing**: Controls memory usage and I/O patterns
- **Worker allocation**: Determines parallelism level
- **Rate limiting**: Balances speed with server respect
- **Resource limits**: Ensures cluster resource compliance

## Monitoring and Control

The process provides multiple monitoring mechanisms:

- **Log files**: Detailed operation tracking at each step
- **Checkpoint files**: Current progress state
- **Verification reports**: Completion status summaries
- **Error tracking**: Failed download categorization

This architecture ensures reliable, scalable, and respectful downloading of large image datasets while providing
comprehensive monitoring and recovery capabilities.
