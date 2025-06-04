# Troubleshooting Guide

This guide helps diagnose and resolve common issues encountered during the distributed downloader operation. Most
problems occur during the download process and can be identified through log analysis.

## General Troubleshooting Approach

The distributed downloader is designed to be robust, with most issues arising from external factors such as server
connectivity or configuration problems. To ensure proper operation:

1. **Check the logs** of the currently running download batch
2. **Review error codes** in the output parquet files
3. **Verify configuration** parameters are appropriate for your environment
4. **Monitor system resources** (memory, network, storage)

## Error Code Reference

The downloader uses specific error codes to categorize different types of failures:

### Internal Errors

- **-2**: Internal processing error (not related to network requests)
    - **Common causes**: Image conversion failures, invalid image formats, HTML pages served instead of images
    - **OpenCV read failures**: Occurs when the URL points to non-image content
    - **Resolution**: Check URL validity and server response content type

### Network Errors

- **-1**: Network request error
    - **Common causes**: Broken URLs, server downtime, network connectivity issues
    - **DNS resolution failures**: Server cannot be reached
    - **Connection timeouts**: Network or server response delays
    - **Resolution**: Verify URL accessibility and network connectivity

### HTTP Status Codes

All values ≥ 0 represent standard HTTP status codes. Focus on these common problematic codes:

#### 403 Forbidden

- **Meaning**: Server denies access to the requested resource
- **Common causes**:
    - Rate limiting or IP blocking
    - Authentication requirements
    - Geographic restrictions
- **Resolution**:
    - Reduce request frequency in configuration
    - Check if server requires authentication
    - Consider using different IP ranges or proxy servers

#### 429 Too Many Requests

- **Meaning**: Server is rate limiting the client
- **Common causes**:
    - Request rate exceeds server limits
    - Insufficient delays between requests
- **Resolution**:
    - Reduce `default_rate_limit` in configuration
    - Increase `rate_multiplier` to add delays between requests
    - Restart the downloader for changes to take effect

#### Other Common HTTP Codes

- **404 Not Found**: URL no longer exists or was moved
- **500 Internal Server Error**: Server-side problems
- **503 Service Unavailable**: Server temporarily unavailable

## Configuration Adjustments

### Rate Limiting Resolution

When encountering rate limiting errors (403, 429):

1. **Modify configuration file**:
   
   ```yaml
   downloader_parameters:
     default_rate_limit: 1  # Reduce from default value
     rate_multiplier: 1.5   # Increase delay between requests
   ```

2. **Restart the download process** completely for changes to take effect

3. **Monitor error rates** in subsequent batches

### Resource Optimization

If experiencing memory or performance issues:

1. **Reduce batch size**:
   
   ```yaml
   downloader_parameters:
     batch_size: 5000  # Reduce from default 10000
   ```

2. **Adjust worker allocation**:
   
   ```yaml
   downloader_parameters:
     workers_per_node: 10  # Reduce from default 20
     max_nodes: 10         # Reduce if needed
   ```

## Log Analysis

### Download Logs Location

Logs are stored in the configured `logs_folder` under:

```
logs/
├── current/
│   └── 0000/
│       └── 0000/
│           ├── MPI_downloader_verifier.log
│           ├── MPI_multimedia_downloader.log
│           └── MPI_multimedia_downloader-{worker_id}.log
```

### Key Log Files

1. **MPI_multimedia_downloader-{worker_id}.log**:
    - Contains detailed download progress and errors
    - Shows individual image processing results
    - Includes timing information and performance metrics

2. **MPI_downloader_verifier.log**:
    - Shows batch completion verification
    - Lists any missing or failed downloads
    - Indicates whether batches need reprocessing

3. **initialization.log**:
    - Contains information about data partitioning
    - Shows any issues with input file processing

### Log Analysis Tips

- **Search for ERROR or WARNING**: `grep -i "error\|warning" *.log`
- **Count error types**: `grep "error_code" *.log | sort | uniq -c`
- **Monitor progress**: Check timestamps in logs to ensure processing continues
- **Verify completion**: Look for "completed" messages in verifier logs

## Common Issues and Solutions

### 1. Download Stops or Hangs

**Symptoms**: No new log entries, processes appear frozen

**Possible Causes**:

- Network connectivity issues
- Slurm job time limits exceeded
- Memory exhaustion

**Solutions**:

- Check Slurm job status: `squeue -u $USER`
- Review system resource usage
- Restart the download process (it will resume from checkpoint)

### 2. High Error Rates

**Symptoms**: Many entries in errors.parquet files

**Investigation Steps**:

1. Analyze error code distribution
2. Check if errors are concentrated on specific servers
3. Review server response patterns

**Solutions**:

- Add problematic servers to ignored_table
- Adjust rate limiting parameters
- Check URL validity in source data

### 3. Slow Download Performance

**Symptoms**: Downloads taking much longer than expected

**Possible Causes**:

- Conservative rate limiting settings
- Network bottlenecks
- Server response delays

**Solutions**:

- Monitor network utilization
- Adjust worker allocation
- Consider server-specific rate limits

### 4. Storage Issues

**Symptoms**: Jobs failing with disk space errors

**Solutions**:

- Monitor available disk space
- Clean up unnecessary log files
- Consider compressing completed data

## Recovery Procedures

### Resuming Failed Downloads

The downloader automatically resumes from checkpoints:

1. **Run the same command** that started the initial download
2. **The system will**:
    - Read the checkpoint file
    - Identify completed batches
    - Resume processing from the last incomplete batch

### Manual Intervention

If automatic recovery fails:

1. **Check checkpoint file** (`inner_checkpoint_file` in config)
2. **Review verification logs** to identify completed batches
3. **Consider resetting specific stages**:
    - `--reset_profiled`: Restart from profiling step
    - `--reset_batched`: Restart from initialization

### Data Validation

After completing downloads:

1. **Check success/error ratios** in parquet files
2. **Verify image data integrity** using tools pipeline
3. **Review log summaries** for any systemic issues

## Getting Help

When reporting issues, include:

1. **Configuration file** (with sensitive information removed)
2. **Relevant log excerpts** showing errors
3. **Error code distributions** from output files
4. **System environment** details (OS, MPI version, Python version)
5. **Steps to reproduce** the issue

This information helps diagnose problems quickly and provide targeted solutions.
