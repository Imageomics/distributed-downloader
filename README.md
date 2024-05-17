# Distributed Downloader
MPI-based distributed downloading tool for retrieving data from diverse domains.

## Background

This MPI-based distributed downloader was initially designed for the purpose of downloading all images from the monthly [GBIF occurence snapshot](https://www.gbif.org/occurrence-snapshots). The overall setup is general enough that it could be transformed into a functional tool beyond just our use; it should work on any list of URLs. We chose to build this tool instead of using something like [img2dataset](https://github.com/rom1504/img2dataset) to better avoid overloading source servers (GBIF documents approximately 200M images) and have more control over the final dataset construction and metadata management (e.g., using `HDF5` as discussed in [issue #1](https://github.com/Imageomics/distributed-downloader/issues/1)). 
