import argparse
import os
import shutil

import h5py
import mpi4py.MPI as MPI
import pandas

from mpi_downloader.dataclasses import error_dtype, profile_dtype

_DEFAULT_RATE_LIMIT = 10

parser = argparse.ArgumentParser(description='Server profiler')

parser.add_argument('input_path', metavar='input_path', type=str, help='the path to folder of work')
parser.add_argument('max_nodes', metavar='max_nodes', type=int, help='the path to folder of work')
parser.add_argument('max_workers_per_nodes', metavar='max_workers_per_nodes', type=int,
                    help='the path to folder of work')
parser.add_argument('batch_size', metavar='batch_size', type=int, help='the path to folder of work')

# parse the arguments
_args = parser.parse_args()
Input_path: str = _args.input_path
Server_urls_batched = f"{Input_path}/servers_batched"
Server_profiler_hdf = f"{Input_path}/servers_profiles.hdf5"
Server_errors_hdf = f"{Input_path}/servers_errors.hdf5"
Server_profile_spec = f"{Input_path}/profile_spec.csv"
Server_profiler_csv = f"{Input_path}/servers_profiles.csv"
Server_samples = f"{Input_path}/samples"
Number_of_workers: int = _args.max_nodes * _args.max_workers_per_nodes
Batch_size: int = _args.batch_size

if os.path.exists(Server_samples) or os.path.isdir(Server_samples):
    shutil.rmtree(Server_samples)
os.makedirs(Server_samples)

server_list = os.listdir(Server_urls_batched)
server_count = len(server_list)
print("Counted all servers")

with h5py.File(Server_errors_hdf, 'w', driver='mpio', comm=MPI.COMM_WORLD) as errors_hdf:
    errors = errors_hdf.create_dataset("errors",
                                       (server_count * Batch_size,),
                                       chunks=(Batch_size,),
                                       dtype=error_dtype,
                                       )
with h5py.File(Server_profiler_hdf, 'w', driver='mpio', comm=MPI.COMM_WORLD) as profiles_hdf:
    profiles = profiles_hdf.create_dataset("profiles",
                                           (server_count,),
                                           dtype=profile_dtype,
                                           )
    profile_spec = []
    profile_csv = []
    for i, server in enumerate(server_list):
        if not os.path.isdir(f"{Server_urls_batched}/{server}"):
            continue

        server_name = server.split("=")[1]
        server_total_partitions = len(os.listdir(f"{Server_urls_batched}/{server}"))
        profile_spec.append([i % Number_of_workers, server_name, server_total_partitions])
        profile_csv.append([server_name, server_total_partitions, 0, 0, _DEFAULT_RATE_LIMIT])
        profiles[i] = (server_name, server_total_partitions, 0, 0, _DEFAULT_RATE_LIMIT)

print("created df")

profile_spec_df = pandas.DataFrame(profile_spec, columns=["Rank", "ServerName", "BatchesCount"])
profile_spec_df.to_csv(Server_profile_spec, index=True, index_label="Offset", header=True)

pandas.DataFrame(profile_csv, columns=profile_dtype.names).to_csv(Server_profiler_csv, index=False, header=True)
print("wrote csv to disk")

print("Profiling started")

# for folder in os.listdir(Input_path):
#     for file in os.listdir(f"{Input_path}/{folder}"):
#         success_len += Batch_size
#         error_len += Batch_size

# successes = f.create_dataset("successes",
#                               (success_len,),
#                               maxshape=(success_len,),
#                               dtype=success_dtype,
#                               chunks=(Batch_size,),
#                               # compression="gzip"
#                               )

# errors = f.create_dataset("errors",
#                            (error_len,),
#                            maxshape=(error_len,),
#                            dtype=error_dtype,
#                            chunks=(Batch_size,),
#                            # compression="gzip"
#                            )

# images = f.create_dataset("images",
#                            (success_len, 1024, 1024, 3),
#                            maxshape=(success_len, 1024, 1024, 3),
#                            dtype='uint8',
#                            chunks=(Batch_size, 1024, 1024, 3),
#
#                            # compression='gzip',
#                            **hdf5plugin.LZ4()
#                            )
