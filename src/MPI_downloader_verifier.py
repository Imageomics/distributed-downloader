import argparse
import os
import subprocess

import pandas

from mpi_downloader.utils import get_latest_schedule, verify_downloaded_batches

parser = argparse.ArgumentParser(description='Server downloader controller')

parser.add_argument('input_path', metavar='input_path', type=str, help='the path to folder of work')
parser.add_argument("schedule_path", metavar="schedule_path", type=str, help="the path to the schedule")

parser.add_argument('--recheck', action='store_true', help='recheck the downloaded batches')

# parse the arguments
_args = parser.parse_args()
Input_path: str = _args.input_path
Server_schedule: str = _args.schedule_path
config_file: str = f"{Server_schedule}/_config.csv"
verification_file: str = f"{Server_schedule}/_verification.csv"
Recheck = _args.recheck

Server_urls_downloaded = f"{Input_path}/downloaded_images"

if not os.path.exists(config_file):
    raise ValueError(f"Config file {config_file} not found")

if Recheck:
    Verification_df = pandas.DataFrame(columns=["ServerName", "PartitionId", "Status"])
else:
    if os.path.exists(verification_file):
        Verification_df = pandas.read_csv(verification_file)
    else:
        Verification_df = pandas.DataFrame(columns=["ServerName", "PartitionId", "Status"])

Verification_original_df = Verification_df.copy()

Server_profiler_df = pandas.read_csv(f"{Input_path}/servers_profiles.csv")

latest_schedule = get_latest_schedule(Server_schedule)
Server_config_df = pandas.read_csv(config_file)
Server_config_df["StartIndex"] = 0
Server_config_df["EndIndex"] = 0
server_config_columns = Server_config_df.columns.to_list()
Server_config_df = Server_config_df.merge(Server_profiler_df, left_on="ServerName", right_on="server_name", how="left")

Server_config_df["EndIndex"] = Server_config_df["total_batches"] - 1

Server_config_df = Server_config_df[server_config_columns]

Latest_schedule = get_latest_schedule(Server_schedule)
if Latest_schedule is not None and len(Latest_schedule) > 0:
    Latest_schedule_aggr = Latest_schedule.groupby("ServerName").agg(
        {"PartitionIdFrom": "min", "PartitionIdTo": "max"}).reset_index()
    Server_config_df = Server_config_df.merge(Latest_schedule_aggr, on="ServerName", how="left")
    Server_config_df["StartIndex"] = Server_config_df["PartitionIdFrom"]
    Server_config_df = Server_config_df[server_config_columns]

for idx, row in Server_config_df.iterrows():
    new_verification_df = verify_downloaded_batches(row, Server_urls_downloaded)
    Verification_df = pandas.concat([Verification_df, pandas.DataFrame(new_verification_df)], ignore_index=True).drop_duplicates()

Verification_df.to_csv(verification_file, index=False, header=True)

# verification_unchanged = Verification_df.equals(Verification_original_df)
# unchanged_warning = os.path.exists(f"{Server_schedule}/_UNCHANGED")
#
# if unchanged_warning:
#     if verification_unchanged:
#         os.remove(f"{Server_schedule}/_UNCHANGED")
#         raise ValueError("Infinite Loop")
#     else:
#         os.remove(f"{Server_schedule}/_UNCHANGED")
# else:
#     if verification_unchanged:
#         open(f"{Server_schedule}/_UNCHANGED", "w").close()


downloaded_count = Verification_df.groupby("ServerName").agg({"Status": "count"}).reset_index()
downloaded_count = downloaded_count.rename(columns={"Status": "Downloaded"})
downloaded_count = downloaded_count.merge(Server_config_df, on="ServerName", how="outer")
downloaded_count["Downloaded"] = downloaded_count["Downloaded"].fillna(0)
downloaded_count = downloaded_count[["ServerName", "Downloaded"]]
downloaded_count = downloaded_count.merge(Server_profiler_df, left_on="ServerName", right_on="server_name", how="left")
downloaded_count = downloaded_count[["ServerName", "total_batches", "Downloaded"]]
downloaded_count = downloaded_count[downloaded_count["Downloaded"] < downloaded_count["total_batches"]]

if len(downloaded_count) > 0:
    print(f"Need more jobs")
else:
    print("All servers have downloaded all the batches")
    open(f"{Server_schedule}/_DONE", "w").close()
