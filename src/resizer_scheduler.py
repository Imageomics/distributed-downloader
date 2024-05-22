import os
import re

import pandas

# schedule_path = "/users/PAS2119/andreykopanev/gbif/data/schedule_full.csv"
# base_path = "/fs/scratch/PAS2136/gbif/processed/2024-05-01/multimedia_prep/downloaded_images"
# number_of_nodes = 20
# number_of_workers = 5
schedule_path = "/mnt/d/Projects/Python/ImUn/gbif/schedule_full.csv"
base_path = "/mnt/d/Projects/Python/ImUn/gbif/downloaded_images"
number_of_nodes = 4
number_of_workers = 1

number_of_ranks = number_of_nodes * number_of_workers


def concat_ids(partition: pandas.DataFrame) -> pandas.DataFrame:
    ids = partition["Id"].str.cat(sep=" ")
    rank = int(partition["Rank"].iloc[0])
    server = partition["ServerName"].iloc[0]
    result = pandas.DataFrame([[rank, server, ids]], columns=["Rank", "ServerName", "Ids"])
    return result


all_schedules = []
corrupted_count = 0
not_that_corrupted = 0

for folder in os.listdir(base_path):
    server_name = folder.split("=")[1]
    for partition in os.listdir(f"{base_path}/{folder}"):
        partition_path = f"{base_path}/{folder}/{partition}"
        if os.path.exists(f"{partition_path}/_corrupted.txt"):
            with open(f"{partition_path}/_corrupted.txt", "r") as f:
                corrupted_text = f.read()
            if len(re.findall("\(.*,.*,.*\)", corrupted_text)) == 0:
                corrupted_count += 1
                print(f"{partition_path}: {corrupted_text}")
                continue
            else:
                # os.remove(f"{partition_path}/_corrupted.txt")
                not_that_corrupted += 1
        if (os.path.exists(f"{partition_path}/verification.parquet") or
                not os.path.exists(f"{partition_path}/successes.parquet") or
                not os.path.exists(f"{partition_path}/completed")):
            continue
        all_schedules.append([server_name, partition.split("=")[1]])

schedule_df = pandas.DataFrame(all_schedules, columns=["ServerName", "Id"])
print(schedule_df.count())
print(corrupted_count)
print(not_that_corrupted)
# schedule_df["Rank"] = schedule_df.index % number_of_ranks
# schedule_grouped = schedule_df.groupby(["Rank", "ServerName"])
#
# schedules = schedule_grouped.apply(concat_ids).reset_index(drop=True)
#
# schedules.to_csv(schedule_path, index=False, header=True)
