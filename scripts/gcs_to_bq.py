import os
import sys

import pandas as pd
import pandas_gbq
from dotenv import load_dotenv
from prefect import flow, task
from prefect_dbt import DbtCoreOperation
from prefect_gcp import GcpCredentials
from google.cloud import storage
from google.cloud import bigquery
import google
from typing import Iterator, Union
import argparse
from dateutil.parser import parse
import pyarrow
from pathlib import Path
import json
import csv
from datetime import datetime
from hashlib import sha1
from calendar import monthrange
import time

def sha(row):
    """Apply sha1() to the key"""
    return sha1(row).hexdigest()

@task
def dbt_transform() -> None:
    """Run the dbt transformations on our BigQuery table"""

    DBT_PATH = os.environ.get("DBT_PATH")
    DBT_HOME = os.environ.get("DBT_HOME")

    dbt_path = f"{os.getcwd()}/{DBT_PATH}"

    dbt_op = DbtCoreOperation(
        commands=["dbt build"],
        working_dir=dbt_path,
        project_dir=dbt_path,
        profiles_dir=f"{Path.home()}/{DBT_HOME}/",
    )

    dbt_op.run()

@task
def gcs_to_bq(file: str) -> None:
    """Loading data to BigQuery"""
    # script runs but no data in BigQuery
    gcp_credentials = GcpCredentials.load(
        os.environ.get("PREFECT_GCP_CREDENTIALS_BLOCK_NAME")
    )
    try:
        df = pd.read_parquet(f"gs://{BUCKET_NAME}/{file}")
        # df["Date"] = pd.to_datetime(df["Date"].dt.date)
        # pyarrow.lib.ArrowTypeError: Expected bytes, got a 'datetime.time' object
        # df["Time"] = pd.to_timedelta(df["Time"].astype(str))
        df["key"] = df["ISIN"].fillna("") + "_" + df["Date"].astype(str).fillna("") + "_" + df["Time"].astype(str).fillna("")
        df["key"] = df["key"].str.lower().str.encode('utf-8').apply(sha)
        print(f"Number of rows {df.shape}")
        table_schema=[
            {
                "name": "key",
                "type": "STRING",
            },
            {
                "name": "ISIN",
                "type": "STRING",
            },
            {    
                "name": "Mnemonic",
                "type": "STRING",
            },
            {
                "name": "SecurityDesc",
                "type": "STRING",
            },
            {
                "name": "SecurityType",
                "type": "STRING",
            },
            {
                "name": "Currency",
                "type": "STRING",
            },
            {
                "name": "SecurityID",
                "type": "INT64",
            },
            {
                "name": "Date",
                "type": "TIMESTAMP",
            },
            {
                "name": "Time",
                "type": "STRING",
            },
            {
                "name": "StartPrice",
                "type": "FLOAT64",
            },
            {
                "name": "MaxPrice",
                "type": "FLOAT64",
            },
            {
                "name": "MinPrice",
                "type": "FLOAT64",
            },
            {
                "name": "EndPrice",
                "type": "FLOAT64",
            },
            {
                "name": "TradedVolume",
                "type": "INT64",
            },
            {
                "name": "NumberOfTrades",
                "type": "INT64",
            }
        ]
        # pandas_gbq.to_gbq(
        #     df,
        #     destination_table=f"{DATASET_NAME}.{DATASET_TABLE}",
        #     project_id=os.environ.get("GCP_PROJECT_ID"),
        #     credentials=gcp_credentials.get_credentials_from_service_account(),
        #     if_exists="append",
        #     table_schema=table_schema
        # )
        # https://stackoverflow.com/questions/48886761/efficiently-write-a-pandas-dataframe-to-google-bigquery
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig()
        job = client.load_table_from_dataframe(
                    df, 
                    f"{DATASET_NAME}.{DATASET_TABLE}", 
                    job_config=job_config
        )
        job.result()
    except FileNotFoundError:
        pass
    except pyarrow.lib.ArrowTypeError:
        pass

# @task
# def gcs_to_bq(file: str) -> None:
#     """Loading data to BigQuery"""
#     client = bigquery.Client()  
#     job_config = bigquery.LoadJobConfig(
#         source_format=bigquery.SourceFormat.PARQUET,
#         write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
#     )

#     dataset = bigquery.Dataset(f"{client.project}.{DATASET_NAME}")
#     dataset.location = GCP_REGION

#     # Send the dataset to the API for creation, with an explicit timeout.
#     # Raises google.api_core.exceptions.Conflict if the Dataset already
#     # exists within the project.
#     try:
#         dataset = client.create_dataset(dataset, timeout=30)  # Make an API request
#     except google.api_core.exceptions.Conflict:
#         pass

#     load_job = client.load_table_from_uri(
#         f"gs://{BUCKET_NAME}/{file}", 
#         f"{dataset}.{DATASET_TABLE}", 
#         job_config=job_config
#     )

@task(log_prints=True, name="list files from gcs")
def list_gcs(bucket_name: str, prefix: str, delimiter: str) -> Iterator[str]:
    """Loop through files in GCS directory"""
    storage_client = storage.Client()
    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)
    for blob in blobs:
        yield blob.name

@task(log_prints=True, name="dataset_load_check")
def is_dataset_loaded(prefix: str) -> Union[dict, None]:
    """Check whether dataset has been loaded or not"""
    try:
        ds = pd.read_csv("config/datasets_loaded.csv")
    except Exception as e:
        raise(e)
    date = prefix.split("/")[-1]
    record = ds.query("load_date == @date")
    if record.shape[0] > 0:
        return record.to_dict(orient="records")[0]


@task(log_prints=True, name="history_load")
def historical_load(filename: str) -> Iterator[str]:
    """Read json file to load historical datasets"""
    try:
        with open(filename, "r") as f:
            data = json.load(f)
            for period in data["load_date"]:
                yield period
    except Exception as e:
        raise(e)

@task(log_prints=True, name="update_dataload")
def update_datasets_loaded(filename: str, prefix: str):
    """Update the datasets_loaded config file to keep 
    track of the datasets loaded"""
    with open(filename, 'a') as f:
        writer = csv.writer(f)
        rec = [prefix.split("/")[-1], datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")]
        writer.writerow(rec)
    
@flow(log_prints=True, name="Pipeline to read files from GCS and load to BigQuery")
def pipeline(bucket_name: str, prefix: str, delimiter: str = "") -> int:
    """Loop through files in GCS directory"""
    files = list_gcs(bucket_name, prefix, delimiter)
    num = 0
    for file in files:
        print(f"file: {file}")
        num += 1
        gcs_to_bq(file)
    return num

@flow(log_prints=True, name="Entry point")
def main(prefix: str, history_file: str) -> None:
    bucket_name = BUCKET_NAME
    datasets_loaded_file = "config/datasets_loaded.csv"
    if len(history_file) > 0:
        for period in historical_load(history_file):
            try:
                yr, mn = datetime.strptime(period, "%Y-%m").strftime("%Y-%m").split("-")
            except Exception as e:
                raise(e)
            for day in range(1, monthrange(int(yr), int(mn))[1] + 1):
                dt = f"{yr}-{int(mn):02}-{day:02}"
                prefix = f"{os.environ.get('GCP_PREFIX')}/{dt}"
                rec = is_dataset_loaded(prefix)
                if not rec is None:
                    print(f"History load completed for {rec}")
                    continue
                num = pipeline(bucket_name, prefix)
                if num == 0:
                    print(f"No files for period {prefix.split('/')[-1]}")
                
                if num > 0:
                    dbt_transform()
                    update_datasets_loaded(datasets_loaded_file, prefix)
    else:
        rec = is_dataset_loaded(prefix)
        if not rec is None:
            print(f"History load completed for {rec}")
        else:
            num = pipeline(bucket_name, prefix)
            if num == 0:
                print(f"No files for period {prefix.split('/')[-1]}")
            
            if num > 0:
                dbt_transform()
                update_datasets_loaded(datasets_loaded_file, prefix)
        

if __name__ == "__main__":
    start = time.time()
    load_dotenv()

    DATASET_NAME = os.environ.get("GCP_DATASET_NAME")
    DATASET_TABLE = os.environ.get("GCP_DATASET_TABLE_NAME")
    BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME")
    GCP_REGION = os.environ.get("GCP_REGION")
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

    parser = argparse.ArgumentParser("Pipeline to read from GCS and write to BigQuery")
    parser.add_argument("--date", type=str, help="Enter period of load. Format yyyy-mm-dd", default="2022-03-25")
    parser.add_argument("--file", type=str, help="Enter path to historical loading file.")

    args = parser.parse_args()
    if args.date:
        prefix = args.date
    
    if len(prefix) < 10:
        raise ValueError(f"Invalid date {prefix}. Format is yyyy-mm-dd")
    if "-" not in args.date or args.date.count("-") != 2:
        raise ValueError(f"Invalid date {prefix}. Format is yyyy-mm-dd")
    try: 
        parse(args.date)
    except ValueError:
        raise ValueError(f"Invalid date {prefix}. Format is yyyy-mm-dd")
    
    if args.file is None:
        args.file = ""
    
    
    main(f"{os.environ.get('GCP_PREFIX')}/{args.date}", args.file)
    end = time.time()
    print(f"Load to BigQuery completed {(end-start)/60} mins")