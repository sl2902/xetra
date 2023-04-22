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
from typing import Iterator
import argparse
from dateutil.parser import parse
import pyarrow
from pathlib import Path

@task
def dbt_transform() -> None:
    """Run the dbt transformations on our BigQuery table"""

    dbt_path = f"{os.getcwd()}/dbt/xetra"

    dbt_op = DbtCoreOperation(
        commands=["dbt build"],
        working_dir=dbt_path,
        project_dir=dbt_path,
        profiles_dir=f"{Path.home()}/.dbt/",
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
        print(f"Number of rows {df.shape}")
        table_schema=[
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
def main(prefix: str) -> None:
    bucket_name = BUCKET_NAME
    num = pipeline(bucket_name, prefix)
    if num == 0:
        print(f"No files for period {prefix}")
    
    if num > 0:
        dbt_transform()
        

if __name__ == "__main__":
    load_dotenv()

    DATASET_NAME = os.environ.get("GCP_DATASET_NAME")
    DATASET_TABLE = os.environ.get("GCP_DATASET_TABLE_NAME")
    BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME")
    GCP_REGION = os.environ.get("GCP_REGION")
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

    parser = argparse.ArgumentParser("Pipeline to read from GCS and write to BigQuery")
    parser.add_argument("--date", type=str, help="Enter period of load. Format yyyy-mm-dd", default="2022-03-25")

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
    
    main(f"{os.environ.get('GCP_PREFIX')}/{prefix}")