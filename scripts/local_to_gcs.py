import io
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import time
from pathlib import Path
from calendar import monthrange
import argparse


BUCKET = os.environ.get("GCP_BUCKET_NAME")

def leap_year(year):
    """Check whether year is a leap year or not"""
    return (year%4 == 0) and not ( year%100 == 0) or year%400 == 0

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(str(object_name) if isinstance(object_name, Path) else object_name)
    blob.upload_from_filename(local_file)

def web_to_gcs(year: int, base_path: str):
    """Copy file from local to GCS"""
    Path(f"{base_path}").mkdir(parents=True, exist_ok=True)
    for month in range(1, 13):
        for day in range(1, monthrange(year, month)[1] + 1):
            # csv file_name
            dirpath = Path(f"{base_path}/{year}-{month:02}-{day:02}").resolve()
            if not dirpath.exists(): continue
            print(f"Processing files in {base_path}/{year}-{month:02}-{day:02}")
            for file in os.listdir(dirpath):
                if file.endswith(".csv"):
                    try:
                        df = pd.read_csv(os.path.join(dirpath, file))
                    except FileNotFoundError:
                        print(f"{file} doesn't exist")
                        continue
                    if len(df) > 0:
                        df["Date"] = pd.to_datetime(df["Date"])
                        df["Time"] = pd.to_datetime(df["Time"], format="%H:%M").dt.time
                        file = file.replace(".csv", "")
                        out_path = Path(f"{base_path}/{year}-{month:02}-{day:02}/{file}.parquet")
                        df.to_parquet(out_path, engine="pyarrow")
                        # upload it to gcs 
                        upload_to_gcs(BUCKET, out_path, out_path)
                        print(f"GCS: {out_path}")

if __name__ == "__main__":
    start = time.time()
    parser = argparse.ArgumentParser("ETL Local to GCS")
    parser.add_argument("--year", type=int, help="Add year of data upload", default=2022)
    base_path = os.environ.get("GCP_PREFIX")

    args = parser.parse_args()
    if args.year:
        year = args.year
    web_to_gcs(year, base_path)
    end = time.time()
    print(f"Local to GCS file transmission completed {(end-start)/60} mins")