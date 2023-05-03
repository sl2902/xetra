import json
import os

from prefect.blocks.system import JSON

CONFIG_FILE = os.environ.get("CONFIG_FILE")

with open(CONFIG_FILE, "r") as config_file:
    config = json.load(config_file)

# Set the value of the JSON block
json_block_value = {
    "GCP_PROJECT_ID": config.get("GCP_PROJECT_ID"),
    "GCP_DATASET_NAME": config.get("GCP_DATASET_NAME"),
    "GCP_DATASET_TABLE_NAME": config.get("GCP_DATASET_TABLE_NAME"),
    "LOCAL_SERVICE_ACCOUNT_FILE_PATH": config.get("LOCAL_SERVICE_ACCOUNT_FILE_PATH"),
    "GCP_REGION": config.get("GCP_REGION"),
    "GCP_BUCKET_NAME": config.get("GCP_BUCKET_NAME"),
    "GCP_PREFIX": config.get("GCP_PREFIX"),
    "PREFECT_API_KEY": config.get("PREFECT_API_KEY"),
    "PREFECT_API_URL": config.get("PREFECT_API_URL"),
    "PREFECT_DOCKER_IMAGE": config.get("PREFECT_DOCKER_IMAGE"),
    "DBT_PROFILE": config.get("DBT_PROFILE"),
    "DBT_HOME": config.get("DBT_HOME"),
    "DBT_PATH":  config.get("DBT_PATH")
}

json_block = JSON(value=json_block_value)
json_block.save("de-prefect-config", overwrite=True)