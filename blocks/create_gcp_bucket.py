from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.blocks.system import JSON

PREFECT_GCP_CREDENTIALS_BLOCK_NAME = "de-prefect-gcpcreds"
PREFECT_GCP_BUCKET_NAME = "de-prefect-gcpbucket"

def create_gcp_bucket(bucket_name: str) -> None:
    bucket_block = GcsBucket(
        gcp_credentials=GcpCredentials.load(PREFECT_GCP_CREDENTIALS_BLOCK_NAME),
        bucket=bucket_name,
    )
    bucket_block.save(PREFECT_GCP_BUCKET_NAME, overwrite=True)

if __name__ == "__main__":
     env = JSON.load("de-prefect-config")
     bucket_name = env.value.get("GCP_BUCKET_NAME")
     create_gcp_bucket(bucket_name)