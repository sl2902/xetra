from prefect_gcp import GcpCredentials
import json
import os

PREFECT_GCP_CREDENTIALS_BLOCK_NAME = "de-prefect-gcpcreds"

def gcp_credentials() -> None:
    with open(SERVICE_ACCOUNT_FILE, "r") as file:
        json_content = json.load(file)

    service_account_info = {
        "type": json_content.get("type"),
        "project_id": json_content.get("project_id"),
        "private_key_id": json_content.get("private_key_id"),
        "private_key": json_content.get("private_key"),
        "client_email": json_content.get("client_email"),
        "client_id": json_content.get("client_id"),
        "auth_uri": json_content.get("auth_uri"),
        "token_uri": json_content.get("token_uri"),
        "auth_provider_x509_cert_url": json_content.get("auth_provider_x509_cert_url"),
        "client_x509_cert_url": json_content.get("client_x509_cert_url"),
    }
    GcpCredentials(service_account_info=service_account_info).save(PREFECT_GCP_CREDENTIALS_BLOCK_NAME, overwrite=True)

if __name__ == "__main__":
    SERVICE_ACCOUNT_FILE = os.environ.get("LOCAL_SERVICE_ACCOUNT_FILE_PATH")
    gcp_credentials()