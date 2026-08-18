# src/gmv_max_produk/utils/bq_client.py
import os
from google.cloud import bigquery
from google.oauth2 import service_account


def get_bq_client() -> bigquery.Client:
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")

    creds = service_account.Credentials.from_service_account_file(sa_path)
    return bigquery.Client(credentials=creds, project=creds.project_id)