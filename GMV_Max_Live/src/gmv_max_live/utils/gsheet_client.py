# src/gmv_max_produk/utils/gsheet_client.py
import os
import gspread
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load variables from .env into environment
load_dotenv()


def get_gspread_client() -> gspread.Client:
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")

    creds = service_account.Credentials.from_service_account_file(sa_path)
    return gspread.service_account(filename=sa_path)