# src/gmv_max_live/utils/gsheet_client.py
import os
import time

import gspread
from google.oauth2 import service_account
from dotenv import load_dotenv
from gspread.exceptions import APIError

# Load variables from .env into environment
load_dotenv()


def with_retry_on_429(func, *args, max_retries=6, delay=15, **kwargs):
    """Call func(*args, **kwargs), retrying on Google Sheets API 429 errors.

    Retries with a fixed short delay (default 15s, matching roughly the
    per-minute quota window) until max_retries is exhausted; any other
    error is re-raised immediately.
    """
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            if "429" in str(e) and attempt < max_retries - 1:
                print(
                    f"[RETRY] Google Sheets rate limited (429), "
                    f"sleeping {delay}s before retry {attempt + 1}/{max_retries}..."
                )
                time.sleep(delay)
            else:
                raise


def get_gspread_client() -> gspread.Client:
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")

    creds = service_account.Credentials.from_service_account_file(sa_path)
    return gspread.service_account(filename=sa_path)