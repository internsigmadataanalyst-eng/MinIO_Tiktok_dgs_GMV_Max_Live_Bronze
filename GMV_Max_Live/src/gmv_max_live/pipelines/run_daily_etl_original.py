# src/gmv_max_live/pipelines/run_daily_etl.py

import os
from google.oauth2 import service_account

from gmv_max_live.utils.gsheet_client import get_gspread_client
from gmv_max_live.utils.bq_client import get_bq_client
from gmv_max_live.ingestion.fetch_gmv_max_live_gsheet import (
    fetch_gmv_max_live,
)
from gmv_max_live.transform.clean_bronze import build_bronze_maxp
from gmv_max_live.transform.merge_silver import merge_to_silver
from gmv_max_live.load.load_to_bigquery import load_df

PROJECT_ID = "database-sigma"


def _get_credentials():
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)


def run_daily_etl():
    print("== Start ETL GMV Max live ==")

    # 1) Client
    gc = get_gspread_client()
    bq_client = get_bq_client()
    creds = _get_credentials()

    # 2) Ingest dari GSheet
    df_raw = fetch_gmv_max_live(gc)
    print(f"[INGEST] Rows raw from GSheet: {len(df_raw)}")

    # 3) Bronze: cleaning + snapshot + hash
    df_bronze, _ = build_bronze_maxp(df_raw)
    print(f"[BRONZE] Rows bronze to load: {len(df_bronze)}")

    load_df(
        df_bronze,
        table_id="BRONZE_DB.bronze_maxp",
        project_id=PROJECT_ID,
        if_exists="append",
        credentials=creds,
    )
    print("[BRONZE] Load to BRONZE_DB.bronze_maxp DONE")

    # 4) Silver: MERGE
    print("[SILVER] Running MERGE into SILVER_DB.silver_tt_ads_gmvmax ...")
    merge_to_silver()
    print("[SILVER] MERGE DONE")

    print("== ETL GMV Max live DONE ==")


# Kalau kamu mau bisa juga di-run langsung:
if __name__ == "__main__":
    run_daily_etl()