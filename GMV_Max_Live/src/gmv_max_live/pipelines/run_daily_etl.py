# src/gmv_max_live/pipelines/run_daily_etl.py

import os
import io
from datetime import date
from google.oauth2 import service_account

from dotenv import load_dotenv

# Load variables from .env into environment
load_dotenv()

from src.gmv_max_live.utils.gsheet_client import get_gspread_client
from src.gmv_max_live.utils.bq_client import get_bq_client
from src.gmv_max_live.utils.minio_client import (
    get_minio_client,
    get_sheet_watermarks,
    update_sheet_watermarks,
    write_quarantine,
    sync_error_manifest,
)
from src.gmv_max_live.utils.transform_utils import (
    NUMERIC_COLS,
    PERCENT_COLS,
    validate_and_normalize_raw,
)
from src.gmv_max_live.ingestion.fetch_gmv_max_live_gsheet import (
    fetch_gmv_max_live,
    SHEET_REGISTRY,
)
from src.gmv_max_live.transform.clean_bronze import build_bronze_maxl
from src.gmv_max_live.transform.merge_silver_duckdb import test_merge_to_silver_duckdb

PROJECT_ID = "database-sigma"
WATERMARK_PATH = "watermarks/gmv_max.json"


def _get_credentials():
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)


def run_daily_etl():
    print("== Start ETL GMV Max Live ==")

    # 1) Client
    gc = get_gspread_client()
    bq_client = get_bq_client()
    creds = _get_credentials()
    minio_client, minio_bucket = get_minio_client()

    # 2) Date key: partition pakai YYYYMMDD, nama file pakai YYYYMMDDHH
    #    (jam agar 2 run di hari yang sama menghasilkan file terpisah, tanpa overwrite).
    today_obj = date.today()
    today_key = today_obj.strftime("%Y%m%d")
    run_key = today_obj.strftime("%Y%m%d%H")

    # 3) Per-sheet watermark check
    # sheet_registry hanya dibutuhkan utk FAILSAFE migrasi format lama (sheet_name -> creds).
    sheet_registry = {name: os.getenv(env_key) for name, env_key in SHEET_REGISTRY.items()}
    watermark_map, watermark_records = get_sheet_watermarks(
        minio_client, minio_bucket, WATERMARK_PATH, sheet_registry=sheet_registry
    )

    # 4) Ingest from GSheet (each sheet tagged with sheet_name)
    df_raw = fetch_gmv_max_live(gc)
    print(f"[INGEST] Rows raw from GSheet: {len(df_raw)}")

    # 4b) STEP 2: validate & normalize as early as possible (mixed-column
    #     detection + date-error capture). Runs exactly once, before anything else.
    
    # buang baris tanpa id_campaign
    df_raw = df_raw[df_raw["ID Campaign"].astype(str).str.strip() != ""]
    df_valid, df_error, v_report = validate_and_normalize_raw(
        df_raw, NUMERIC_COLS, percent_cols=PERCENT_COLS
    )
    print(
        f"[VALIDATE] Rows valid: {len(df_valid)} | bad rows: {v_report['n_bad_rows']} "
        f"(date errors: {v_report['n_date_errors']}) | blank rows dropped: {v_report['n_blank_rows']}"
    )
    if v_report["has_changes"]:
        print(f"[VALIDATE] Corrupted/Shifted columns: {v_report['affected_columns']}")
        print(
            f"[VALIDATE] Affected date range: {v_report['first_affected_date']} "
            f"---> {v_report['last_affected_date']}"
        )

    # STEP 3Q/6: sync error manifest EVERY run (append new open entries +
    # resolve entries whose format has been fixed since the last run).
    sync_error_manifest(minio_client, minio_bucket, df_error, v_report, today_key, run_key)

    if not df_error.empty:
        write_quarantine(minio_client, minio_bucket, df_error, today_key, run_key)

    # 5) Bronze Transformation + per-sheet incremental filter
    df_bronze, sheet_max_dates = build_bronze_maxl(
        df_valid, sheet_watermarks=watermark_map
    )
    print(f"[BRONZE] Rows bronze to load: {len(df_bronze)}")

    if df_bronze.empty:
        print("[MINIO] No new data to process. Data is up-to-date.")
        print("== ETL GMV Max DONE ==")
        return

    # 6) Parquet conversion & Load to MinIO
    file_path = f"gmv/max/date={today_key}/max_{run_key}.parquet"
    folder_path = f"gmv/max/date={today_key}/"

    # Folder partition marker
    minio_client.put_object(minio_bucket, folder_path, io.BytesIO(b""), length=0)

    # Convert & Upload Parquet
    parquet_bytes = df_bronze.to_parquet(index=False, engine="pyarrow")
    minio_client.put_object(
        minio_bucket,
        file_path,
        io.BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/octet-stream",
    )
    print(f"[MINIO] Successfully uploaded Parquet file to: {file_path}")

    # 7) Update per-sheet watermark (selalu tulis format baru)
    update_sheet_watermarks(
        minio_client, minio_bucket, WATERMARK_PATH, watermark_records, sheet_max_dates,
        sheet_registry=sheet_registry,
    )

    # 8) Testing Load to Bronze & Silver via DuckDB (In-Memory)
    test_merge_to_silver_duckdb(df_bronze)

    print("== ETL GMV Max Live DONE ==")


# Kalau kamu mau bisa juga di-run langsung:
if __name__ == "__main__":
    run_daily_etl()
