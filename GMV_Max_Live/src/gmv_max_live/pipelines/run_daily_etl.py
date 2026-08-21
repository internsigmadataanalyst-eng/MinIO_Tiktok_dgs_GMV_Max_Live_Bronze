# src/gmv_max_live/pipelines/run_daily_etl.py

import os
import io
from datetime import date
from google.oauth2 import service_account

import pandas as pd

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
from src.gmv_max_live.transform.merge_silver import merge_to_silver
from src.gmv_max_live.load.load_to_bigquery import load_df

PROJECT_ID = "database-sigma"
WATERMARK_PATH = "watermarks/gmv_max.json"


def _get_credentials():
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)


def _select_recovered(
    df_valid: pd.DataFrame, resolved: list, report: dict
) -> pd.DataFrame:
    """PATH A: select rows from df_valid that were recovered from a resolved error.

    A resolved entry (sheet_name, creds, error_date) means the key was in the
    error manifest last run but is NO LONGER in df_error this run (the data
    got fixed). Those rows bypass the watermark filter downstream.

    Full recovery only: we include the key's rows ONLY when the number of
    valid rows now equals the manifest n_rows. Otherwise the group is either
    only partially fixed (some rows still bad -> entry stays open) or extra
    rows appeared on that historical date. Skipping avoids duplicates and
    partial/incorrect recovery; the data is never silently lost because the
    entry remains "open" and will be retried on a later run.

    Counters are added to `report`:
      recovery_resolved        : resolved keys considered
      recovery_recovered_rows  : rows selected for Path A
      recovery_count_mismatch  : keys fixed but row_count != n_rows (skipped)
      recovery_absent          : resolved keys with no matching rows (deleted)
    """
    df = df_valid.copy()

    if df.empty or not resolved:
        report.setdefault("recovery_resolved", 0)
        report.setdefault("recovery_recovered_rows", 0)
        report.setdefault("recovery_count_mismatch", 0)
        report.setdefault("recovery_absent", 0)
        return df.iloc[0:0]

    key_series = (
        df["sheet_name"].astype(str)
        + "|" + df["creds"].astype(str)
        + "|" + df["Tanggal"].dt.date.astype(str)
    )

    match = pd.Series(False, index=df.index)
    count_mismatch = 0
    absent = 0

    for r in resolved:
        key = f'{r["sheet_name"]}|{r["creds"]}|{r["error_date"]}'
        grp = df.index[key_series == key]
        n_expected = int(r.get("n_rows") or 0)

        if len(grp) == 0:
            absent += 1                      # rows removed from sheet
        elif len(grp) == n_expected:
            match.loc[grp] = True            # fully recovered -> Path A
        else:
            count_mismatch += 1              # FIXED but count mismatch -> skip

    report["recovery_resolved"] = len(resolved)
    report["recovery_recovered_rows"] = int(match.sum())
    report["recovery_count_mismatch"] = count_mismatch
    report["recovery_absent"] = absent

    return df[match]


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
    # Resolved entries feed PATH A (error recovery) below.
    resolved = sync_error_manifest(minio_client, minio_bucket, df_error, v_report, today_key, run_key, df_valid=df_valid)

    if not df_error.empty:
        write_quarantine(minio_client, minio_bucket, df_error, today_key, run_key)

    # PATH A: recovered rows (fixed since last run) bypass the watermark.
    df_recovered = _select_recovered(df_valid, resolved, v_report)
    print(
        f"[RECOVERY] resolved={v_report.get('recovery_resolved', 0)} "
        f"| recovered_rows={v_report.get('recovery_recovered_rows', 0)} "
        f"| absent={v_report.get('recovery_absent', 0)} "
        f"| count_mismatch_skipped={v_report.get('recovery_count_mismatch', 0)}"
    )

    # PATH B: remaining rows use the standard per-sheet watermark filter.
    df_regular = df_valid.drop(df_recovered.index)
    df_bronze_regular, sheet_max_dates = build_bronze_maxl(
        df_regular, sheet_watermarks=watermark_map
    )

    # PATH A transform: empty watermarks = full load, max dates discarded.
    if df_recovered.empty:
        df_bronze_recovered = df_bronze_regular.iloc[0:0]
    else:
        df_bronze_recovered, _ = build_bronze_maxl(df_recovered, sheet_watermarks={})

    # MERGE & DEDUPLICATE
    df_bronze = pd.concat(
        [df_bronze_regular, df_bronze_recovered], ignore_index=True
    ).drop_duplicates(subset=["row_hash_raw"])
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
    # test_merge_to_silver_duckdb(df_bronze)

    load_df(
        df_bronze,
        table_id="Testing.bronze_maxl",
        project_id=PROJECT_ID,
        if_exists="append",
        credentials=creds,
    )
    print("[BRONZE] Load to BRONZE_DB.bronze_maxp DONE")

    # 4) Silver: MERGE
    print("[SILVER] Running MERGE into SILVER_DB.silver_tt_ads_gmvmax ...")
    merge_to_silver()
    print("[SILVER] MERGE DONE")

    print("== ETL GMV Max Produk DONE ==")


# Kalau kamu mau bisa juga di-run langsung:
if __name__ == "__main__":
    run_daily_etl()
