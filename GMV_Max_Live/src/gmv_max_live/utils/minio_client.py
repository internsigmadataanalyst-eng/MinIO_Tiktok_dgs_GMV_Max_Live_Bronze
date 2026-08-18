# src/gmv_max_live/utils/minio_client.py
import os
import json
import io
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import pandas as pd


def get_minio_client() -> tuple[Minio, str]:
    """Instantiates and returns the MinIO client alongside the configured bucket name."""
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    minio_bucket = os.getenv("MINIO_BUCKET")
    minio_secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

    client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure,
    )
    return client, minio_bucket


def get_sheet_watermarks(minio_client: Minio, bucket: str, watermark_path: str, sheet_registry: dict | None = None) -> tuple[dict, list]:
    """Fetches the per-sheet watermark table from MinIO.

    Args:
        sheet_registry: {sheet_name: creds} mapping used ONLY to translate the
            OLD watermark format ({sheet_name,...}) into creds-keyed rows.

    Returns:
        watermark_map: {creds: last_processed_date}
        records: raw rows as read (old or new shape) — migration happens at write time.

    Assumes the watermark file holds ONLY the per-sheet table format
    ({"sheets": [...]}); a file with any other shape raises KeyError.
    """
    try:
        minio_client.stat_object(bucket, watermark_path)
    except S3Error as e:
        if e.code in ["NoSuchKey", "AccessDenied"]:
            return {}, []
        raise e

    response = minio_client.get_object(bucket, watermark_path)
    data = json.loads(response.read().decode("utf-8"))
    response.close()
    response.release_conn()

    records = data["sheets"]

    # === FAILSAFE START: old-format read compatibility (sheet_name-keyed) ===
    # Old format rows: {"sheet_name", "last_processed_date", "updated_at"}.
    # New format rows: {"creds", "sheet_name", "last_processed_date", "updated_at"}.
    # A row without "creds" is translated via sheet_registry (sheet_name -> creds).
    # If translation fails, fall back to sheet_name so the sheet gets a FULL LOAD
    # (never silently drops data). REMOVE this block after the next successful run.
    name_to_creds = sheet_registry or {}
    watermark_map = {}
    for rec in records:
        creds = rec.get("creds")
        if not creds:
            creds = name_to_creds.get(str(rec["sheet_name"])) or str(rec["sheet_name"])
        creds = str(creds)
        date_val = str(rec["last_processed_date"]).strip()[:10]
        # Beberapa sheet_name bisa berbagi creds (contoh: riwa & riwa_ajwa).
        # Selalu pakai MAX agar tidak ada data lama yang diproses ulang.
        watermark_map[creds] = max(watermark_map.get(creds, ""), date_val)
    # === FAILSAFE END ===

    print(f"[MINIO] Watermark found for {len(records)} sheet(s).")
    return watermark_map, records


def update_sheet_watermarks(minio_client: Minio, bucket: str, watermark_path: str, prev_records: list, sheet_max_dates: dict, sheet_registry: dict | None = None):
    """Persists the per-sheet watermark table to MinIO in the NEW format.

    Only sheets in sheet_max_dates get a new last_processed_date and updated_at.
    Sheets already up-to-date keep their previous values; new sheets are appended.
    """
    now = datetime.now().isoformat()
    name_to_creds = sheet_registry or {}
    creds_to_name = {creds: name for name, creds in name_to_creds.items()}

    # === FAILSAFE START: migrate OLD-format prev_records to NEW format ===
    # Old rows keyed by "sheet_name" are rewritten as {"creds", "sheet_name", ...}
    # so the file is saved in the NEW format from this run onward.
    # REMOVE this block (and sheet_registry param) after the next successful run.
    by_creds = {}
    for rec in prev_records:
        creds = rec.get("creds") or name_to_creds.get(str(rec["sheet_name"])) or str(rec["sheet_name"])
        sheet_name = creds_to_name.get(creds) or rec.get("sheet_name") or creds
        by_creds[str(creds)] = {
            "creds": str(creds),
            "sheet_name": sheet_name,
            "last_processed_date": rec["last_processed_date"],
            "updated_at": rec.get("updated_at", ""),
        }
    # === FAILSAFE END ===

    for creds, max_date in sheet_max_dates.items():
        prev_sheet_name = by_creds.get(creds, {}).get("sheet_name")
        by_creds[creds] = {
            "creds": creds,
            "sheet_name": creds_to_name.get(creds) or prev_sheet_name or creds,
            "last_processed_date": max_date,
            "updated_at": now,
        }

    records = sorted(by_creds.values(), key=lambda rec: str(rec["creds"]))

    payload = json.dumps({"sheets": records}).encode("utf-8")
    minio_client.put_object(
        bucket,
        watermark_path,
        io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )
    print(f"[MINIO] Updated watermark JSON for {len(records)} sheet(s).")


ERROR_MANIFEST_PATH = "error_list_watermark/error_manifest.json"
FIX_MANIFEST_PREFIX = "fix_error_list_watermark"
QUARANTINE_PREFIX = "quarantine"


def write_quarantine(minio_client: Minio, bucket: str, df_error: pd.DataFrame, today_key: str, run_key: str):
    """Saves bad rows to MinIO under quarantine/date=YYYYMMDD/<run_key>.parquet."""
    if df_error.empty:
        return

    folder_path = f"{QUARANTINE_PREFIX}/date={today_key}/"
    file_path = f"{folder_path}quarantine_{run_key}.parquet"

    minio_client.put_object(bucket, folder_path, io.BytesIO(b""), length=0)

    parquet_bytes = df_error.to_parquet(index=False, engine="pyarrow")
    minio_client.put_object(
        bucket,
        file_path,
        io.BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/octet-stream",
    )
    print(f"[MINIO] Quarantine bad rows to: {file_path}")


def sync_error_manifest(minio_client: Minio, bucket: str, df_error: pd.DataFrame, report: dict, today_key: str, run_key: str, manifest_path: str = ERROR_MANIFEST_PATH):
    """Syncs the error manifest at error_list_watermark/error_manifest.json.

    Runs EVERY run (even with empty df_error):
      1. Reads current open entries.
      2. Builds current-run entries from df_error grouped by
         (sheet_name, creds, error_date) — the same grain as the watermark.
      3. Resolves: open entries whose key is no longer detected this run are
         removed from the manifest and written as fix records to
         fix_error_list_watermark/date=YYYYMMDD/fix_<run_key>.json.
      4. Refreshes open entries still detected this run with the latest
         n_rows / affected_columns / path, and appends new open entries not
         already present.
      5. Writes the manifest back only if something changed (avoids creating
         an empty file when there is nothing to do).
    """
    from src.gmv_max_live.utils.transform_utils import parse_mixed_dates

    now = datetime.now().isoformat()
    quarantine_path = f"{QUARANTINE_PREFIX}/date={today_key}/quarantine_{run_key}.parquet"

    current_entries = []
    if not df_error.empty:
        df_error = df_error.copy()
        if "Tanggal" in df_error.columns:
            df_error["_parsed_date"] = parse_mixed_dates(df_error["Tanggal"], return_date=False)
            df_error["_error_date"] = df_error["_parsed_date"].dt.date.astype(str)
            df_error["_error_date"] = df_error["_error_date"].where(
                df_error["_parsed_date"].notna(), "INVALID_DATE"
            )
        else:
            df_error["_error_date"] = "INVALID_DATE"

        sn_col = "sheet_name" if "sheet_name" in df_error.columns else "creds"
        cr_col = "creds" if "creds" in df_error.columns else sn_col

        for (sheet_name, creds, error_date), idx in df_error.groupby([sn_col, cr_col, "_error_date"]).groups.items():
            current_entries.append({
                "sheet_name": str(sheet_name),
                "creds": str(creds),
                "error_date": str(error_date),
                "affected_columns": list(report["affected_columns"]),
                "n_rows": int(len(idx)),
                "reported_at": now,
                "path": quarantine_path,
                "status": "open",
            })

    manifest_existed = True
    try:
        minio_client.stat_object(bucket, manifest_path)
        response = minio_client.get_object(bucket, manifest_path)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()
        open_records = [r for r in data.get("errors", []) if r.get("status") == "open"]
    except S3Error as e:
        if e.code in ["NoSuchKey", "AccessDenied"]:
            open_records = []
            manifest_existed = False
        else:
            raise e

    current_keys = {(e["sheet_name"], e["creds"], e["error_date"]) for e in current_entries}

    resolved = []
    refreshed = {}
    for rec in open_records:
        key = (str(rec.get("sheet_name")), str(rec.get("creds")), str(rec.get("error_date")))
        if key in current_keys:
            refreshed[key] = rec
        else:
            resolved.append(rec)

    # Fresh current-run entries always win: refresh n_rows / affected_columns /
    # reported_at / path for keys that already exist, append brand-new keys.
    for entry in current_entries:
        refreshed[(entry["sheet_name"], entry["creds"], entry["error_date"])] = entry

    remaining = list(refreshed.values())

    # Detect changes by comparing serialized content so refreshed entries (same
    # key, different n_rows / columns) are written back, not just appended ones.
    new_payload = json.dumps({"errors": remaining}, ensure_ascii=False, sort_keys=True)
    old_payload = json.dumps({"errors": open_records}, ensure_ascii=False, sort_keys=True)
    changed = bool(resolved) or new_payload != old_payload
    if not changed and not manifest_existed:
        return

    if resolved:
        fix_folder = f"{FIX_MANIFEST_PREFIX}/date={today_key}/"
        fix_path = f"{fix_folder}fix_{run_key}.json"
        fixes = [{
            "sheet_name": r["sheet_name"],
            "creds": r["creds"],
            "error_date": r["error_date"],
            "affected_columns": r.get("affected_columns", []),
            "resolved_at": now,
            "path": r.get("path", ""),
            "status": "fixed",
        } for r in resolved]
        minio_client.put_object(bucket, fix_folder, io.BytesIO(b""), length=0)
        payload = json.dumps({"fixes": fixes}, ensure_ascii=False).encode("utf-8")
        minio_client.put_object(
            bucket,
            fix_path,
            io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        print(f"[MINIO] Resolved {len(fixes)} error entr(y/ies) -> fix record: {fix_path}")

    payload = json.dumps({"errors": remaining}, ensure_ascii=False).encode("utf-8")
    minio_client.put_object(
        bucket,
        manifest_path,
        io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )
    if current_entries:
        print(f"[MINIO] Synced {len(current_entries)} open error entr(y/ies) to {manifest_path}")


def filter_by_sheet_watermark(df: pd.DataFrame, sheet_col: str, date_col: str, watermarks: dict) -> tuple[pd.DataFrame, dict]:
    """Per-sheet incremental filter on already-clean dates (Timestamp dtype).

    For each group key (e.g. creds/sheet_name), keep rows where date > watermarks[key].
    Groups without a watermark are treated as full load.
    Returns (filtered_df, sheet_max_dates) where sheet_max_dates maps
    the group key -> last processed date (ISO) computed only from kept rows.
    """
    parsed = pd.to_datetime(df[date_col])

    keep = pd.Series(True, index=df.index)
    for name, idx in df.groupby(sheet_col).groups.items():
        wm = watermarks.get(name)
        if wm:
            cutoff = pd.Timestamp(wm)
            keep.loc[idx] = parsed.loc[idx] > cutoff

    filtered = df[keep].copy()

    sheet_max_dates = {}
    parsed_kept = parsed[keep]
    for name, idx in filtered.groupby(sheet_col).groups.items():
        mx = parsed_kept.loc[idx].dropna()
        if not mx.empty:
            sheet_max_dates[name] = mx.max().date().isoformat()

    return filtered, sheet_max_dates
