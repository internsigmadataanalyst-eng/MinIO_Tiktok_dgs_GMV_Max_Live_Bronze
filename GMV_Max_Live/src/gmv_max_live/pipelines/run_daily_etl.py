# src/gmv_max_live/pipelines/run_daily_etl.py

import os
import io
import traceback
from datetime import datetime
from google.oauth2 import service_account

import pandas as pd

from dotenv import load_dotenv

# Load variables from .env into environment
load_dotenv()

from src.gmv_max_live.utils.gsheet_client import get_gspread_client
# from src.gmv_max_live.utils.bq_client import get_bq_client
from src.gmv_max_live.utils.minio_client import (
    get_minio_client,
    get_sheet_watermarks,
    update_sheet_watermarks,
    write_quarantine,
    sync_error_manifest,
    filter_already_quarantined,
    QUARANTINE_PREFIX,
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
from src.gmv_max_live.transform.merge_silver import merge_to_silver
from src.gmv_max_live.load.load_to_bigquery import load_df
from src.gmv_max_live.utils.bronze_compare import (
    effective_watermark_changes,
    show_watermark,
)
from src.gmv_max_live.utils.log import (
    get_log_folder,
    write_section_log,
    setup_event_logging,
    is_event_logging_enabled,
    emit,
)
from src.gmv_max_live.utils.watermark_monitor import gmv_max_live_watermark_check
from src.gmv_max_live.utils.notify import (
    send_alert_email,
    build_gate_abort_email,
    build_pipeline_success_email,
    build_quarantine_email,
    build_recovery_email,
    QUARANTINE_SAMPLE_ROWS,
    QUARANTINE_SAMPLE_COLUMNS,
)

PROJECT_ID = "database-sigma"
WATERMARK_PATH = "watermarks/gmv_max.json"

SRC_GSHEET = {"system": "Google_Sheets", "entity": "GMV Max Live"}
SRC_MINIO = {"system": "MinIO", "entity": WATERMARK_PATH}
TGT_MINIO = {"system": "MinIO", "entity": "gmv/max"}
TGT_MINIO_QUARANTINE = {"system": "MinIO", "entity": "quarantine/gmv_max_live/"}
TGT_BQ_BRONZE = {"system": "BigQuery", "entity": f"{PROJECT_ID}.Testing.bronze_maxl"}
TGT_BQ_SILVER = {"system": "BigQuery", "entity": f"{PROJECT_ID}.Testing.silver_tt_ads_gmvmax_live"}
WHITELIST_SHEETS = {"deni_etawa", "riwa_ajwa", "ian"} 

# BigQuery targets summarized in every alert email (reminder of what this
# project updates). Full project.dataset.table paths.
BQ_TARGETS = [
    {"table": TGT_BQ_BRONZE["entity"], "action": "append"},
    {"table": TGT_BQ_SILVER["entity"], "action": "MERGE (silver upsert)"},
]

# Populated as the run advances through its write stages; read by main.py's
# exception handler so the failure email knows the stage + rollback story.
_failure_ctx = {
    "stage": "",
    "minio_files": [],
    "rollback_hint": "",
    "rollback_command": "",
    "auto_rollback_note": "",
} 


def _bq_full_load(rows: int) -> list[dict]:
    """BQ-update summary for a run that appended rows to bronze + merged silver."""
    return [
        {"table": TGT_BQ_BRONZE["entity"], "action": "append", "rows": rows},
        {"table": TGT_BQ_SILVER["entity"], "action": "MERGE (silver upsert)"},
    ]


def _bq_noop() -> list[dict]:
    """BQ-update summary for a successful run with no new rows to load."""
    return [
        {"table": TGT_BQ_BRONZE["entity"], "action": "no change", "rows": 0},
        {"table": TGT_BQ_SILVER["entity"], "action": "no change"},
    ]


def _finish(
    watermark_records,
    note: str,
    status: str = "",
    dry_run: bool = False,
    run_key: str = "",
    watermark_updates: dict | None = None,
    bq_updates: list[dict] | None = None,
):
    """Final step on every exit path: show the current watermark, print the
    ETL DONE line, then send the success alert (skipped in dry-run)."""
    show_watermark(watermark_records)
    print(note)
    subject, body_html = build_pipeline_success_email(
        "gmv_max_live",
        run_key=run_key,
        log_path=f"logs/run_{run_key}/etl_full_{run_key}.log" if run_key else "",
        status=status,
        watermark_updates=watermark_updates,
        bq_updates=bq_updates,
    )
    send_alert_email(subject, body_html, dry_run=dry_run)


def _get_credentials():
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)


def _fetch_existing_bronze_hashes(
    creds, table_id="Testing.bronze_maxl", project_id=PROJECT_ID
) -> set:
    """Returns the set of row_hash_raw already present in Bronze.

    Used as an append-time idempotency gate: boundary-day rows (tanggal ==
    watermark) and re-loaded recovery rows are legitimately re-selected by the
    watermark filter every run; this drops the ones whose content is unchanged,
    so Bronze stops accumulating duplicates while still accepting edits (a
    changed row produces a NEW hash and flows through).
    """
    from pandas_gbq import read_gbq

    df_hashes = read_gbq(
        f"SELECT DISTINCT row_hash_raw FROM `{project_id}.{table_id}`",
        project_id=project_id,
        credentials=creds,
        dialect="standard",
    )
    return set(df_hashes["row_hash_raw"].dropna().astype(str))


def _select_recovered(
    df_valid: pd.DataFrame, resolved: list, report: dict
) -> pd.DataFrame:
    """PATH A: select rows from df_valid that were recovered from a resolved error.

    Grain is (sheet_name, creds, toko, error_date) — toko verbatim. A resolved
    entry means the key was in the error manifest last run but is NO LONGER
    in df_error this run (the data got fixed). Those rows bypass the watermark
    filter downstream.

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

    if "Toko" in df.columns:
        toko_series = df["Toko"].astype(str)
    elif "toko" in df.columns:
        toko_series = df["toko"].astype(str)
    else:
        toko_series = pd.Series("", index=df.index, dtype=str)

    try:
        tanggal_str = df["Tanggal"].dt.date.astype(str)
    except Exception:
        tanggal_str = pd.to_datetime(df["Tanggal"]).dt.date.astype(str)

    key_series = (
        df["sheet_name"].astype(str)
        + "|" + df["creds"].astype(str)
        + "|" + toko_series
        + "|" + tanggal_str
    )

    match = pd.Series(False, index=df.index)
    count_mismatch = 0
    absent = 0

    for r in resolved:
        key = f'{r["sheet_name"]}|{r["creds"]}|{r.get("toko") or ""}|{r["error_date"]}'
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


def _write_wm_log(log_folder, run_key, status_df, sheet_passes, verdict_msg):
    """Write the watermark drift check log file."""
    wm_log_lines = []
    wm_log_lines.append(
        f"=== WATERMARK DRIFT CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n"
    )

    wm_log_lines.append("-" * 50)
    wm_log_lines.append("DATASET: GMV MAX LIVE (toko grain)")
    wm_log_lines.append("-" * 50)
    wm_log_lines.append(f"  {'sheet':<10} {'grain':<12} {'gsheet':<12} {'wm':<12} {'status'}")
    for _, row in status_df.iterrows():
        wm_log_lines.append(
            f"  {str(row['sheet_name']):<10} {str(row['grain']):<12} "
            f"{str(row['sheet_max_tanggal']):<12} {str(row['last_processed_date']):<12} "
            f"{'BEHIND' if row['is_behind'] else 'ok'}"
        )

    wm_log_lines.append(f"\nGate verdict: {verdict_msg}")
    pass_count = int(sheet_passes.sum()) if len(sheet_passes) else 0
    total = len(sheet_passes)
    wm_log_lines.append(f"  {pass_count}/{total} sheets have >=1 toko behind")

    write_section_log(log_folder, f"wm_monitor_logs_{run_key}.log", "\n".join(wm_log_lines) + "\n")


def _write_failure_log(log_folder, run_key, message: str):
    """Write a gate-abort failure log file for this run."""
    f_path = write_section_log(
        log_folder,
        f"etl_failed_{run_key}.log",
        f"ETL FAILED because: {message}\n",
    )
    print(f"[FATAL] ETL failed because: {message}")
    print(f"[FATAL] Failure written to: {f_path}")


def _fmt_drift_date(val) -> str:
    """Format a drift-check date value for the gate-2 email table ('' for NaT)."""
    try:
        if val is None or pd.isna(val):
            return ""
        if hasattr(val, "strftime"):
            return val.strftime("%Y-%m-%d")
        return str(val)
    except Exception:
        return str(val or "")


def _build_drift_rows(status_df: pd.DataFrame) -> list[dict]:
    """Per-sheet watermark drift summary from the pre-flight check.

    Column order matches the gate-2 email table: Sheet | Toko | Sheet max date |
    Current watermark | Status (BEHIND/ok). Sorted by sheet_name then toko.
    """
    rows = []
    for _, row in status_df.iterrows():
        rows.append({
            "sheet_name": str(row.get("sheet_name") or ""),
            "toko": str(row.get("grain") or ""),
            "gsheet_max": _fmt_drift_date(row.get("sheet_max_tanggal")),
            "watermark": _fmt_drift_date(row.get("last_processed_date")),
            "status": "BEHIND" if row.get("is_behind") else "ok",
        })
    rows.sort(key=lambda r: (r["sheet_name"], r["toko"]))
    return rows


def run_daily_etl(dry_run: bool | None = None):
    print("== Start ETL GMV Max Live ==")

    if dry_run is None:
        dry_run = os.getenv("ETL_DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "y"}

    if dry_run:
        print("[DRY-RUN] Mode aktif: TIDAK ada data yang ditulis ke MinIO/BigQuery/Silver.")

    # Failure context tracker for main.py's exception handler. Cleared on each
    # run so a stale stage never leaks into a later run's failure email.
    _failure_ctx.clear()
    _failure_ctx.update({
        "stage": "", "minio_files": [], "rollback_hint": "",
        "rollback_command": "", "auto_rollback_note": "",
    })

    # 1) Client
    gc = get_gspread_client()
    # bq_client = get_bq_client()
    creds = _get_credentials()
    minio_client, minio_bucket = get_minio_client()

    # 2) Date key: partition pakai YYYYMMDD, nama file pakai YYYYMMDDHHMM
    #    (jam+menit agar 2 run di hari yang sama menghasilkan file terpisah, tanpa overwrite).
    now_obj = datetime.now()
    today_key = now_obj.strftime("%Y%m%d")
    run_key = now_obj.strftime("%Y%m%d%H%M")
    log_folder = get_log_folder(run_key) if not dry_run else None

    # Structured event logging (JSON-lines). In standalone runs, set up a fresh
    # event log so the pipeline events are still emitted.
    if not is_event_logging_enabled():
        _, stop_event_logging = setup_event_logging(run_key, dry_run)
    else:
        stop_event_logging = None
    emit("PIPELINE", "run_daily_etl", "ETL start",
         metrics={"dry_run": dry_run},
         source=SRC_GSHEET, target=TGT_BQ_SILVER)

    # 2B) PRE-FLIGHT: Watermark drift gate
    print("\n" + "=" * 70)
    print("--- PRE-FLIGHT: Watermark Drift Check ---")
    print("=" * 70)
    status_df = gmv_max_live_watermark_check()

    view = pd.DataFrame({
        "sheet_name": status_df["sheet_name"],
        "toko": status_df["grain"],
        "gsheet": status_df["sheet_max_tanggal"],
        "wm": status_df["last_processed_date"],
        "flag": status_df["is_behind"].map({True: "BEHIND", False: "ok"}),
    })
    print("-" * 70)
    print("DATASET: GMV MAX LIVE (toko grain)")
    print("-" * 70)
    print(view.sort_values(["sheet_name", "toko"]).to_string(
        index=False,
        formatters={
            "gsheet": lambda v: v.strftime("%Y-%m-%d") if pd.notna(v) else "",
            "wm": lambda v: v.strftime("%Y-%m-%d") if pd.notna(v) else "",
        },
    ))
    print()

    total_sheets = status_df["sheet_name"].nunique() if len(status_df) else 0
    n_behind = int(status_df["is_behind"].sum()) if len(status_df) else 0
    emit("PRE_FLIGHT", "watermark_monitor", "Watermark drift check complete",
         metrics={"total_groups": total_sheets,
                  "groups_behind": status_df.groupby("sheet_name")["is_behind"].any().sum() if len(status_df) else 0,
                  "n_sheets": total_sheets, "n_behind": n_behind},
         source=SRC_MINIO)

    # Gate 1: abort on access errors (enforced in dry-run too).
    # Empty status_df (no watermark yet / first run) -> no errors, let gate 2 pass.
    has_errors = bool(len(status_df)) and status_df["status"].str.startswith("error").any()
    if has_errors:
        error_sheets = status_df[status_df["status"].str.startswith("error")]["sheet_name"].unique().tolist()
        mode = "DRY-RUN WOULD ABORT" if dry_run else "ABORT"
        print(f"[GATE] {mode} - access errors on sheets: {error_sheets}")
        print("[GATE] Fix the sheet access issue and re-run.")
        emit("GATE", "watermark_monitor",
             f"{mode} - access errors on sheets: {error_sheets}",
             level="ERROR",
             metrics={"error_sheets": error_sheets},
             source=SRC_MINIO)
        subject, body_html = build_gate_abort_email(
            gate="1",
            mode=mode,
            message=f"Access errors while checking sheets: {error_sheets}",
            lists={"error_sheets": error_sheets},
            note="Fix the sheet access issue and re-run.",
            bq_updates=BQ_TARGETS,
        )
        send_alert_email(subject, body_html, dry_run=dry_run)
        if log_folder:
            _write_wm_log(log_folder, run_key, status_df, pd.Series(dtype=bool),
                          f"ABORT - access errors on sheets: {error_sheets}")
            _write_failure_log(
                log_folder, run_key,
                f"access errors on sheets: {error_sheets}",
            )
        return

    # Gate 2: each sheet must have >=1 toko behind (enforced in dry-run too)
    sheet_passes = status_df.groupby("sheet_name")["is_behind"].any()
    caught_up = [s for s in sheet_passes[~sheet_passes].index.tolist() if s not in WHITELIST_SHEETS]
    behind_sheets = sheet_passes[sheet_passes].index.tolist()
    whitelisted_skipped = [s for s in sheet_passes[~sheet_passes].index.tolist() if s in WHITELIST_SHEETS]

    if caught_up:
        mode = "DRY-RUN WOULD ABORT" if dry_run else "ABORT"
        print(f"[GATE] Sheets already up-to-date (skipped): {caught_up}")
        print(f"[GATE] Sheets with new data: {behind_sheets}")
        if whitelisted_skipped:
            print(f"[GATE] Whitelisted sheets : {whitelisted_skipped}")
        print(f"[GATE] {mode} - sheets with no new data are required before continuing.")

        today_ts = pd.Timestamp(datetime.now().date())
        suspicious = []
        for sheet_name in caught_up:
            sel = status_df[status_df["sheet_name"] == sheet_name]
            for _, row in sel[~sel["is_behind"]].iterrows():
                wm = row["last_processed_date"]
                sm = row["sheet_max_tanggal"]
                if pd.notna(wm) and pd.notna(sm) and wm > sm:
                    suspicious.append({
                        "sheet_name": sheet_name,
                        "grain": row["grain"],
                        "watermark": wm,
                        "sheet_max": sm,
                        "in_future": bool(wm > today_ts),
                    })
        if suspicious:
            print("[GATE] NOTE: some caught-up tokos have a watermark AHEAD of their live sheet max:")
            for s in suspicious:
                flag = " (watermark date is in the FUTURE)" if s["in_future"] else ""
                print(
                    f"[GATE]   {s['sheet_name']}/{s['grain']}: "
                    f"watermark {s['watermark']:%Y-%m-%d} vs sheet max {s['sheet_max']:%Y-%m-%d}{flag}"
                )
            if any(s["in_future"] for s in suspicious):
                print("[GATE]   Fix the future-dated input upstream, then run:")
                print("[GATE]     python -m scripts.repair_watermark --apply")

        toko_detail = []
        for sheet_name in caught_up:
            sel = status_df[status_df["sheet_name"] == sheet_name]
            toko_without_new = sel[~sel["is_behind"]]["grain"].tolist()
            toko_detail.append({"sheet_name": sheet_name,
                                "toko_without_new_data": toko_without_new})

        emit(
            "GATE", "etl_gate",
            f"Sheets with no new data are required before continuing: {caught_up}",
            level="ERROR",
            metrics={
                "caught_up": caught_up,
                "behind": behind_sheets,
                "whitelisted_skipped": whitelisted_skipped,
                "caught_up_detail": toko_detail,
                "caught_up_suspicious": [
                    {**s, "watermark": s["watermark"].strftime("%Y-%m-%d"),
                     "sheet_max": s["sheet_max"].strftime("%Y-%m-%d")}
                    for s in suspicious
                ],
                "total_groups": int(len(status_df)),
                "sheets_up_to_date": len(caught_up),
                "sheets_with_new_data": len(behind_sheets),
            },
            source=SRC_GSHEET,
            target=SRC_MINIO,
        )
        subject, body_html = build_gate_abort_email(
            gate="2",
            mode=mode,
            message=(
                "Sheets with no new data are required before continuing: "
                f"{caught_up}"
            ),
            lists={
                "sheets_up_to_date": caught_up,
                "behind_sheets": behind_sheets,
                "whitelisted_skipped": whitelisted_skipped,
            },
            note=(
                "Fix the future-dated input upstream, then run: "
                "python -m scripts.repair_watermark --apply"
                if any(s["in_future"] for s in suspicious)
                else ""
            ),
            bq_updates=BQ_TARGETS,
            drift_rows=_build_drift_rows(status_df),
        )
        send_alert_email(subject, body_html, dry_run=dry_run)
        if log_folder:
            _write_wm_log(log_folder, run_key, status_df, sheet_passes,
                          f"ABORT - sheets with no new data: {caught_up}")
            _write_failure_log(log_folder, run_key,
                               f"sheets with no new data (already up-to-date): {caught_up}")
        return

    print("--- PRE-FLIGHT PASSED ---\n")

    # Write wm_monitor log
    if log_folder:
        pass_count = int(sheet_passes.sum())
        total = len(sheet_passes)
        verdict = f"PASS - {pass_count}/{total} sheets have >=1 toko behind"
        _write_wm_log(log_folder, run_key, status_df, sheet_passes, verdict)

    # 3) Per-sheet watermark check
    watermark_map, watermark_records = get_sheet_watermarks(
        minio_client, minio_bucket, WATERMARK_PATH
    )

    # 4) Ingest from GSheet (each sheet tagged with sheet_name)
    df_raw = fetch_gmv_max_live(gc)
    print(f"[INGEST] Rows raw from GSheet: {len(df_raw)}")
    emit("INGEST", "gsheet_ingester", f"Fetched {len(df_raw)} raw rows from GSheet",
         metrics={"rows_raw": int(len(df_raw))},
         source=SRC_GSHEET)

    # 4b) STEP 2: validate & normalize as early as possible (mixed-column
    #     detection + date-error capture). Runs exactly once, before anything else.
    
    # buang baris tanpa id_campaign
    df_raw = df_raw[df_raw["ID Campaign"].astype(str).str.strip() != ""]
    df_valid, df_error, v_report = validate_and_normalize_raw(
        df_raw, NUMERIC_COLS, percent_cols=PERCENT_COLS
    )
    print(
        f"[VALIDATE] Rows valid: {len(df_valid)} | bad rows: {v_report['n_bad_rows']} "
        f"(date errors: {v_report['n_date_errors']} | future date errors: {v_report.get('n_date_future',0)} | toko_blank: {v_report.get('n_toko_blank',0)}) | blank rows dropped: {v_report['n_blank_rows']}"
    )
    emit(
        "VALIDATE", "validator",
        f"Validated: {len(df_valid)} valid, {v_report['n_bad_rows']} bad",
        level="WARN" if v_report["n_bad_rows"] else "INFO",
        metrics={
            "rows_raw": int(len(df_raw)),
            "rows_valid": int(len(df_valid)),
            "n_bad_rows": int(v_report["n_bad_rows"]),
            "n_date_errors": int(v_report["n_date_errors"]),
            "n_date_future": int(v_report.get("n_date_future", 0)),
            "n_toko_blank": int(v_report.get("n_toko_blank", 0)),
            "n_blank_rows": int(v_report["n_blank_rows"]),
        },
        source=SRC_GSHEET,
        target=TGT_MINIO_QUARANTINE,
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
    resolved = sync_error_manifest(minio_client, minio_bucket, df_error, v_report, today_key, run_key, df_valid=df_valid, dry_run=dry_run)

    df_error_new = (
        filter_already_quarantined(minio_client, minio_bucket, df_error)
        if not df_error.empty
        else df_error
    )
    if not df_error_new.empty:
        if "error_reason" in df_error_new.columns:
            all_reasons = df_error_new["error_reason"].str.split("|").explode()
            reason_counts = all_reasons.value_counts()
            col_pattern = df_error_new["error_reason"].str.findall(r"date_unparsable\((\w+)=")
            affected_from_dates = set()
            for cols in col_pattern:
                affected_from_dates.update(cols)
            affected_cols = sorted(affected_from_dates | set(v_report.get("affected_columns", [])))
        else:
            reason_counts = None
            affected_cols = []

        if dry_run:
            print(f"[DRY-RUN] Akan quarantine {len(df_error_new)} bad row(s)")
        else:
            write_quarantine(minio_client, minio_bucket, df_error_new, today_key, run_key)

            # Write quarantine log with summary + sample bad rows
            if log_folder:
                import re as _re
                q_lines = []
                q_lines.append(f"=== QUARANTINE REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                q_lines.append("Dataset: gmv_max_live")
                q_lines.append(f"  Total quarantined rows: {len(df_error_new)}\n")

                if reason_counts is not None:
                    q_lines.append("  Error reasons breakdown:")
                    for reason, count in reason_counts.items():
                        q_lines.append(f"    {reason} : {count} rows")
                    q_lines.append("")

                    if affected_cols:
                        q_lines.append(f"  Affected columns: {affected_cols}\n")

                sample = df_error_new.head(5)
                q_lines.append(f"  Sample bad rows (first {len(sample)}):")
                display_cols = [c for c in ["Tanggal", "tanggal", "Toko", "toko",
                                             "ID Campaign", "id_campaign", "error_reason"] if c in sample.columns]
                if display_cols:
                    header = " | ".join(f"{c:<15}" for c in display_cols)
                    q_lines.append(f"    | {header} |")
                    q_lines.append(f"    | {'-' * len(header)} |")
                    for _, r in sample.iterrows():
                        vals = " | ".join(f"{str(r.get(c, '')):<15}" for c in display_cols)
                        q_lines.append(f"    | {vals} |")

                q_lines.append("")
                write_section_log(log_folder, f"quarantine_errors_{run_key}.log", "\n".join(q_lines) + "\n")

        emit(
            "QUARANTINE", "validator",
            f"{len(df_error_new)} bad row(s) quarantined",
            level="WARN",
            metrics={"n_quarantined": int(len(df_error_new))},
            source=SRC_GSHEET,
            target=TGT_MINIO_QUARANTINE,
        )
        subject, body_html = build_quarantine_email(
            len(df_error_new),
            reason_counts,
            affected_cols,
            sample_rows=[
                {c: r[c] for c in QUARANTINE_SAMPLE_COLUMNS if c in df_error_new.columns}
                for _, r in df_error_new.head(QUARANTINE_SAMPLE_ROWS).iterrows()
            ],
            minio_path=(
                f"{QUARANTINE_PREFIX}/date={today_key}/quarantine_{run_key}.parquet"
                if not dry_run
                else ""
            ),
            log_path=(
                os.path.join(log_folder, f"quarantine_errors_{run_key}.log")
                if log_folder
                else ""
            ),
            bq_updates=BQ_TARGETS,
        )
        send_alert_email(subject, body_html, dry_run=dry_run)

    # PATH A: recovered rows (fixed since last run) bypass the watermark.
    df_recovered = _select_recovered(df_valid, resolved, v_report)
    print(
        f"[RECOVERY] resolved={v_report.get('recovery_resolved', 0)} "
        f"| recovered_rows={v_report.get('recovery_recovered_rows', 0)} "
        f"| absent={v_report.get('recovery_absent', 0)} "
        f"| count_mismatch_skipped={v_report.get('recovery_count_mismatch', 0)}"
    )
    emit(
        "RECOVERY", "error_recovery",
        f"Recovery: resolved={v_report.get('recovery_resolved', 0)}, "
        f"recovered_rows={v_report.get('recovery_recovered_rows', 0)}, "
        f"absent={v_report.get('recovery_absent', 0)}, "
        f"count_mismatch_skipped={v_report.get('recovery_count_mismatch', 0)}",
        level="WARN" if v_report.get("recovery_count_mismatch", 0) or v_report.get("recovery_absent", 0) else "INFO",
        metrics={
            "resolved": int(v_report.get("recovery_resolved", 0)),
            "recovered_rows": int(v_report.get("recovery_recovered_rows", 0)),
            "absent": int(v_report.get("recovery_absent", 0)),
            "count_mismatch_skipped": int(v_report.get("recovery_count_mismatch", 0)),
        },
        source=SRC_GSHEET,
        target=TGT_BQ_BRONZE,
    )
    if v_report.get("recovery_recovered_rows", 0) or v_report.get("recovery_resolved", 0):
        subject, body_html = build_recovery_email(
            resolved=v_report.get("recovery_resolved", 0),
            recovered_rows=v_report.get("recovery_recovered_rows", 0),
            absent=v_report.get("recovery_absent", 0),
            count_mismatch_skipped=v_report.get("recovery_count_mismatch", 0),
            bq_updates=BQ_TARGETS,
        )
        send_alert_email(subject, body_html, dry_run=dry_run)

    # PATH B: remaining rows use the standard per-sheet watermark filter.
    df_regular = df_valid.drop(df_recovered.index)
    df_bronze_regular, sheet_max_dates = build_bronze_maxl(
        df_regular, sheet_watermarks=watermark_map
    )

    # PATH A transform: empty watermarks = full load, max dates fed into the
    # per-sheet watermark so recovered rows are not re-selected every run.
    if df_recovered.empty:
        df_bronze_recovered = df_bronze_regular.iloc[0:0]
    else:
        df_bronze_recovered, recovered_max_dates = build_bronze_maxl(
            df_recovered, sheet_watermarks={}
        )
        for key, max_date in recovered_max_dates.items():
            sheet_max_dates[key] = max(sheet_max_dates.get(key, max_date), max_date)

    # MERGE & DEDUPLICATE
    df_bronze = pd.concat(
        [df_bronze_regular, df_bronze_recovered], ignore_index=True
    ).drop_duplicates(subset=["row_hash_raw"])

    # Idempotency gate: drop rows whose content hash already exists in Bronze.
    # Boundary-day re-emissions and previously-recovered rows are re-selected by
    # the watermark each run; only genuinely new/changed rows should be appended.
    if not df_bronze.empty:
        existing_hashes = _fetch_existing_bronze_hashes(creds)
        if existing_hashes:
            before = len(df_bronze)
            df_bronze = df_bronze[
                ~df_bronze["row_hash_raw"].astype(str).isin(existing_hashes)
            ]
            skipped = before - len(df_bronze)
            if skipped:
                print(
                    f"[IDEMPOTENCY] Skipped {skipped} row(s) already present in bronze"
                )
                emit(
                    "BRONZE", "idempotency_gate",
                    f"Skipped {skipped} row(s) already present in bronze",
                    level="INFO",
                    metrics={"skipped": int(skipped)},
                    source=SRC_GSHEET,
                    target=TGT_BQ_BRONZE,
                )

    print(f"[BRONZE] Rows bronze to load: {len(df_bronze)}")
    emit("BRONZE", "bronze_builder", f"Rows bronze to load: {len(df_bronze)}",
         metrics={"rows_loaded": int(len(df_bronze))},
         source=SRC_GSHEET,
         target=TGT_BQ_BRONZE)

    # Nothing new to append: if rows were still selected (boundary-day /
    # recovered re-emissions) but every one already exists in Bronze, advance
    # the watermark anyway so they stop being re-selected every run.
    if df_bronze.empty and sheet_max_dates:
        if dry_run:
            changes = effective_watermark_changes(watermark_records, sheet_max_dates)
            for (sheet_key, sheet_name, toko), max_date in changes.items():
                print(f"[DRY-RUN]   watermark update ({sheet_key}, {sheet_name}, {toko}) -> {max_date}")
            if changes:
                print("[DRY-RUN] Akan update watermark (tanpa upload parquet).")
            else:
                print("[DRY-RUN] Tidak ada perubahan watermark (nilai sudah sama).")
            emit("FINISH", "etl_pipeline", "ETL DONE (DRY-RUN) - watermark only",
                 metrics={"watermark_changes": len(changes)},
                 source=SRC_GSHEET, target=SRC_MINIO)
            _finish(
                watermark_records,
                "== ETL GMV Max DONE (DRY-RUN) ==",
                status="dry-run — watermark only",
                dry_run=dry_run,
                run_key=run_key,
                watermark_updates=sheet_max_dates,
                bq_updates=_bq_noop(),
            )
            return
        update_sheet_watermarks(
            minio_client, minio_bucket, WATERMARK_PATH, watermark_records,
            sheet_max_dates,
        )
        print("[MINIO] Watermark advanced (no new rows to append).")
        emit("FINISH", "etl_pipeline", "ETL DONE - watermark advanced (no new rows)",
             metrics={"watermark_changes": len(sheet_max_dates)},
             source=SRC_GSHEET, target=SRC_MINIO)
        _finish(
            watermark_records,
            "== ETL GMV Max DONE ==",
            status="no new rows — watermark advanced",
            dry_run=dry_run,
            run_key=run_key,
            watermark_updates=sheet_max_dates,
            bq_updates=_bq_noop(),
        )
        return

    if df_bronze.empty:
        print("[MINIO] No new data to process. Data is up-to-date.")
        emit("FINISH", "etl_pipeline", "ETL DONE - no new data to process",
             metrics={"rows_loaded": 0},
             source=SRC_GSHEET, target=SRC_MINIO)
        _finish(
            watermark_records,
            "== ETL GMV Max DONE ==",
            status="data is up to date — no new rows",
            dry_run=dry_run,
            run_key=run_key,
            bq_updates=_bq_noop(),
        )
        return

    # 6) Parquet conversion & Load to MinIO
    file_path = f"gmv/max/date={today_key}/max_{run_key}.parquet"
    folder_path = f"gmv/max/date={today_key}/"

    if dry_run:
        print(f"[DRY-RUN] Akan upload {len(df_bronze)} baris ke '{file_path}'")
        changes = effective_watermark_changes(watermark_records, sheet_max_dates)
        for (sheet_key, sheet_name, toko), max_date in changes.items():
            print(f"[DRY-RUN]   watermark update ({sheet_key}, {sheet_name}, {toko}) -> {max_date}")
        if not changes:
            print("[DRY-RUN] Tidak ada perubahan watermark (nilai sudah sama).")
        print("[DRY-RUN] Akan: append ke Testing.bronze_maxl + MERGE ke silver_tt_ads_gmvmax")
        print("[DRY-RUN] Selesai. TIDAK ada data yang ditulis (dry-run).")
        emit("FINISH", "etl_pipeline",
             f"ETL DONE (DRY-RUN) - would load {len(df_bronze)} rows",
             metrics={"rows_loaded": int(len(df_bronze)),
                      "watermark_changes": len(changes)},
             source=SRC_GSHEET, target=TGT_BQ_BRONZE)
        _finish(
            watermark_records,
            "== ETL GMV Max Live DONE (DRY-RUN) ==",
            status="dry-run — full load (would run)",
            dry_run=dry_run,
            run_key=run_key,
            watermark_updates=changes,
            bq_updates=_bq_full_load(len(df_bronze)),
        )
        return

    # Folder partition marker
    minio_client.put_object(minio_bucket, folder_path, io.BytesIO(b""), length=0)

    # Convert & Upload Parquet
    try:
        parquet_bytes = df_bronze.to_parquet(index=False, engine="pyarrow")
        minio_client.put_object(
            minio_bucket,
            file_path,
            io.BytesIO(parquet_bytes),
            length=len(parquet_bytes),
            content_type="application/octet-stream",
        )
    except Exception as e:
        emit(
            "LOAD", "minio_loader",
            f"Parquet upload failed: {type(e).__name__} {e}",
            level="ERROR",
            metrics={"rows": int(len(df_bronze)), "target_path": file_path},
            source=SRC_GSHEET,
            target=TGT_MINIO,
        )
        if log_folder:
            err_lines = [
                f"=== BRONZE/PARQUET ERROR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                f"\nDataset: gmv_max_live",
                f"  Stage: Parquet upload",
                f"  Target path: {file_path}",
                f"  Rows: {len(df_bronze)}",
                f"  Error: {type(e).__name__} {e}\n",
                f"  Traceback:\n{traceback.format_exc()}",
            ]
            write_section_log(log_folder, f"bronze_parquet_errors_{run_key}.log", "\n".join(err_lines) + "\n")
        _failure_ctx.update({
            "stage": "MinIO parquet upload",
            "minio_files": [folder_path, file_path],
            "rollback_hint": (
                "Watermark is NOT yet updated and BigQuery is untouched. "
                "A partial parquet may exist in MinIO; a re-run is safe "
                "and needs no rollback."
            ),
        })
        raise
    print(f"[MINIO] Successfully uploaded Parquet file to: {file_path}")
    emit("LOAD", "minio_loader", f"Parquet uploaded to {file_path}",
         metrics={"rows": int(len(df_bronze)), "target_path": file_path},
         source=SRC_GSHEET, target=TGT_MINIO)

    # 7) Update per-sheet watermark
    update_sheet_watermarks(
        minio_client, minio_bucket, WATERMARK_PATH, watermark_records, sheet_max_dates,
    )

    # 8) Load : Bronze
    try:
        load_df(
            df_bronze,
            table_id="Testing.bronze_maxl",
            project_id=PROJECT_ID,
            if_exists="append",
            credentials=creds,
        )
    except Exception as e:
        emit(
            "LOAD", "bigquery_loader",
            f"BigQuery load failed: {type(e).__name__} {e}",
            level="ERROR",
            metrics={
                "rows_loaded": int(len(df_bronze)),
                "table": f"{PROJECT_ID}.Testing.bronze_maxl",
                "if_exists": "append",
            },
            source=SRC_MINIO,
            target=TGT_BQ_BRONZE,
        )
        if log_folder:
            err_lines = [
                f"=== BRONZE/PARQUET ERROR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                f"\nDataset: gmv_max_live",
                f"  Stage: BigQuery load",
                f"  Target table: Testing.bronze_maxl",
                f"  Rows being loaded: {len(df_bronze)}",
                f"  Error: {type(e).__name__} {e}\n",
                f"  Traceback:\n{traceback.format_exc()}",
            ]
            write_section_log(log_folder, f"bronze_parquet_errors_{run_key}.log", "\n".join(err_lines) + "\n")
        # AUTO ROLLBACK: the watermark already advanced past rows that never hit
        # bronze. Restore it (watermark scope only) so a re-run re-selects them.
        rollback_note = ""
        toggle = os.getenv("ETL_AUTO_ROLLBACK_WATERMARK", "true").strip().lower()
        if toggle in {"1", "true", "yes", "y"}:
            try:
                from src.gmv_max_live.utils.minio_rollback import auto_restore_watermark

                rollback_note = auto_restore_watermark(minio_client, minio_bucket, run_key)
                print(f"[ROLLBACK] {rollback_note}")
                emit("ROLLBACK", "minio_rollback", rollback_note,
                     level="WARN" if "DONE" in rollback_note else "INFO",
                     metrics={"run_key": run_key},
                     source=SRC_MINIO, target=SRC_MINIO)
            except Exception as rb_exc:
                rollback_note = f"AUTO ROLLBACK ERROR: {type(rb_exc).__name__}: {rb_exc}"
                print(f"[ROLLBACK] {rollback_note}")
                emit("ROLLBACK", "minio_rollback", rollback_note,
                     level="ERROR", metrics={"run_key": run_key},
                     source=SRC_MINIO, target=SRC_MINIO)
        _failure_ctx.update({
            "stage": "BigQuery bronze load (append)",
            "minio_files": [file_path, WATERMARK_PATH],
            "rollback_hint": (
                "MinIO parquet written AND the watermark already advanced, but the "
                "bronze append FAILED. Rows past the watermark are not re-read by a "
                "plain re-run - roll the MinIO watermark back before re-running, or "
                "those rows will be skipped in BigQuery."
            ),
            "rollback_command": (
                "python -m src.gmv_max_live.utils.minio_rollback "
                f"--run-key {run_key} --scope full --dry-run"
            ),
            "auto_rollback_note": rollback_note,
        })
        raise
    print("[BRONZE] Load to Testing.bronze_maxl DONE")
    emit("LOAD", "bigquery_loader", "Bronze load to Testing.bronze_maxl DONE",
         metrics={"rows_loaded": int(len(df_bronze)),
                  "table": f"{PROJECT_ID}.Testing.bronze_maxl"},
         source=SRC_MINIO, target=TGT_BQ_BRONZE)

    # 9) Merge : Silver
    print("[SILVER] Running MERGE into Testing.silver_tt_ads_gmvmax_live ...")
    try:
        merge_to_silver()
    except Exception as e:
        emit(
            "SILVER", "silver_merger",
            f"Silver MERGE failed: {type(e).__name__} {e}",
            level="ERROR",
            metrics={"table": f"{PROJECT_ID}.Testing.silver_tt_ads_gmvmax_live"},
            source=TGT_BQ_BRONZE,
            target=TGT_BQ_SILVER,
        )
        if log_folder:
            err_lines = [
                f"=== SILVER/GOLD ERROR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                f"\nStage: Silver MERGE",
                f"  Table: Testing.silver_tt_ads_gmvmax_live",
                f"  Error: {type(e).__name__} {e}\n",
                f"  Traceback:\n{traceback.format_exc()}",
            ]
            write_section_log(log_folder, f"silver_gold_errors_{run_key}.log", "\n".join(err_lines) + "\n")
        _failure_ctx.update({
            "stage": "BigQuery silver MERGE",
            "minio_files": [file_path, WATERMARK_PATH],
            "rollback_hint": (
                "MinIO archive + bronze append succeeded; only the silver MERGE failed. "
                "The silver upsert is idempotent over bronze, so a plain re-run is safe "
                "without any MinIO rollback."
            ),
        })
        raise
    print("[SILVER] MERGE DONE")
    emit("SILVER", "silver_merger", "Silver MERGE into Testing.silver_tt_ads_gmvmax_live DONE",
         metrics={"table": f"{PROJECT_ID}.Testing.silver_tt_ads_gmvmax_live"},
         source=TGT_BQ_BRONZE, target=TGT_BQ_SILVER)

    emit("FINISH", "etl_pipeline", "ETL GMV Max Live DONE",
         metrics={"rows_loaded": int(len(df_bronze))},
         source=SRC_GSHEET, target=TGT_BQ_SILVER)
    _finish(
        watermark_records,
        "== ETL GMV Max Live DONE ==",
        status="full load finished — Silver MERGE done",
        dry_run=dry_run,
        run_key=run_key,
        watermark_updates=sheet_max_dates,
        bq_updates=_bq_full_load(len(df_bronze)),
    )


# Kalau kamu mau bisa juga di-run langsung:
if __name__ == "__main__":
    run_daily_etl()
