# src/gmv_max_live/pipelines/config.py
"""Shared constants and mutable pipeline state for the GMV Max Live ETL."""

import os

from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "database-sigma"
WATERMARK_PATH = "watermarks/gmv_max.json"

SRC_GSHEET = {"system": "Google_Sheets", "entity": "GMV Max Live"}
SRC_MINIO = {"system": "MinIO", "entity": WATERMARK_PATH}
TGT_MINIO = {"system": "MinIO", "entity": "gmv/max"}
TGT_MINIO_QUARANTINE = {"system": "MinIO", "entity": "quarantine/gmv_max_live/"}
TGT_BQ_BRONZE = {"system": "BigQuery", "entity": f"{PROJECT_ID}.Testing.bronze_maxl"}
TGT_BQ_SILVER = {"system": "BigQuery", "entity": f"{PROJECT_ID}.Testing.silver_tt_ads_gmvmax_live"}

WHITELIST_SHEETS = set(
    s.strip() for s in os.getenv("WHITELIST_SHEETS", "deni_etawa,riwa_ajwa,ian").split(",") if s.strip()
)

# How long an open quarantine entry may keep the pre-flight gate open so PATH A
# can re-admit recovered rows. After this, the entry stops satisfying the gate and
# is reported in the gate-abort email instead (it is never silently forgotten).
QUARANTINE_RECOVERY_MAX_AGE_DAYS = int(
    os.getenv("QUARANTINE_RECOVERY_MAX_AGE_DAYS", "14")
)

BQ_TARGETS = [
    {"table": TGT_BQ_BRONZE["entity"], "action": "append"},
    {"table": TGT_BQ_SILVER["entity"], "action": "MERGE (silver upsert)"},
]

_failure_ctx = {
    "stage": "",
    "minio_files": [],
    "rollback_hint": "",
}


def bq_full_load(rows: int) -> list[dict]:
    """BQ-update summary for a run that appended rows to bronze + merged silver."""
    return [
        {"table": TGT_BQ_BRONZE["entity"], "action": "append", "rows": rows},
        {"table": TGT_BQ_SILVER["entity"], "action": "MERGE (silver upsert)"},
    ]


def bq_noop() -> list[dict]:
    """BQ-update summary for a successful run with no new rows to load."""
    return [
        {"table": TGT_BQ_BRONZE["entity"], "action": "no change", "rows": 0},
        {"table": TGT_BQ_SILVER["entity"], "action": "no change"},
    ]


def get_credentials():
    from google.oauth2 import service_account

    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)
