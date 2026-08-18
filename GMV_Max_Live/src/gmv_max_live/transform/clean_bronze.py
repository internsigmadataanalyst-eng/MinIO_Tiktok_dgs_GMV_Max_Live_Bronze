# src/gmv_max_live/transform/clean_bronze.py
import uuid
import hashlib
from datetime import datetime, timezone

import pandas as pd

from src.gmv_max_live.utils.transform_utils import (
    clean_numeric_columns,
    parse_mixed_dates,
    to_snake_case,
)
from src.gmv_max_live.utils.minio_client import filter_by_sheet_watermark

# NUMERIC_COLS = [
#     "Biaya",
#     "Biaya Bersih",
#     "Pesanan (SKU)",
#     "Pesanan (Toko saat ini)",
#     "Biaya per pesanan (Toko saat ini)",
#     "Pendapatan kotor",
#     "Penghasilan bruto (Toko saat ini)",
#     "ROI (Toko saat ini)",
#     "Tayangan LIVE",
#     "Biaya per tayangan LIVE",
#     "Tayangan LIVE 10 detik",
#     "Biaya per tayangan LIVE 10 detik",
#     "Pengikut saat LIVE",
# ]


def _canon(x):
    import pandas as pd

    x = "" if pd.isna(x) else str(x).strip()
    return x.upper()


def build_bronze_maxl(
    gmv_max_live_raw: pd.DataFrame, sheet_watermarks: dict | None = None
) -> tuple[pd.DataFrame, dict]:
    """
    Dari raw GSheet → cleaning numeric + tanggal + snake_case,
    tambah snapshot_ts, snapshot_date, run_id, row_hash_raw.
    Filter incremental per sheet_name berdasarkan watermark (sheet_watermarks).
    Output: (df siap di-load ke BRONZE_DB.bronze_gmv_max_live, sheet_max_dates)
    """
    # # numeric cleaning
    # tiktok_maxl_clean = clean_numeric_columns(
    #     gmv_max_live_raw, NUMERIC_COLS, fillna_value=0
    # )

    tiktok_maxl_clean = gmv_max_live_raw.copy()

    # parse tanggal
    tiktok_maxl_clean["Tanggal"] = parse_mixed_dates(
        tiktok_maxl_clean["Tanggal"], return_date=False
    )
    tiktok_maxl_clean["Waktu peluncuran"] = parse_mixed_dates(
        tiktok_maxl_clean["Waktu peluncuran"], return_date=False
    )

    # copy & snake_case
    df = tiktok_maxl_clean.copy()
    df.columns = df.columns.map(to_snake_case)

    # buang baris tanpa id_campaign
    df = df[df["id_campaign"].astype(str).str.strip() != ""]

    # snapshot fields
    now_utc = datetime.now(timezone.utc)
    df["snapshot_ts"] = now_utc
    df["snapshot_date"] = now_utc.date()
    df["run_id"] = str(uuid.uuid4())

    # row_hash_raw: sesuai scriptmu
    cols_for_hash = ["tanggal","toko","id_campaign","waktu_peluncuran","biaya","pendapatan_kotor","tayangan_live"]

    df["row_hash_raw"] = (
        df[cols_for_hash]
        .map(_canon)
        .astype(str)
        .agg("||".join, axis=1)
        .apply(lambda s: hashlib.sha256(s.encode()).hexdigest())
    )

    df = df.loc[:, df.columns.astype(bool)]

    columns_to_int_bq = [
        "biaya",
        "biaya_bersih",
        "pesanan_sku",
        "pesanan_toko_saat_ini",
        "biaya_per_pesanan_toko_saat_ini",
        "pendapatan_kotor",
        "penghasilan_bruto_toko_saat_ini",
        "roi_toko_saat_ini",
        "tayangan_live",
        "biaya_per_tayangan_live",
        "tayangan_live_10_detik",
        "biaya_per_tayangan_live_10_detik",
        "pengikut_saat_live",
    ]

    for col in columns_to_int_bq:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(df[col], errors='coerce')
                .fillna(0)
                .apply(lambda x: int(round(x)))
                .astype(pd.Int64Dtype())
            )

    # Filter incremental per sheet (creds-keyed) berdasarkan watermark
    if "creds" in df.columns:
        df, sheet_max_dates = filter_by_sheet_watermark(
            df, "creds", "tanggal", sheet_watermarks or {}
        )
    else:
        sheet_max_dates = {}

    # NOTE: creds & sheet_name sengaja DIPERTAHANKAN di level bronze.
    return df, sheet_max_dates