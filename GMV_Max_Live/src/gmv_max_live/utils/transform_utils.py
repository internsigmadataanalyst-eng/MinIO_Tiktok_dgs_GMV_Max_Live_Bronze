# src/gmv_max_produk/utils/transform_utils.py
import re
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Union

EXCEL_EPOCH = pd.Timestamp("1899-12-30")

NUMERIC_COLS = [
    "Biaya",
    "Biaya Bersih",
    "Pesanan (SKU)",
    "Pesanan (Toko saat ini)",
    "Biaya per pesanan (Toko saat ini)",
    "Pendapatan kotor",
    "Penghasilan bruto (Toko saat ini)",
    "ROI (Toko saat ini)",
    "Tayangan LIVE",
    "Biaya per tayangan LIVE",
    "Tayangan LIVE 10 detik",
    "Biaya per tayangan LIVE 10 detik",
    "Pengikut saat LIVE",
]

PERCENT_COLS = [
]

# A valid numeric cell may only contain digits / '.' / ',' / 'Rp' / whitespace /
# '#' / '-' (both are censored markers: '######' or '-' mean the value is
# hidden / could not be displayed, e.g. a too-narrow column, not a real error).
# Anything else (e.g. '%', letters) means the cell does not belong to a
# count/currency column (typically a shifted value from another column).
_NUMERIC_FORBIDDEN = re.compile(r"[^0-9.,Rp\s#-]")

# A valid rate/percentage cell may only contain digits / '.' / ',' / '%' /
# whitespace / '#' / '-'. Plain '0' or '0,00%' is valid; pure text (e.g.
# 'IDR') is not, while '######' / '-' are treated as censored data, not errors.
_PERCENT_FORBIDDEN = re.compile(r"[^0-9.,%\s#-]")

def to_snake_case(column_name: str) -> str:
    return (
        column_name.lower()
        .strip()
        .replace(" ", "_")
        .replace(".", "")
        .replace("(", "")
        .replace(")", "")
        .replace("%", "")
        .replace("/", "")
        .replace("-", "")
    )


def _coerce_numeric_series(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Cleans a raw string series into numerics using the exact same steps as
    clean_numeric_columns. Returns (numeric_values, cleaned_string_series)."""
    s = series.astype(str).str.strip()
    s = s.replace("-", np.nan)
    s = s.str.replace(r"[^\d,\.]", "", regex=True)
    s = s.str.replace(".", "", regex=False)
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce"), s


def clean_numeric_columns(df: pd.DataFrame, cols, fillna_value=0) -> pd.DataFrame:
    df = df.copy()

    for col in cols:
        if col not in df.columns:
            print(f"Kolom '{col}' tidak ditemukan di DataFrame. Lewati Nggih.")
            continue

        df[col] = df[col].astype(str)
        df[col] = df[col].replace("-", np.nan)
        df[col] = df[col].str.replace(r"[^\d,\.]", "", regex=True)
        df[col] = df[col].str.replace(".", "", regex=False)
        df[col] = df[col].str.replace(",", ".", regex=False)
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].fillna(fillna_value)

        if (df[col] % 1 == 0).all():
            df[col] = df[col].astype(int)

    return df


def detect_blank_rows(
    df: pd.DataFrame, ignore_cols: List[str] = None
) -> pd.Series:
    """Flags rows where every non-tag column is blank.

    Tag/metadata columns (creds, sheet_name, error_reason, ...) are excluded
    from the blank check so fully-empty source rows are detected even though
    their tags are populated.
    """
    if ignore_cols is None:
        ignore_cols = ["creds", "sheet_name", "error_reason"]

    data_cols = [c for c in df.columns if c not in ignore_cols]
    if not data_cols:
        return pd.Series(False, index=df.index)

    def _is_blank(v: Any) -> bool:
        if pd.isna(v):
            return True
        return str(v).strip().lower() in ("", "-", "nan", "none", "nat")

    sub = df[data_cols]
    blank = pd.DataFrame(
        {c: sub[c].map(_is_blank) for c in sub.columns}, index=sub.index
    )
    return blank.all(axis=1)


def _is_non_empty(raw: pd.Series) -> pd.Series:
    """Mask of cells holding real content (excludes blank / dash / nan variants)."""
    return raw.notna() & ~raw.str.lower().isin(["", "nan", "none", "-", "n/a"])


def detect_numeric_corruption(
    df: pd.DataFrame,
    numeric_cols,
    percent_cols=None,
    date_col: str = "Tanggal",
) -> dict:
    """Row-level mixed-column detection.

    - numeric columns: a cell is corrupt when it holds content and either
      (a) contains any character other than digits / '.' / ',' / 'Rp' /
      whitespace (e.g. '%', letters, '######' -- a shifted value), or
      (b) still fails numeric coercion after the lossy clean (e.g. pure
      punctuation that strips to nothing).
    - percent columns (optional): a cell is corrupt when it holds content and
      contains any character other than digits / '.' / ',' / '%' / whitespace
      (e.g. 'IDR', '######', text). Plain '0', '125' and '0,00%' stay valid.

    Returns a report with the affected rows mask and date context.
    """
    df = df.copy()
    affected_mask = pd.Series(False, index=df.index)
    corrupted_cols = []

    for col in numeric_cols:
        if col not in df.columns:
            continue

        raw = df[col].astype(str).str.strip()
        values, _ = _coerce_numeric_series(raw)

        is_not_empty = _is_non_empty(raw)
        is_forbidden = raw.str.contains(_NUMERIC_FORBIDDEN, na=False)
        is_corrupted = is_not_empty & (is_forbidden | values.isna())

        if is_corrupted.any():
            corrupted_cols.append(col)
            affected_mask = affected_mask | is_corrupted

    for col in (percent_cols or []):
        if col not in df.columns:
            continue

        raw = df[col].astype(str).str.strip()

        is_not_empty = _is_non_empty(raw)
        is_forbidden = raw.str.contains(_PERCENT_FORBIDDEN, na=False)
        is_corrupted = is_not_empty & is_forbidden

        if is_corrupted.any():
            corrupted_cols.append(col)
            affected_mask = affected_mask | is_corrupted

    report = {
        "has_changes": bool(affected_mask.any()),
        "affected_columns": corrupted_cols,
        "affected_mask": affected_mask,
        "n_bad_rows": int(affected_mask.sum()),
        "affected_dates": [],
        "first_affected_date": None,
        "last_affected_date": None,
    }

    if affected_mask.any() and date_col in df.columns:
        dates = (
            parse_mixed_dates(df.loc[affected_mask, date_col], return_date=False)
            .dropna()
            .sort_values()
            .unique()
            .tolist()
        )
        report["affected_dates"] = dates
        report["first_affected_date"] = dates[0] if dates else None
        report["last_affected_date"] = dates[-1] if dates else None

    return report


def validate_and_normalize_raw(
    df: pd.DataFrame, numeric_cols, date_col: str = "Tanggal", percent_cols: list = None
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """STEP 2: row-level validation & normalization.

    - Detects mixed/corrupted cells in numeric columns (main filter) and, when
      given, in percentage/rate columns (percent_cols).
    - Parses mixed dates; rows with NaT are captured as date-error rows.
    - Splits into (df_valid, df_error, report). df_error keeps the ORIGINAL raw
      values plus an error_reason column; df_valid is cleaned for downstream.
    """
    df = df.copy()

    blank_mask = detect_blank_rows(df)

    corruption = detect_numeric_corruption(
        df, numeric_cols, percent_cols=percent_cols, date_col=date_col
    )
    df_clean = clean_numeric_columns(df, numeric_cols, fillna_value=0)

    parsed = parse_mixed_dates(df_clean[date_col], return_date=False)
    df_clean[date_col] = parsed
    date_error = parsed.isna()

    error_mask = (corruption["affected_mask"] | date_error) & ~blank_mask

    df_error = df[error_mask].copy()
    reasons = []
    for idx in df_error.index:
        reason_parts = []
        if corruption["affected_mask"].loc[idx]:
            reason_parts.append("numeric_mixed")
        if date_error.loc[idx]:
            reason_parts.append("date_unparsable")
        reasons.append("|".join(reason_parts))
    df_error["error_reason"] = reasons

    df_valid = df_clean[~(error_mask | blank_mask)].copy()

    report = {
        "has_changes": bool(error_mask.any()),
        "affected_columns": corruption["affected_columns"],
        "first_affected_date": corruption["first_affected_date"],
        "last_affected_date": corruption["last_affected_date"],
        "affected_dates": corruption["affected_dates"],
        "n_bad_rows": int(error_mask.sum()),
        "n_date_errors": int(date_error.sum()),
        "n_blank_rows": int(blank_mask.sum()),
    }

    return df_valid, df_error, report


def parse_mixed_dates(series: pd.Series, return_date=True) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"": np.nan, "-": np.nan, "nan": np.nan, "None": np.nan})

    s_norm = s.str.replace(r"[-\.]", "/", regex=True)

    mask_ymd = s_norm.str.match(r"^\s*\d{4}/\d{1,2}/\d{1,2}\s*$", na=False)
    ymd = pd.to_datetime(s_norm.where(mask_ymd), format="%Y/%m/%d", errors="coerce")

    mask_dmy4 = s_norm.str.match(r"^\s*\d{1,2}/\d{1,2}/\d{4}\s*$", na=False)
    dmy4 = pd.to_datetime(s_norm.where(mask_dmy4), format="%d/%m/%Y", errors="coerce")

    mask_dmy2 = s_norm.str.match(r"^\s*\d{1,2}/\d{1,2}/\d{2}\s*$", na=False)
    dmy2 = pd.to_datetime(s_norm.where(mask_dmy2), format="%d/%m/%y", errors="coerce")

    iso_generic = pd.to_datetime(s, errors="coerce", format=None)

    mask_serial = s.str.match(r"^\d{3,6}$", na=False)
    serial_vals = pd.to_numeric(s.where(mask_serial), errors="coerce")
    serial = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    serial.loc[mask_serial] = EXCEL_EPOCH + pd.to_timedelta(
        serial_vals.loc[mask_serial], unit="D"
    )

    parsed = (
        ymd.combine_first(dmy4)
        .combine_first(dmy2)
        .combine_first(serial)
        .combine_first(iso_generic)
    )

    if return_date:
        return parsed.dt.date

    return parsed