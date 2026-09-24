# src/gmv_max_live/utils/recovery.py
"""Error-recovery path-A logic: select rows from resolved errors."""

import pandas as pd


def select_recovered(
    df_valid: pd.DataFrame,
    resolved: list,
    report: dict,
) -> pd.DataFrame:
    """Select rows from *df_valid* that were recovered from a resolved error.

    Grain is (sheet_name, creds, toko, error_date) — toko verbatim.  A resolved
    entry means the key was in the error manifest last run but is NO LONGER in
    df_error this run (the data got fixed).  Those rows bypass the watermark
    filter downstream.

    Full recovery only: we include the key's rows ONLY when the number of
    valid rows now equals the manifest ``n_rows``.  Otherwise the group is
    either only partially fixed (some rows still bad -> entry stays open) or
    extra rows appeared on that historical date.

    Counters are added to ``report``:
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
            absent += 1
        elif len(grp) == n_expected:
            match.loc[grp] = True
        else:
            count_mismatch += 1

    report["recovery_resolved"] = len(resolved)
    report["recovery_recovered_rows"] = int(match.sum())
    report["recovery_count_mismatch"] = count_mismatch
    report["recovery_absent"] = absent

    return df[match]
