# src/data_live/utils/bronze_compare.py
"""Read-and-compare the original vs testing bronze tables for the last N days,
then show the current MinIO watermark.

Ported from Pesanan_Affiliasi and parameterized per project so it works for the
sibling ETLs. Delivered as a copied module per repository.

Highlights:
- `effective_watermark_changes` : only reports watermark updates whose candidate
  max-date actually differs from the stored value (avoids fake 'watermark update'
  lines in dry-run when nothing changes).
- `finish` : the final step on every exit path. When `compare=False` (a project
  with no original bronze table, e.g. GMV_Max_Live) it only shows the watermark.
"""
import pandas as pd

PROJECT_ID = "database-sigma"

# Business-key columns used to dedup (ROW_NUMBER) a bronze table's order/line
# items within the window. Kept latest snapshot per key. Varies per project,
# so set it in each per-repo copy.
DEDUP_COLS = ["toko", "tanggal", "id_campaign"]


def read_bronze_last_days(
    creds, table_id: str, days: int = 15, project_id: str = PROJECT_ID,
    date_col: str = "tanggal", dedup_cols=None,
) -> pd.DataFrame:
    """Reads the last `days` days (incl. today) from the given bronze table.

    Deduplicates per order-item keeping the latest snapshot (rn = 1), then returns
    the rows as a DataFrame. Window is dynamic: DATE_SUB(CURRENT_DATE(), days-1)
    .. CURRENT_DATE(). Dedup partition is the unique business key columns.
    """
    from pandas_gbq import read_gbq

    dedup_cols = dedup_cols or list(DEDUP_COLS)
    partition = ", ".join(dedup_cols)

    query = f"""
    SELECT * EXCEPT(rn)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY {partition}
                ORDER BY snapshot_ts DESC
            ) AS rn
        FROM `{project_id}.{table_id}`
        WHERE DATE({date_col})
              BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {days - 1} DAY)
                  AND CURRENT_DATE()
    )
    WHERE rn = 1
    """
    return read_gbq(
        query,
        project_id=project_id,
        credentials=creds,
        dialect="standard",
    )


def effective_watermark_changes(
    watermark_records: list, sheet_max_dates: dict
) -> dict:
    """Returns only the (creds, sheet_name, toko) -> max_date entries whose
    candidate max_date actually DIFFERS from the currently-stored watermark.

    Groups with no stored watermark yet (new groups) are included. This avoids
    reporting a 'watermark update' when nothing would change.
    """
    stored = {}
    for rec in watermark_records or []:
        creds = str(rec.get("creds") or "")
        sheet_name = str(rec.get("sheet_name") or "")
        toko = str(rec.get("toko") or "")
        date_val = str(rec.get("last_processed_date") or rec.get("last_update") or "").strip()[:10]
        stored[(creds, sheet_name, toko)] = date_val

    changes = {}
    for (creds, sheet_name, toko), max_date in sheet_max_dates.items():
        key = (str(creds), str(sheet_name or ""), str(toko or ""))
        if max(stored.get(key, ""), "").strip() == str(max_date).strip()[:10]:
            continue
        changes[key] = str(max_date)
    return changes


def _per_group_max(df: pd.DataFrame, group_col: str, date_col: str) -> pd.DataFrame:
    """Returns per-`group_col` max of `date_col` for a deduped bronze frame.

    If `group_col` is falsy or absent from the dataframe, returns a single-row
    overall max instead (used when a dataset has no toko-like grouping column).
    """
    has_group = bool(group_col) and group_col in df.columns
    sub = df[[group_col, date_col]].copy() if has_group else df[[date_col]].copy()
    if has_group:
        sub[group_col] = sub[group_col].astype(str).str.upper().str.strip()
    sub[date_col] = pd.to_datetime(sub[date_col], errors="coerce")
    sub = sub.dropna(subset=[date_col])
    if sub.empty:
        return pd.DataFrame(columns=["group", "max_date"])
    if not has_group:
        val = pd.to_datetime(sub[date_col]).max()
        return pd.DataFrame({"group": ["(overall)"], "max_date": [val.date().isoformat()]})
    return (
        sub.groupby(group_col)[date_col]
        .max()
        .dt.date
        .astype(str)
        .reset_index()
        .rename(columns={group_col: "group", date_col: "max_date"})
        .sort_values("group")
    )


def compare_and_show_watermark(
    creds,
    watermark_records: list,
    *,
    original_tables=(),
    testing_tables=(),
    date_col: str = "tanggal",
    group_col: str = "toko",
    dedup_cols=None,
    days: int = 15,
    project_id: str = PROJECT_ID,
):
    """Compares original vs testing bronze (last `days` days) per group.

    Shows a side-by-side per-group summary: group | max_date_original | max_date_testing
    so date gaps per group are visible without printing the full rows. When
    `original_tables` is empty this is a single-table view (max_date_testing only).
    Then prints the full MinIO watermark records.
    """
    if not original_tables:
        test_dfs = [read_bronze_last_days(creds, t, days=days, project_id=project_id, date_col=date_col, dedup_cols=dedup_cols)
                    for t in (testing_tables or ())]
        test_n = sum(len(d) for d in test_dfs)
        print(f"\n[COMPARE] Bronze last {days} days (single table): testing rows={test_n:,}")
        if not test_dfs:
            print("[COMPARE] (tidak ada tabel bronze untuk dibandingkan)")
            _show_watermark(watermark_records)
            return
        test_df = pd.concat(test_dfs, ignore_index=True)
        t_max = _per_group_max(test_df, group_col, date_col).rename(
            columns={"max_date": "max_date_testing"}
        )
        print("[COMPARE] Max Date (testing):")
        print(t_max.to_string(index=False))
        _show_watermark(watermark_records)
        return

    orig_dfs = [read_bronze_last_days(creds, t, days=days, project_id=project_id, date_col=date_col, dedup_cols=dedup_cols)
                for t in original_tables]
    test_dfs = [read_bronze_last_days(creds, t, days=days, project_id=project_id, date_col=date_col, dedup_cols=dedup_cols)
                for t in testing_tables]
    orig = pd.concat(orig_dfs, ignore_index=True)
    test_ = pd.concat(test_dfs, ignore_index=True)

    print(f"\n[COMPARE] Bronze last {days} days: original rows={len(orig):,} | "
          f"testing rows={len(test_):,}")

    grouped = group_col and group_col in orig.columns and group_col in test_.columns

    if grouped:
        o_max = _per_group_max(orig, group_col, date_col).rename(
            columns={"max_date": "max_date_original"}
        )
        t_max = _per_group_max(test_, group_col, date_col).rename(
            columns={"max_date": "max_date_testing"}
        )
        summary = o_max.merge(t_max, on="group", how="outer", indicator=True).sort_values("group")
        print("[COMPARE] Per-group Max Date (original vs testing):")
        print(summary[["group", "max_date_original", "max_date_testing"]].to_string(index=False))
    else:
        o_val = _per_group_max(orig, None, date_col)
        t_val = _per_group_max(test_, None, date_col)
        print("[COMPARE] Max Date (no grouping column; overall):")
        row = pd.DataFrame(
            [{
                "max_date_original": o_val["max_date"].iloc[0] if not o_val.empty else "",
                "max_date_testing": t_val["max_date"].iloc[0] if not t_val.empty else "",
            }]
        )
        print(row.to_string(index=False))

    _show_watermark(watermark_records)


def _show_watermark(watermark_records: list):
    if not watermark_records:
        print("[COMPARE] (tidak ada watermark MinIO yang ditemukan)")
        return
    print("\n[COMPARE] Current watermark (MinIO):")
    wm_cols = [c for c in ("creds", "sheet_name", "toko", "last_processed_date")
               if c in (watermark_records[0] or {})]
    wm_df = pd.DataFrame(watermark_records)[wm_cols].sort_values(wm_cols)
    print(wm_df.to_string(index=False))


def show_watermark(watermark_records: list):
    """Shows only the MinIO watermark (no bronze read/compare)."""
    _show_watermark(watermark_records)


def fmt_drift_date(val) -> str:
    """Format a drift-check date value for the gate-2 email table ('' for NaT)."""
    try:
        if val is None or pd.isna(val):
            return ""
        if hasattr(val, "strftime"):
            return val.strftime("%Y-%m-%d")
        return str(val)
    except Exception:
        return str(val or "")


def build_drift_rows(status_df: pd.DataFrame) -> list[dict]:
    """Per-sheet watermark drift summary from the pre-flight check.

    Column order matches the gate-2 email table: Sheet | Toko | Sheet max date |
    Current watermark | Status (BEHIND/ok). Sorted by sheet_name then toko.
    """
    rows = []
    for _, row in status_df.iterrows():
        rows.append({
            "sheet_name": str(row.get("sheet_name") or ""),
            "toko": str(row.get("grain") or ""),
            "gsheet_max": fmt_drift_date(row.get("sheet_max_tanggal")),
            "watermark": fmt_drift_date(row.get("last_processed_date")),
            "status": "BEHIND" if row.get("is_behind") else "ok",
        })
    rows.sort(key=lambda r: (r["sheet_name"], r["toko"]))
    return rows


def write_wm_log(log_folder, run_key, status_df, sheet_passes, verdict_msg):
    """Write the watermark drift check log file."""
    from datetime import datetime
    from src.gmv_max_live.utils.log import write_section_log

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


def finish(
    creds,
    watermark_records: list,
    note: str,
    *,
    original_tables=(),
    testing_tables=(),
    compare: bool = True,
    date_col: str = "tanggal",
    group_col: str = "toko",
    dedup_cols=None,
    days: int = 15,
    project_id: str = PROJECT_ID,
):
    """Final step on every exit path.

    If `compare` is True runs the bronze compare + shows watermark; otherwise
    (e.g. GMV_Max_Live, no original table) only shows the watermark. Then prints
    the ETL DONE line.
    """
    if compare:
        compare_and_show_watermark(
            creds, watermark_records,
            original_tables=original_tables, testing_tables=testing_tables,
            date_col=date_col, group_col=group_col, dedup_cols=dedup_cols,
            days=days, project_id=project_id,
        )
    else:
        show_watermark(watermark_records)
    print(note)
