# src/gmv_max_live/ingestion/fetch_gmv_max_live_gsheet.py
import os

import gspread
import pandas as pd


SHEET_REGISTRY = {
    "matz": "SH_KEY_MATZ",
    "ian": "SH_KEY_IAN",
    "deni": "SH_KEY_DENI",
    "riwa": "SH_KEY_RIWA",
    "imam": "SH_KEY_IMAM",
}


def fetch_gmv_max_live(gc: gspread.Client) -> pd.DataFrame:
    """
    Ambil data gmv max live dari GSheet yang terdaftar di SHEET_REGISTRY,
    tag tiap sheet dengan kolom 'sheet_name', lalu concat jadi satu
    DataFrame raw (belum dibersihkan).
    """
    frames = []
    for sheet_name, env_key in SHEET_REGISTRY.items():
        sh = gc.open_by_key(os.getenv(env_key))
        ws = sh.worksheet("GMV MAX Live")
        values = ws.get_all_values()
        df_sheet = pd.DataFrame(values[3:], columns=values[2])
        df_sheet = df_sheet.loc[:, ~df_sheet.columns.duplicated()]
        df_sheet["creds"] = os.getenv(env_key)
        df_sheet["sheet_name"] = sheet_name
        frames.append(df_sheet)
        print(f"[INGEST] {sheet_name}: {len(df_sheet)} rows")

    tiktok_maxl = pd.concat(frames, ignore_index=True)
    return tiktok_maxl
