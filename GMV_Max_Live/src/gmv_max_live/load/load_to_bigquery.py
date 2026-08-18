# src/gmv_max_produk/load/load_to_bigquery.py
from pandas_gbq import to_gbq
import pandas as pd
from google.oauth2.service_account import Credentials


def load_df(
    df: pd.DataFrame,
    table_id: str,
    project_id: str,
    if_exists: str,
    credentials: Credentials,
):
    to_gbq(
        df,
        destination_table=table_id,
        project_id=project_id,
        if_exists=if_exists,
        credentials=credentials,
    )