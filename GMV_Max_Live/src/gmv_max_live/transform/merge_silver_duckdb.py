import re
from pathlib import Path
import duckdb
import pandas as pd


def _transpile_bq_to_duckdb(sql_content: str) -> str:
    """Helper untuk merubah sintaks BigQuery SQL agar kompatibel dengan DuckDB di Memory."""
    # 1. Hapus prefix 'database-sigma.' & tanda backtick (`)
    sql_executable = (
        sql_content.replace("`database-sigma.", "")
        .replace("database-sigma.", "")
        .replace("`", "")
    )

    # 2. Sisipkan kata 'INTO' pada klausa MERGE
    sql_executable = re.sub(
        r"\bMERGE\s+(?!INTO\b)", "MERGE INTO ", sql_executable, flags=re.IGNORECASE
    )

    # 3. Ubah EXCEPT(...) BigQuery menjadi EXCLUDE(...) DuckDB
    sql_executable = re.sub(
        r"\bEXCEPT\s*\(", "EXCLUDE (", sql_executable, flags=re.IGNORECASE
    )

    # 4. Ubah BigQuery raw string r'...' menjadi standard string '...' DuckDB
    sql_executable = re.sub(r"\br(['\"][^'\"]*['\"])", r"\1", sql_executable)

    # 5. Hapus TO_HEX() karena SHA256() DuckDB (dengan ext bigquery)
    #    sudah mengembalikan String Hexadecimal
    sql_executable = re.sub(
        r"TO_HEX\s*\(\s*SHA256\s*\((.*)\)\s*\)",
        r"SHA256(\1)",
        sql_executable,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # 6. Ubah ARRAY_TO_STRING BigQuery menjadi array_to_string DuckDB
    sql_executable = re.sub(
        r"\bARRAY_TO_STRING\b",
        "array_to_string",
        sql_executable,
        flags=re.IGNORECASE,
    )

    # 7. Ubah INSERT ROW BigQuery menjadi INSERT BY NAME (DuckDB)
    sql_executable = re.sub(
        r"\bWHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s+ROW\b",
        "WHEN NOT MATCHED THEN INSERT BY NAME",
        sql_executable,
        flags=re.IGNORECASE,
    )

    # 8. Ubah fungsi & tipe data khas BigQuery
    sql_executable = re.sub(
        r"\bSAFE_CAST\b", "TRY_CAST", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"\bINT64\b", "BIGINT", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"\bFLOAT64\b", "DOUBLE", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"\bNUMERIC\b", "DECIMAL", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"FORMAT_DATE\(\s*'%F'\s*,\s*([^)]+)\)",
        r"STRFTIME(\1, '%Y-%m-%d')",
        sql_executable,
        flags=re.IGNORECASE,
    )

    return sql_executable


def test_merge_to_silver_duckdb(df_bronze: pd.DataFrame):
    """Menjalankan simulasi MERGE Bronze -> Silver di DuckDB In-Memory."""
    # 1. Init Connection
    con = duckdb.connect(":memory:")
    con.sql("INSTALL bigquery FROM community; LOAD bigquery;")

    # 2. Setup Bronze
    con.sql("CREATE SCHEMA IF NOT EXISTS BRONZE_DB;")
    con.sql("CREATE TABLE BRONZE_DB.bronze_maxl AS SELECT * FROM df_bronze")
    print("[BRONZE] Load to BRONZE_DB.bronze_maxl DONE")

    # 3. Setup Silver Schema & Config Table
    con.sql("CREATE SCHEMA IF NOT EXISTS SILVER_DB;")
    con.sql("CREATE SCHEMA IF NOT EXISTS CONFIG_DB;")

    con.sql("""
        CREATE TABLE IF NOT EXISTS CONFIG_DB.config_gmvmax_scaling (
            toko VARCHAR,
            start_date DATE,
            end_date DATE,
            scale_factor DECIMAL
        );
    """)

    con.sql("""
        CREATE TABLE IF NOT EXISTS SILVER_DB.silver_tt_ads_gmvmax_live (
            tanggal DATE,
            toko VARCHAR,
            nama_live VARCHAR,
            waktu_peluncuran TIMESTAMP,
            status VARCHAR,
            nama_kampanye VARCHAR,
            id_campaign VARCHAR,
            mata_uang VARCHAR,
            spend DECIMAL,
            spend_net DECIMAL,
            orders_sku BIGINT,
            orders_sku_toko BIGINT,
            cpo DECIMAL,
            revenue_gross DECIMAL,
            revenue_net DECIMAL,
            roi DOUBLE,
            views_live BIGINT,
            cost_per_view DECIMAL,
            views_live_10s BIGINT,
            cost_per_view_10s DECIMAL,
            followers_live BIGINT,
            snapshot_ts TIMESTAMP,
            snapshot_date DATE,
            run_id VARCHAR,
            row_hash_raw VARCHAR,
            row_hash_clean VARCHAR
        );
    """)

    # 4. Read & Transpile SQL
    print("[SILVER] Running MERGE into SILVER_DB.silver_tt_ads_gmvmax_live ...")
    root_dir = Path(__file__).resolve().parents[3]
    sql_path = root_dir / "sql" / "silver_merge_tt_ads_gmvmax_live.sql"

    sql_content = sql_path.read_text(encoding="utf-8")
    sql_executable = _transpile_bq_to_duckdb(sql_content)

    # 5. Execute MERGE Query
    try:
        con.sql(sql_executable)
        print("✅ MERGE SQL Execution Success!")
    except Exception as e:
        print(f"❌ Error saat eksekusi SQL: {e}")

    print("[SILVER] MERGE DONE")
    con.close()