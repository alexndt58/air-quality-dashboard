# prototype/ingestion/ingest.py

import duckdb
import re
from pathlib import Path

def ingest(raw_dir: str = "data/raw", db_path: str = "data/airquality.duckdb"):
    """
    1. Connect to (or create) data/airquality.duckdb
    2. Read all aurn_hourly*.csv* and metoffice*.csv* from data/raw/
    3. Load them into DuckDB tables raw_aurn and raw_weather
    """
    raw_path = Path(raw_dir)
    con = duckdb.connect(database=db_path, read_only=False)

    # --- raw_aurn ingestion (skip metadata for official files) ---
    aurn_files = sorted(raw_path.glob("*aurn*.csv*"))
    if aurn_files:
        con.execute("DROP TABLE IF EXISTS raw_aurn")
        for idx, f in enumerate(aurn_files):
            # skip header on files like aurn_hourly_YYYY*.csv*
            skip = 3 if re.match(r"^aurn_hourly_\d{4}", f.name) else 0
            mode = "CREATE TABLE raw_aurn AS" if idx == 0 else "INSERT INTO raw_aurn"
            sql = f"""
                {mode}
                SELECT * FROM read_csv_auto(
                  '{f}',
                  skip={skip},
                  delim=',',
                  ignore_errors=true,
                  null_padding=true
                )
            """
            con.execute(sql)
    else:
        print(f"⚠️  No AURN CSVs found in {raw_dir}")

    # --- raw_weather ingestion (always create table) ---
    wx_files = sorted(raw_path.glob("metoffice*.csv*"))
    con.execute("DROP TABLE IF EXISTS raw_weather")
    if wx_files:
        con.execute(f"CREATE TABLE raw_weather AS SELECT * FROM read_csv_auto('{wx_files[0]}')")
        for f in wx_files[1:]:
            con.execute(f"INSERT INTO raw_weather SELECT * FROM read_csv_auto('{f}')")
    else:
        print(f"⚠️  No Met Office CSVs found in {raw_dir}")
        # fallback: empty table matching raw_aurn schema
        con.execute("CREATE TABLE raw_weather AS SELECT * FROM raw_aurn WHERE FALSE")

    con.close()
    print(f"✅ Ingestion complete. Tables raw_aurn and raw_weather are in {db_path}")


if __name__ == "__main__":
    ingest()
