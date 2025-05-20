# prototype/ingestion/ingest.py

import duckdb
import re
from pathlib import Path

def ingest(raw_dir: str = "data/raw", db_path: str = "data/airquality.duckdb"):
    """
    Ingests CSVs from raw_dir into DuckDB tables:
    - raw_aurn: AURN data (skips metadata for official files)
    - raw_weather: Met Office data or empty schema if missing
    """
    raw_path = Path(raw_dir)
    # Ensure database directory exists
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=db_path, read_only=False)

    # --- raw_aurn ingestion ---
    aurn_files = sorted(raw_path.glob("*aurn*.csv*"))
    con.execute("DROP TABLE IF EXISTS raw_aurn")
    if aurn_files:
        for idx, f in enumerate(aurn_files):
            skip = 3 if re.match(r"^aurn_hourly_\d{4}", f.name) else 0
            if idx == 0:
                con.execute(
                    f"""
                    CREATE TABLE raw_aurn AS
                    SELECT * FROM read_csv_auto(
                        '{f}', skip={skip}, delim=',', ignore_errors=true, null_padding=true
                    )
                    """
                )
            else:
                con.execute(
                    f"""
                    INSERT INTO raw_aurn
                    SELECT * FROM read_csv_auto(
                        '{f}', skip={skip}, delim=',', ignore_errors=true, null_padding=true
                    )
                    """
                )
    else:
        print(f"⚠️  No AURN CSVs found in {raw_dir}")

    # --- raw_weather ingestion ---
    wx_files = sorted(raw_path.glob("*met*.csv*"))
    con.execute("DROP TABLE IF EXISTS raw_weather")
    if wx_files:
        con.execute(
            f"""
            CREATE TABLE raw_weather AS
            SELECT * FROM read_csv_auto(
                '{wx_files[0]}', delim=',', ignore_errors=true, null_padding=true
            )
            """
        )
        for f in wx_files[1:]:
            con.execute(
                f"""
                INSERT INTO raw_weather
                SELECT * FROM read_csv_auto(
                    '{f}', delim=',', ignore_errors=true, null_padding=true
                )
                """
            )
    else:
        print(f"⚠️  No Met Office CSVs found in {raw_dir}")
        # Create empty table with expected schema
        con.execute(
            "CREATE TABLE raw_weather(datetime TIMESTAMP, temp DOUBLE, wind_speed DOUBLE)"
        )

    con.close()

if __name__ == "__main__":
    ingest()
    print("✅ Ingestion complete.")
