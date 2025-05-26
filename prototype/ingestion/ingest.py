# prototype/ingestion/ingest.py

import os
import tarfile
import zipfile
from pathlib import Path
import duckdb

def extract_archives(raw_dir):
    """
    Extract any .tar, .tar.gz, .zip files in raw_dir to the same folder.
    """
    raw_path = Path(raw_dir)
    for archive in raw_path.glob("*.tar*"):
        print(f"Extracting {archive.name} ...")
        with tarfile.open(archive, "r:*") as tar:
            tar.extractall(path=raw_path)
    for archive in raw_path.glob("*.zip"):
        print(f"Extracting {archive.name} ...")
        with zipfile.ZipFile(archive, 'r') as zip_ref:
            zip_ref.extractall(path=raw_path)

def ingest(raw_dir="data/raw", db_path="data/airquality.duckdb"):
    """
    Ingest AURN and Met Office CSVs from raw_dir into DuckDB at db_path.
    """
    # 1. Extract compressed archives first
    extract_archives(raw_dir)

    raw_path = Path(raw_dir)
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=str(db_path), read_only=False)

    # --- raw_aurn ingestion (skip first 3 metadata lines) ---
    aurn_files = sorted(raw_path.glob("aurn_hourly*.csv*"))
    if aurn_files:
        con.execute("DROP TABLE IF EXISTS raw_aurn")
        print(f"Loading AURN data from {aurn_files[0].name}")
        con.execute(f"""
            CREATE TABLE raw_aurn AS
            SELECT * FROM read_csv_auto(
                '{aurn_files[0]}',
                skip=3
            )
        """)
        for f in aurn_files[1:]:
            print(f"Inserting more AURN data from {f.name}")
            con.execute(f"""
                INSERT INTO raw_aurn
                SELECT * FROM read_csv_auto(
                    '{f}',
                    skip=3
                )
            """)
    else:
        print(f"⚠️  No AURN CSVs found in {raw_dir}")

    # --- raw_weather ingestion ---
    wx_files = sorted(raw_path.glob("metoffice*.csv*"))
    con.execute("DROP TABLE IF EXISTS raw_weather")
    if wx_files:
        print(f"Loading Met Office weather from {wx_files[0].name}")
        con.execute(f"""
            CREATE TABLE raw_weather AS
            SELECT * FROM read_csv_auto('{wx_files[0]}')
        """)
        for f in wx_files[1:]:
            print(f"Inserting more weather data from {f.name}")
            con.execute(f"""
                INSERT INTO raw_weather
                SELECT * FROM read_csv_auto('{f}')
            """)
    else:
        print(f"⚠️  No Met Office CSVs found in {raw_dir}")
        # If no weather files, make an empty table with raw_aurn structure (if present)
        if "raw_aurn" in [t[0] for t in con.execute("SHOW TABLES").fetchall()]:
            con.execute("""
                CREATE TABLE raw_weather AS
                SELECT * FROM raw_aurn WHERE FALSE
            """)
        else:
            # Or create a dummy empty table if even aurn is missing
            con.execute("CREATE TABLE raw_weather(dummy INT); DELETE FROM raw_weather;")

    con.close()
    print(f" Ingestion complete. Tables raw_aurn & raw_weather in {db_path}")

if __name__ == "__main__":
    ingest()
