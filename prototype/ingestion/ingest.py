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
    Ingest air quality and weather CSVs from raw_dir into DuckDB at db_path.
    """
    # 1. Extract compressed archives first (skips if none)
    extract_archives(raw_dir)

    raw_path = Path(raw_dir)
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=str(db_path), read_only=False)

    # --- Air Quality Data ingestion ---
    airquality_file = raw_path / "AirQualityDataHourly.csv"
    if airquality_file.exists():
        print(f"Loading air quality data from {airquality_file.name}")
        con.execute("DROP TABLE IF EXISTS raw_airquality")
        con.execute(f"""
            CREATE TABLE raw_airquality AS
            SELECT * FROM read_csv_auto('{airquality_file}')
        """)
    else:
        print(f"⚠️  Air quality file not found: {airquality_file}")

    # --- Weather Data ingestion ---
    weather_file = raw_path / "heathrow-2023.csv"
    if weather_file.exists():
        print(f"Loading weather data from {weather_file.name}")
        con.execute("DROP TABLE IF EXISTS raw_weather")
        con.execute(f"""
            CREATE TABLE raw_weather AS
            SELECT * FROM read_csv_auto('{weather_file}')
        """)
    else:
        print(f"⚠️  Weather file not found: {weather_file}")
        # Make an empty table if needed
        if "raw_airquality" in [t[0] for t in con.execute("SHOW TABLES").fetchall()]:
            con.execute("""
                CREATE TABLE raw_weather AS
                SELECT * FROM raw_airquality WHERE FALSE
            """)
        else:
            con.execute("CREATE TABLE raw_weather(dummy INT); DELETE FROM raw_weather;")

    con.close()
    print(f"✅ Ingestion complete. Tables raw_airquality & raw_weather in {db_path}")

if __name__ == "__main__":
    ingest()
