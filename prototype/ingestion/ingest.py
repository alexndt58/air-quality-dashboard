# prototype/ingestion/ingest.py

import duckdb
from pathlib import Path

def ingest(raw_dir="data/raw", db_path="data/airquality.duckdb"):
    raw_path = Path(raw_dir)
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=str(db_path), read_only=False)

    # Ingest Air Quality Data
    aurn_file = raw_path / "AirQualityDataHourly.csv"
    if aurn_file.exists():
        print(f"Loading air quality data from {aurn_file.name}")
        con.execute("DROP TABLE IF EXISTS raw_aurn")
        con.execute(f"""
            CREATE TABLE raw_aurn AS
            SELECT * FROM read_csv_auto('{aurn_file}', header=True)
        """)
    else:
        print(f"⚠️  Air quality data not found: {aurn_file}")

    # No weather data
    con.execute("DROP TABLE IF EXISTS raw_weather")
    con.execute("CREATE TABLE raw_weather(dummy INT); DELETE FROM raw_weather;")
    con.close()
    print(f"✅ Ingestion complete. Tables raw_aurn in {db_path}")

if __name__ == "__main__":
    ingest()
