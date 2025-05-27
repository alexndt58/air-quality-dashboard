# prototype/ingestion/ingest.py
import duckdb
from pathlib import Path

def ingest(csv_path="data/raw/AirQualityDataHourly.csv", db_path="data/airquality.duckdb"):
    # Create DuckDB table from the CSV (replace if exists)
    con = duckdb.connect(db_path)
    con.execute("DROP TABLE IF EXISTS air_quality_raw")
    con.execute(f"""
        CREATE TABLE air_quality_raw AS
        SELECT * FROM read_csv_auto('{csv_path}')
    """)
    con.close()
    print(f"✅ Ingested {csv_path} into {db_path} as table air_quality_raw")

if __name__ == "__main__":
    ingest()

