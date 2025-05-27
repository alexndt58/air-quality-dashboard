# prototype/cleaning/clean.py

import duckdb
import pandas as pd

def clean(db_path="data/airquality.duckdb", max_gap_hours=2):
    con = duckdb.connect(db_path, read_only=False)
    df = con.execute("SELECT * FROM raw_aurn").df()

    # Parse datetime (change this if your col name is different!)
    if "datetime" not in df.columns:
        raise Exception("No 'datetime' column found in AirQualityDataHourly.csv!")
    df["datetime"] = pd.to_datetime(df["datetime"])

    # Choose columns (adjust to your file’s real names)
    pollutants = ["NO2", "PM10", "PM2.5"]   # Adjust to match your actual column names
    needed_cols = ["datetime", "site_name"] + pollutants
    df = df[[col for col in needed_cols if col in df.columns]]

    # Simple cleaning: drop rows with missing site/datetime or all pollutants
    df = df.dropna(subset=["site_name", "datetime"] + pollutants)

    # Write cleaned table to DuckDB
    con.execute("DROP TABLE IF EXISTS clean_aurn")
    con.execute("CREATE TABLE clean_aurn AS SELECT * FROM df")
    con.close()
    print("✅ Cleaning complete. Table clean_aurn written to DB.")

if __name__ == "__main__":
    clean()
