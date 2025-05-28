#!/usr/bin/env python3
"""
Load pollutant CSV(s) from a directory into DuckDB.

- If only one CSV (e.g. `AirQualityDataHourly.csv`), load it into `raw_aurn`.
- If two (AURN & MET), split into `raw_aurn` & `raw_weather`.
- If `MET` missing, creates empty `raw_weather`.

Usage:
    ingest(raw_dir: str, db_path: str)
"""
import os
import duckdb


def ingest(raw_dir: str, db_path: str) -> None:
    """
    Ingest CSV files from `raw_dir` into a DuckDB database at `db_path`.
    """
    # Ensure output directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(database=db_path, read_only=False)

    # Discover CSVs
    files = sorted(f for f in os.listdir(raw_dir) if f.lower().endswith('.csv'))
    aurn_file = None
    weather_file = None

    if len(files) == 1:
        # Single CSV → treat as aurn
        aurn_file = os.path.join(raw_dir, files[0])
    else:
        # Multiple CSVs → assign by prefix
        for fname in files:
            low = fname.lower()
            path = os.path.join(raw_dir, fname)
            if low.startswith('aurn'):
                aurn_file = path
            elif low.startswith('met') or low.startswith('weather'):
                weather_file = path

    # Ingest AURN
    if aurn_file:
        con.execute(
            f"CREATE OR REPLACE TABLE raw_aurn AS "
            f"SELECT * FROM read_csv_auto('{aurn_file}')"
        )
    else:
        # Empty raw_aurn
        con.execute(
            "CREATE OR REPLACE TABLE raw_aurn AS "
            "SELECT CAST(NULL AS TIMESTAMP) AS datetime, "
            "CAST(NULL AS DOUBLE) AS no2, "
            "CAST(NULL AS DOUBLE) AS pm25, "
            "CAST(NULL AS VARCHAR) AS site_name, "
            "CAST(NULL AS DOUBLE) AS latitude, "
            "CAST(NULL AS DOUBLE) AS longitude "
            "WHERE FALSE"
        )

    # Ingest Weather
    if weather_file:
        con.execute(
            f"CREATE OR REPLACE TABLE raw_weather AS "
            f"SELECT * FROM read_csv_auto('{weather_file}')"
        )
    else:
        # Empty raw_weather
        con.execute(
            "CREATE OR REPLACE TABLE raw_weather AS "
            "SELECT CAST(NULL AS TIMESTAMP) AS datetime, "
            "CAST(NULL AS DOUBLE) AS temp, "
            "CAST(NULL AS DOUBLE) AS wind_speed "
            "WHERE FALSE"
        )

    con.close()
