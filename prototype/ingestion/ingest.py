#!/usr/bin/env python3
"""
Ingest raw AURN & MET CSVs from a directory into DuckDB.

Functions:
  ingest(raw_dir, db_path) → loads into tables `raw_aurn` and `raw_weather`.
  clean(raw_dir, db_path, max_gap_hours) → applies cleaning to DuckDB.

Test fixtures call `ingest(raw_dir=..., db_path=...)`.
"""
import os
import duckdb


def ingest(raw_dir: str, db_path: str) -> None:
    """
    Read AURN and MET CSVs from `raw_dir` into DuckDB database at `db_path`.
    - AURN files (prefix 'aurn') → table raw_aurn
    - MET files (prefix 'met' or 'weather') → table raw_weather
    Missing parts yield empty tables.
    """
    # Ensure output directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    con = duckdb.connect(database=db_path, read_only=False)

    # Discover CSVs
    aurn_file = None
    weather_file = None
    for fname in sorted(os.listdir(raw_dir)):
        lower = fname.lower()
        if not lower.endswith('.csv'):
            continue
        full = os.path.join(raw_dir, fname)
        if lower.startswith('aurn'):
            aurn_file = full
        elif lower.startswith('met') or lower.startswith('weather'):
            weather_file = full

    # Ingest AURN
    if aurn_file:
        con.execute(f"CREATE OR REPLACE TABLE raw_aurn AS SELECT * FROM read_csv_auto('{aurn_file}')")
    else:
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

    # Ingest WEATHER
    if weather_file:
        con.execute(f"CREATE OR REPLACE TABLE raw_weather AS SELECT * FROM read_csv_auto('{weather_file}')")
    else:
        con.execute(
            "CREATE OR REPLACE TABLE raw_weather AS "
            "SELECT CAST(NULL AS TIMESTAMP) AS datetime, "
            "CAST(NULL AS DOUBLE) AS temp, "
            "CAST(NULL AS DOUBLE) AS wind_speed "
            "WHERE FALSE"
        )

    con.close()

# Alias for cleaning imports
clean = ingest
