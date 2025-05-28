# run_pipeline.py
"""
End-to-end ETL driver for the air-quality dataset.

Steps
-----
1. Clean raw CSV → ``data/clean/clean_air_quality.csv``   (see prototype/cleaning/clean.py)
2. Ingest the cleaned CSV into DuckDB → ``data/airquality.duckdb`` (see prototype/ingestion/ingest.py)

Usage
-----
# default paths
python run_pipeline.py

# custom paths / table name
python run_pipeline.py \
    --raw   data/raw/AirQualityDataHourly.csv \
    --clean data/clean/clean_air_quality.csv \
    --db    data/airquality.duckdb \
    --table air_quality
"""

from __future__ import annotations

import argparse
from prototype.cleaning.clean import clean
from prototype.ingestion.ingest import ingest


DEFAULT_RAW   = "data/raw/AirQualityDataHourly.csv"
DEFAULT_CLEAN = "data/clean/clean_air_quality.csv"
DEFAULT_DB    = "data/airquality.duckdb"
DEFAULT_TABLE = "air_quality"


def run_pipeline(raw_csv: str = DEFAULT_RAW,
                 clean_csv: str = DEFAULT_CLEAN,
                 db_path: str = DEFAULT_DB,
                 table_name: str = DEFAULT_TABLE) -> None:
    # ── 1. CLEAN ──────────────────────────────────────────────────────────────
    print("🧹  Cleaning raw CSV …")
    clean()               # writes to ``clean_csv`` (hard-coded in clean.py)

    # ── 2. INGEST ────────────────────────────────────────────────────────────
    print("📥  Ingesting cleaned CSV into DuckDB …")
    ingest(csv_path=clean_csv, db_path=db_path, table_name=table_name)

    print("✅ Pipeline complete.")


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run the air-quality ETL pipeline.")
    ap.add_argument("--raw",   default=DEFAULT_RAW,
                    help="Path to the raw CSV (used by cleaning step).")
    ap.add_argument("--clean", default=DEFAULT_CLEAN,
                    help="Destination for the cleaned CSV.")
    ap.add_argument("--db",    default=DEFAULT_DB,
                    help="DuckDB database file.")
    ap.add_argument("--table", default=DEFAULT_TABLE,
                    help="Destination table name in DuckDB.")
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_pipeline(raw_csv=args.raw,
                 clean_csv=args.clean,
                 db_path=args.db,
                 table_name=args.table)
