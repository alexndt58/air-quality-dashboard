# prototype/ingestion/ingest.py
"""
Load an (optionally cleaned) Air-Quality CSV into DuckDB.

Usage (from repo root):
    python prototype/ingestion/ingest.py \
        --csv data/raw/AirQualityDataHourly.csv \
        --db  data/airquality.duckdb \
        --table air_quality_raw
"""

from __future__ import annotations

import argparse
from pathlib import Path
import duckdb


def ingest(csv_path: str | Path,
           db_path: str | Path = "data/airquality.duckdb",
           table_name: str = "air_quality_raw") -> None:
    """
    Create or replace *table_name* in *db_path* with the contents of *csv_path*.

    Parameters
    ----------
    csv_path : str | Path
        Source CSV to read (raw or cleaned).
    db_path : str | Path, default "data/airquality.duckdb"
        DuckDB database file.
    table_name : str, default "air_quality_raw"
        Destination table (will be replaced if it already exists).
    """
    csv_path = Path(csv_path).expanduser().resolve()
    db_path = Path(db_path).expanduser().resolve()

    if not csv_path.exists():
        raise FileNotFoundError(f"🚫 CSV not found: {csv_path}")

    # Ensure the target directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(db_path) as con:
        # Drop then create-as-select gives us an idempotent load
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(
            f"""
            CREATE TABLE {table_name} AS
            SELECT *
            FROM read_csv_auto('{csv_path.as_posix()}',
                               SAMPLE_SIZE=-1)  -- read full file for types
            """
        )

    print(f"✅ Ingested {csv_path} into {db_path} → table '{table_name}'")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest Air-Quality CSV into DuckDB")
    p.add_argument("--csv",  "-c", default="data/raw/AirQualityDataHourly.csv",
                   help="Path to source CSV (raw or cleaned).")
    p.add_argument("--db",   "-d", default="data/airquality.duckdb",
                   help="DuckDB database file.")
    p.add_argument("--table","-t", default="air_quality_raw",
                   help="Destination table name.")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    ingest(csv_path=args.csv, db_path=args.db, table_name=args.table)
