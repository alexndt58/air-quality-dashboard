# **prototype/ingestion/ingest.py** ---

# python
#!/usr/bin/env python3
"""
Load raw AURN CSVs from a directory into DuckDB.

Functions:
  ingest(raw_dir, db_path) → loads CSV into table `raw_aurn`.

Test fixtures call `ingest(raw_dir=..., db_path=...)`.
"""
import os
import duckdb

def ingest(raw_dir: str, db_path: str) -> None:
    """
    Read the first CSV in `raw_dir` into a DuckDB database at `db_path`.
    Creates or replaces table `raw_aurn`.
    """
    # Ensure output directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    con = duckdb.connect(database=db_path, read_only=False)

    # Find first CSV file
    csvs = [f for f in sorted(os.listdir(raw_dir)) if f.lower().endswith('.csv')]
    if csvs:
        path = os.path.join(raw_dir, csvs[0])
        con.execute(
            f"CREATE OR REPLACE TABLE raw_aurn AS "
            f"SELECT * FROM read_csv_auto('{path}')"
        )
    else:
        # Empty fallback
        con.execute(
            "CREATE OR REPLACE TABLE raw_aurn AS "
            "SELECT CAST(NULL AS TIMESTAMP) AS datetime "
            "WHERE FALSE"
        )

    con.close()

