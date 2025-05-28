import os
import duckdb

def ingest(raw_dir: str, db_path: str) -> None:
    """
    Read every CSV in `raw_dir` into a DuckDB DB at `db_path`.
    Each CSV filename (sans `.csv`) becomes a table name.
    """
    # Ensure parent directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect (creates DB file)
    con = duckdb.connect(database=db_path, read_only=False)

    # Iterate CSVs
    for fname in sorted(os.listdir(raw_dir)):
        if not fname.lower().endswith(".csv"):
            continue
        table = os.path.splitext(fname)[0]
        full = os.path.join(raw_dir, fname)
        con.execute(
            f"CREATE OR REPLACE TABLE {table} AS "
            f"SELECT * FROM read_csv_auto('{full}')"
        )

    con.close()

# alias for backward compatibility
ingest_db = ingest
