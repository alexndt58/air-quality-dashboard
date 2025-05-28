import os
import duckdb


def ingest(raw_dir: str, db_path: str) -> None:
    """
    Read every CSV in `raw_dir` into a DuckDB database at `db_path`.
    Each CSV filename (sans `.csv`) becomes a table name.
    """
    # Ensure parent directory exists for the DB
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect (creates) DuckDB database
    con = duckdb.connect(database=db_path, read_only=False)

    # Iterate through CSV files
    for fname in sorted(os.listdir(raw_dir)):
        if not fname.lower().endswith('.csv'):
            continue
        table_name = os.path.splitext(fname)[0]
        csv_path = os.path.join(raw_dir, fname)
        con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            f"SELECT * FROM read_csv_auto('{csv_path}')"
        )
    con.close()

# Alias for compatibility
ingest_db = ingest
clean = ingest
