import os
import duckdb


def ingest(raw_dir: str, db_path: str) -> None:
    """
    Ingest all CSV files from `raw_dir` into a DuckDB database at `db_path`.
    Each CSV becomes a table named after the file (without .csv).
    """
    # Ensure target directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect (will create) DuckDB database
    con = duckdb.connect(database=db_path, read_only=False)

    # Load each CSV in raw_dir
    for fname in sorted(os.listdir(raw_dir)):
        if not fname.lower().endswith('.csv'):
            continue
        table_name = os.path.splitext(fname)[0]
        file_path = os.path.join(raw_dir, fname)
        con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            f"SELECT * FROM read_csv_auto('{file_path}')"
        )

    con.close()
