# prototype/ingestion/ingest.py

import duckdb
from pathlib import Path

def ingest(raw_dir: str, db_path: str):
    """
    Scan raw_dir for all CSVs, and ingest each into a DuckDB table
    named after the file (stem, lowercase), dropping any existing.
    """
    raw_path = Path(raw_dir)
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_file))

    for csv_file in sorted(raw_path.glob("*.csv")):
        table_name = csv_file.stem.lower()
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{csv_file}')
        """)
        print(f"• Ingested {csv_file.name} → table `{table_name}`")

    con.close()
    print(f"✅ All CSVs in {raw_dir} ingested into {db_path}")
