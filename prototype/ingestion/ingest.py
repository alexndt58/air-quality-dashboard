# prototype/ingestion/ingest.py
import duckdb
from pathlib import Path

def ingest(raw_dir: str = "data/raw", db_path: str = "data/airquality.duckdb"):
    """
    Ingest every CSV file in `raw_dir` into DuckDB at `db_path`,
    creating one table per file (named after the file stem).
    """
    con = duckdb.connect(db_path)
    raw_dir = Path(raw_dir)
    for csv in raw_dir.glob("*.csv"):
        table = csv.stem
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"""
            CREATE TABLE {table} AS
            SELECT * FROM read_csv_auto('{csv}')
        """)
        print(f"  • Ingested {csv.name} → table `{table}`")
    con.close()
    print(f"✅ All CSVs in {raw_dir} ingested into {db_path}")
