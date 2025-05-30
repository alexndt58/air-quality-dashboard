# prototype/ingestion/ingest.py
"""
Ingest all CSVs from a raw directory into DuckDB, with logging, CLI args,
and transactional safety.
"""
import duckdb
import click
import logging
from pathlib import Path

# Configure module-level logger
logger = logging.getLogger(__name__)

@click.command()
@click.option(
    "--raw-dir", 
    required=True, 
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Directory containing raw CSV files"
)
@click.option(
    "--db-path", 
    required=True, 
    type=click.Path(path_type=Path),
    help="DuckDB file path to write to"
)
@click.option(
    "--pattern", 
    default="*.csv", 
    show_default=True,
    help="Glob pattern to match CSV files"
)
def ingest(raw_dir: Path, db_path: Path, pattern: str):
    """
    Ingest matching CSVs from raw_dir into DuckDB.
    Each table is named after the CSV stem, cleaned (lowercase, no spaces).
    """
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    raw_dir = raw_dir.resolve()
    db_path = db_path.resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Connect with a transaction for atomicity
    con = duckdb.connect(str(db_path))
    try:
        con.begin()
        files = sorted(raw_dir.glob(pattern))
        if not files:
            logger.warning("No files matched %s in %s", pattern, raw_dir)
        for csv_file in files:
            table = csv_file.stem.lower().replace(' ', '_')
            logger.info("Ingesting %s → table '%s'", csv_file.name, table)
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.execute(
                f"CREATE TABLE {table} AS SELECT * FROM read_csv_auto('{csv_file}', header=True)"
            )
        con.commit()
        logger.info("✅ Ingestion complete into %s", db_path)
    except Exception as e:
        con.rollback()
        logger.exception("Ingestion failed, rolled back transaction")
        raise
    finally:
        con.close()

if __name__ == "__main__":
    ingest()
