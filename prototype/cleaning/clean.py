# prototype/cleaning/clean.py
"""
Clean tables inside DuckDB: remove gaps > max_gap_hours, drop empty, and produce cleaned tables.
"""
import duckdb
import click
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

@click.command()
@click.option(
    "--db-path",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="DuckDB file path to read/write"
)
@click.option(
    "--max-gap-hours",
    default=2,
    show_default=True,
    type=float,
    help="Maximum allowed gap between consecutive timestamps"
)
def clean(db_path: Path, max_gap_hours: float):
    """
    Read each raw table, enforce max gap, drop fully-null rows, and store as clean_<name>.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    db_path = db_path.resolve()
    con = duckdb.connect(str(db_path))
    try:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        raw_tables = [t for t in tables if not t.startswith('clean_')]

        for tbl in raw_tables:
            clean_tbl = f"clean_{tbl}"
            logger.info("Cleaning table '%s' → '%s'", tbl, clean_tbl)
            # 1) Check for datetime column
            info = con.execute(f"DESCRIBE {tbl}").df()
            if 'datetime' not in info.column_name.str.lower().tolist():
                logger.warning("Skipping %s: no datetime column", tbl)
                continue
            # 2) Build cleaned table
            con.execute(f"DROP TABLE IF EXISTS {clean_tbl}")
            con.execute(
                f"CREATE TABLE {clean_tbl} AS\
                SELECT * FROM {tbl} \
                WHERE datetime IS NOT NULL"
            )
            # 3) Optionally filter gaps
            con.execute(
                f"ALTER TABLE {clean_tbl} ADD COLUMN _prev_ts TIMESTAMP;"
            )
            con.execute(
                f"UPDATE {clean_tbl} SET _prev_ts = LAG(datetime) OVER (ORDER BY datetime)"
            )
            con.execute(
                f"DELETE FROM {clean_tbl} \
                WHERE _prev_ts IS NOT NULL \
                AND DATEDIFF('hour', _prev_ts, datetime) > {max_gap_hours}"
            )
            # 4) Drop helper
            con.execute(f"ALTER TABLE {clean_tbl} DROP COLUMN _prev_ts")
            logger.info("→ %s created", clean_tbl)
        logger.info("✅ Cleaning complete.")
    except Exception:
        logger.exception("Cleaning failed")
        raise
    finally:
        con.close()

if __name__ == "__main__":
    clean()