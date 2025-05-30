# # prototype/cleaning/clean.py
# """
# Clean tables inside DuckDB: remove gaps > max_gap_hours, drop empty, and produce cleaned tables.
# """
# import duckdb
# import click
# import logging
# from pathlib import Path

# logger = logging.getLogger(__name__)

# @click.command()
# @click.option(
#     "--db-path",
#     required=True,
#     type=click.Path(exists=True, path_type=Path),
#     help="DuckDB file path to read/write"
# )
# @click.option(
#     "--max-gap-hours",
#     default=2,
#     show_default=True,
#     type=float,
#     help="Maximum allowed gap between consecutive timestamps"
# )
# def clean(db_path: Path, max_gap_hours: float):
#     """
#     Read each raw table, enforce max gap, drop fully-null rows, and store as clean_<name>.
#     """
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s %(levelname)s %(message)s"
#     )

#     db_path = db_path.resolve()
#     con = duckdb.connect(str(db_path))
#     try:
#         tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
#         raw_tables = [t for t in tables if not t.startswith('clean_')]

#         for tbl in raw_tables:
#             clean_tbl = f"clean_{tbl}"
#             logger.info("Cleaning table '%s' → '%s'", tbl, clean_tbl)
#             # 1) Check for datetime column
#             info = con.execute(f"DESCRIBE {tbl}").df()
#             if 'datetime' not in info.column_name.str.lower().tolist():
#                 logger.warning("Skipping %s: no datetime column", tbl)
#                 continue
#             # 2) Build cleaned table
#             con.execute(f"DROP TABLE IF EXISTS {clean_tbl}")
#             con.execute(
#                 f"CREATE TABLE {clean_tbl} AS\
#                 SELECT * FROM {tbl} \
#                 WHERE datetime IS NOT NULL"
#             )
#             # 3) Optionally filter gaps
#             con.execute(
#                 f"ALTER TABLE {clean_tbl} ADD COLUMN _prev_ts TIMESTAMP;"
#             )
#             con.execute(
#                 f"UPDATE {clean_tbl} SET _prev_ts = LAG(datetime) OVER (ORDER BY datetime)"
#             )
#             con.execute(
#                 f"DELETE FROM {clean_tbl} \
#                 WHERE _prev_ts IS NOT NULL \
#                 AND DATEDIFF('hour', _prev_ts, datetime) > {max_gap_hours}"
#             )
#             # 4) Drop helper
#             con.execute(f"ALTER TABLE {clean_tbl} DROP COLUMN _prev_ts")
#             logger.info("→ %s created", clean_tbl)
#         logger.info("✅ Cleaning complete.")
#     except Exception:
#         logger.exception("Cleaning failed")
#         raise
#     finally:
#         con.close()

# if __name__ == "__main__":
#     clean()

######################################

# prototype/cleaning/clean.py

import duckdb
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
import numpy as np
import logging
from datetime import datetime

# ── Logger setup ───────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

def clean(db_path: str, max_gap_hours: int = 2):
    """
    Read every raw_* table from the DuckDB at db_path,
    enforce types & ranges, drop/flag large gaps, interpolate small gaps,
    and write out clean_<tablename> tables. Also gathers metrics
    about each table’s cleaning process into a clean_metrics table.
    """
    con = duckdb.connect(db_path)

    # discover all tables that need cleaning
    raw_tables = [
        row[0]
        for row in con.execute("SHOW TABLES").fetchall()
        if not row[0].startswith("clean_") and row[0] != "clean_metrics"
    ]

    metrics = []  # will hold one dict per table

    for tbl in raw_tables:
        logger.info(f"➡️  Cleaning table `{tbl}`")

        # load raw
        df = con.execute(f"SELECT * FROM {tbl}").df()
        rows_before = len(df)

        # coerce & drop bad datetimes
        if "datetime" not in df.columns:
            logger.warning(f"  • `{tbl}` has no 'datetime' column; skipping")
            continue
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df = df.dropna(subset=["datetime"]).reset_index(drop=True)
        rows_after_dt = len(df)

        # build and apply schema
        schema_cols = {"datetime": Column(pa.DateTime, nullable=False)}
        for c in df.columns:
            if c == "datetime":
                continue
            schema_cols[c] = Column(pa.Float, nullable=True, checks=Check.ge(0))
        schema = DataFrameSchema(schema_cols)
        validated = schema.validate(df, lazy=True)
        rows_after_schema = len(validated)

        # detect gaps
        validated = validated.sort_values("datetime")
        diffs = validated["datetime"].diff().dt.total_seconds().div(3600)
        large_gaps = diffs[diffs > max_gap_hours].count()

        # count nulls before interpolation
        numeric_cols = validated.select_dtypes(include=[np.number]).columns
        nulls_before = int(validated[numeric_cols].isna().sum().sum())

        # interpolate small gaps
        validated = validated.set_index("datetime")
        validated[numeric_cols] = validated[numeric_cols].interpolate(
            limit=int(max_gap_hours),
            limit_direction="both",
        )
        validated = validated.reset_index()

        # count nulls after interpolation
        nulls_after = int(validated[numeric_cols].isna().sum().sum())
        interpolated = nulls_before - nulls_after

        # write cleaned table
        clean_name = f"clean_{tbl}"
        con.execute(f"DROP TABLE IF EXISTS {clean_name}")
        con.register("tmp_df", validated)
        con.execute(f"CREATE TABLE {clean_name} AS SELECT * FROM tmp_df")
        con.unregister("tmp_df")
        logger.info(f"✅ Created `{clean_name}` ({len(validated)} rows)")

        # record metrics
        metrics.append({
            "table": tbl,
            "rows_before": rows_before,
            "rows_after_dt": rows_after_dt,
            "rows_after_schema": rows_after_schema,
            "large_gaps_detected": int(large_gaps),
            "nulls_before_interp": nulls_before,
            "nulls_after_interp": nulls_after,
            "values_interpolated": int(interpolated),
            "cleaned_rows": len(validated),
            "run_timestamp": datetime.utcnow()
        })

    # write out metrics table
    if metrics:
        mdf = pd.DataFrame(metrics)
        con.execute("DROP TABLE IF EXISTS clean_metrics")
        con.register("m", mdf)
        con.execute("CREATE TABLE clean_metrics AS SELECT * FROM m")
        con.unregister("m")
        logger.info("ℹ️  Written `clean_metrics` table with cleaning stats")

    con.close()
    logger.info("🎉 Cleaning complete.")
