# prototype/cleaning/clean.py

import duckdb
import pandas as pd

def clean(db_path: str, max_gap_hours: float):
    """
    For each raw table in the DuckDB at db_path,
    drop any row whose time‐gap to the previous > max_gap_hours,
    and write it out as clean_<table>.
    """
    con = duckdb.connect(db_path)
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]

    for tbl in tables:
        if tbl.startswith("clean_"):
            continue

        # Load into pandas
        df = con.execute(f"SELECT * FROM {tbl}").df()

        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df = df.dropna(subset=["datetime"])
            df = df.sort_values("datetime")
            # compute hourly gaps
            df["__diff"] = df["datetime"].diff().dt.total_seconds() / 3600.0
            # keep first row (diff NaN) or diff ≤ max_gap
            df = df[df["__diff"].isna() | (df["__diff"] <= max_gap_hours)]
            df = df.drop(columns="__diff")

        cleaned_tbl = f"clean_{tbl}"
        con.execute(f"DROP TABLE IF EXISTS {cleaned_tbl}")
        # register the cleaned dataframe and persist it
        con.register("tmp_df", df)
        con.execute(f"CREATE TABLE {cleaned_tbl} AS SELECT * FROM tmp_df")
        con.unregister("tmp_df")
        print(f"• Cleaned table `{cleaned_tbl}` created from `{tbl}`")

    con.close()
    print(f"✅ Cleaning complete. Clean tables written to {db_path}")
