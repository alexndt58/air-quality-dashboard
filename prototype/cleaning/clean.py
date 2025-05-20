# prototype/cleaning/clean.py

import duckdb
import pandas as pd
from pathlib import Path

def clean(db_path: str = "data/airquality.duckdb", max_gap_hours: int = 2):
    """
    Cleans raw_aurn & raw_weather tables in the given DuckDB:

      1. Builds a consistent datetime column (from Date+time or datetime)
      2. Auto-renames station & pollutant columns to site_name, no2, pm25
      3. Drops negative pollutant readings but preserves NaNs for forward-fill
      4. Forward-fills gaps (per-site for air, globally for weather)
      5. Writes clean_aurn & clean_weather tables
    """
    # ensure DB folder exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=db_path, read_only=False)

    # ─────────────── Clean AURN ───────────────
    df = con.execute("SELECT * FROM raw_aurn").df()
    cols = {c.lower(): c for c in df.columns}

    # 1) Build datetime
    if "date" in cols and "time" in cols:
        dc, tc = cols["date"], cols["time"]
        df = df.dropna(subset=[dc, tc])
        df["datetime"] = pd.to_datetime(
            df[dc].astype(str) + " " + df[tc].astype(str),
            dayfirst=True, errors="coerce"
        )
        df = df.dropna(subset=["datetime"]).drop(columns=[dc, tc])
    elif "datetime" in cols:
        df["datetime"] = pd.to_datetime(df[cols["datetime"]], errors="coerce")
        df = df.dropna(subset=["datetime"])
    else:
        raise KeyError("raw_aurn missing both Date/time and datetime")

    # 2) Auto-rename columns
    rename_map = {}
    for col in df.columns:
        lc = col.lower()
        if "site" in lc:
            rename_map[col] = "site_name"
        if "nitrogen" in lc or "no2" in lc:
            rename_map[col] = "no2"
        if "pm" in lc and "pm10" not in lc:
            rename_map[col] = "pm25"
    df = df.rename(columns=rename_map)

    # 3) Drop negative pollutant values (keep NaNs)
    mask = pd.Series(True, index=df.index)
    if "no2" in df.columns:
        mask = mask & (df["no2"] >= 0)
    if "pm25" in df.columns:
        mask = mask & (df["pm25"] >= 0)
    df = df.loc[mask]

    # 4) Forward-fill
    df = df.set_index("datetime")
    if "site_name" in df.columns:
        df = df.groupby("site_name", group_keys=False).apply(
            lambda g: g.ffill(limit=max_gap_hours)
        )
    else:
        df = df.ffill(limit=max_gap_hours)
    df = df.reset_index()

    # 5) Write clean_aurn
    con.execute("DROP TABLE IF EXISTS clean_aurn")
    con.register("df_aurn", df)
    con.execute("CREATE TABLE clean_aurn AS SELECT * FROM df_aurn")

    # ────────────── Clean Weather ──────────────
    dfw = con.execute("SELECT * FROM raw_weather").df()
    if not dfw.empty:
        dfw["datetime"] = pd.to_datetime(dfw["datetime"], errors="coerce")
        dfw = dfw.dropna(subset=["datetime", "temp", "wind_speed"])
        dfw = dfw.set_index("datetime").ffill(limit=max_gap_hours).reset_index()
    else:
        dfw = pd.DataFrame(columns=["datetime", "temp", "wind_speed"])

    con.execute("DROP TABLE IF EXISTS clean_weather")
    con.register("df_wx", dfw)
    con.execute("CREATE TABLE clean_weather AS SELECT * FROM df_wx")

    con.close()

if __name__ == "__main__":
    clean()
    print("✅ Cleaning complete: clean_aurn & clean_weather created.")
