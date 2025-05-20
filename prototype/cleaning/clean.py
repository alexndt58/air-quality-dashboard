# prototype/cleaning/clean.py

import duckdb
import pandas as pd


def clean(db_path: str = "data/airquality.duckdb", max_gap_hours: int = 2):
    """
    Cleans raw_aurn and raw_weather tables in the given DuckDB:

    - Builds a proper datetime column
    - Drops invalid (negative) pollutant rows but preserves NaNs for forward-fill
    - Forward-fills missing values up to `max_gap_hours`
    - Writes tables clean_aurn and clean_weather
    """
    con = duckdb.connect(database=db_path, read_only=False)

    # ──────────────────────── Clean AURN ────────────────────────
    df_aurn = con.execute("SELECT * FROM raw_aurn").df()

    # Detect column names in lower-case map
    cols_lower = {c.lower(): c for c in df_aurn.columns}

    if "date" in cols_lower and "time" in cols_lower:
        date_col = cols_lower["date"]
        time_col = cols_lower["time"]

        # Drop rows that lack either Date or time (can't build datetime)
        df_aurn = df_aurn.dropna(subset=[date_col, time_col])

        # Build datetime (AURN dates are DD/MM/YYYY)
        df_aurn["datetime"] = pd.to_datetime(
            df_aurn[date_col].astype(str) + " " + df_aurn[time_col].astype(str),
            dayfirst=True,
            errors="coerce",
        )
        df_aurn = df_aurn.dropna(subset=["datetime"])
    elif "datetime" in cols_lower:
        df_aurn["datetime"] = pd.to_datetime(df_aurn[cols_lower["datetime"]])
    else:
        raise KeyError(
            "raw_aurn table missing both a unified 'datetime' and separate 'Date'/'time' columns"
        )

    # Sort and (optionally) drop original Date/time columns
    df_aurn.sort_values(["site_name", "datetime"], inplace=True)
    for c in [cols_lower.get("date"), cols_lower.get("time")]:
        if c in df_aurn.columns:
            df_aurn.drop(columns=[c], inplace=True)

    # Drop negative pollutant rows (keep NaNs for ffill)
    mask_valid = ~((df_aurn["no2"] < 0) | (df_aurn["pm25"] < 0))
    df_aurn = df_aurn[mask_valid]

    # Forward-fill within each site, up to `max_gap_hours`
    df_aurn = (
        df_aurn.set_index("datetime")
        .groupby("site_name", group_keys=False)
        .apply(lambda g: g.ffill(limit=max_gap_hours))
        .reset_index()
    )

    # Write cleaned AURN
    con.execute("DROP TABLE IF EXISTS clean_aurn")
    con.register("df_aurn", df_aurn)
    con.execute("CREATE TABLE clean_aurn AS SELECT * FROM df_aurn")

    # ──────────────────── Clean Weather ────────────────────
    df_wx = con.execute("SELECT * FROM raw_weather").df()
    if not df_wx.empty:
        df_wx["datetime"] = pd.to_datetime(df_wx["datetime"])
        df_wx.sort_values("datetime", inplace=True)
        df_wx = df_wx.dropna(subset=["temp", "wind_speed"])
        df_wx = df_wx.set_index("datetime").ffill(limit=max_gap_hours).reset_index()
    else:
        # Ensure correct columns even if raw_weather was empty
        df_wx = pd.DataFrame(columns=["datetime", "temp", "wind_speed"])

    con.execute("DROP TABLE IF EXISTS clean_weather")
    con.register("df_wx", df_wx)
    con.execute("CREATE TABLE clean_weather AS SELECT * FROM df_wx")

    con.close()


if __name__ == "__main__":
    clean()
    print("✅ Cleaning complete: clean_aurn & clean_weather created.")
