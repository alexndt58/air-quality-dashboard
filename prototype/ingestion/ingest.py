# prototype/cleaning/clean.py

import duckdb
import pandas as pd


def clean(db_path: str = "data/airquality.duckdb", max_gap_hours: int = 2):
    """
    Cleans raw_aurn and raw_weather in place:
    - Drops invalid (negative) values
    - Forward-fills gaps up to max_gap_hours
    - Writes clean_aurn and clean_weather tables
    """
    con = duckdb.connect(database=db_path, read_only=False)

    # --- Clean AURN ---
    df_aurn = con.execute("SELECT * FROM raw_aurn").df()
    df_aurn["datetime"] = pd.to_datetime(df_aurn["datetime"])
    df_aurn.sort_values(["site_name", "datetime"], inplace=True)
    # Drop negative values but keep NaNs for forward fill
    df_aurn = df_aurn[~((df_aurn.no2 < 0) | (df_aurn.pm25 < 0))]
    # Forward-fill per site without duplicating 'site_name'
    df_aurn = df_aurn.set_index("datetime")
    df_aurn = df_aurn.groupby("site_name", group_keys=False).apply(
        lambda g: g.ffill(limit=max_gap_hours)
    )
    df_aurn = df_aurn.reset_index()
    con.execute("DROP TABLE IF EXISTS clean_aurn")
    con.register("df_aurn", df_aurn)
    con.execute("CREATE TABLE clean_aurn AS SELECT * FROM df_aurn")

    # --- Clean Weather ---
    df_wx = con.execute("SELECT * FROM raw_weather").df()
    df_wx["datetime"] = pd.to_datetime(df_wx["datetime"])
    df_wx.sort_values("datetime", inplace=True)
    # Drop rows with missing values (we assume no negative values in weather)
    df_wx = df_wx.dropna(subset=["temp", "wind_speed"])
    # Forward-fill globally
    df_wx = df_wx.set_index("datetime").ffill(limit=max_gap_hours).reset_index()
    con.execute("DROP TABLE IF EXISTS clean_weather")
    con.register("df_wx", df_wx)
    con.execute("CREATE TABLE clean_weather AS SELECT * FROM df_wx")

    con.close()


if __name__ == "__main__":
    clean()
    print("✅ Cleaning complete: clean_aurn & clean_weather created")
