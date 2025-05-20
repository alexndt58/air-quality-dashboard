# prototype/cleaning/clean.py

import duckdb
import pandas as pd


def clean(db_path: str = "data/airquality.duckdb", max_gap_hours: int = 2):
    """
    Cleans raw_aurn and raw_weather tables in the given DuckDB:
    - Parses/constructs a unified datetime
    - Drops invalid (negative) values only, preserves NaNs for forward fill
    - Forward-fills missing values up to max_gap_hours
    - Writes tables clean_aurn and clean_weather
    """
    con = duckdb.connect(database=db_path, read_only=False)

    # --- Clean AURN ---
    df_aurn = con.execute("SELECT * FROM raw_aurn").df()

    # Construct datetime column (handle separate 'Date' and 'time' if present)
    cols_lower = {c.lower(): c for c in df_aurn.columns}
    if 'date' in cols_lower and 'time' in cols_lower:
        date_col = cols_lower['date']
        time_col = cols_lower['time']
        df_aurn['datetime'] = pd.to_datetime(df_aurn[date_col].astype(str) + ' ' + df_aurn[time_col].astype(str))
    elif 'datetime' in cols_lower:
        df_aurn['datetime'] = pd.to_datetime(df_aurn[cols_lower['datetime']])
    else:
        raise KeyError("raw_aurn table missing both 'datetime' and separate 'Date'/'time' columns")

    # Sort and drop original date/time columns
    df_aurn.sort_values(['site_name','datetime'], inplace=True)
    for c in [cols_lower.get('date'), cols_lower.get('time')]:
        if c in df_aurn.columns:
            df_aurn.drop(columns=[c], inplace=True)

    # Drop rows with negative pollutant values, preserve NaNs
    mask_valid = ~((df_aurn['no2'] < 0) | (df_aurn['pm25'] < 0))
    df_aurn = df_aurn[mask_valid]

    # Forward fill per site
    df_aurn = df_aurn.set_index('datetime')
    df_aurn = (
        df_aurn.groupby('site_name', group_keys=False)
               .apply(lambda g: g.ffill(limit=max_gap_hours))
    )
    df_aurn = df_aurn.reset_index()

    con.execute("DROP TABLE IF EXISTS clean_aurn")
    con.register("df_aurn", df_aurn)
    con.execute("CREATE TABLE clean_aurn AS SELECT * FROM df_aurn")

    # --- Clean Weather ---
    df_wx = con.execute("SELECT * FROM raw_weather").df()
    df_wx['datetime'] = pd.to_datetime(df_wx['datetime'])
    df_wx.sort_values('datetime', inplace=True)
    df_wx = df_wx.dropna(subset=['temp','wind_speed'])
    df_wx = df_wx.set_index('datetime').ffill(limit=max_gap_hours).reset_index()

    con.execute("DROP TABLE IF EXISTS clean_weather")
    con.register('df_wx', df_wx)
    con.execute("CREATE TABLE clean_weather AS SELECT * FROM df_wx")

    con.close()


if __name__ == '__main__':
    clean()
    print("✅ Cleaning complete: clean_aurn & clean_weather created.")
