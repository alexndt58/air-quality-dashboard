```python
# prototype/cleaning/clean.py

import duckdb
import pandas as pd
from pathlib import Path

def clean(db_path: str = "data/airquality.duckdb", max_gap_hours: int = 2):
    """
    Cleans raw_aurn and raw_weather tables in the given DuckDB:
    
    - Builds a proper datetime column (from Date+time or datetime)
    - Identifies station column (site_name or SiteCode)
    - Drops invalid (negative) pollutant rows but preserves NaNs for forward-fill
    - Forward-fills missing values up to `max_gap_hours`
    - Writes out clean_aurn and clean_weather tables
    """
    # ensure output directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=db_path, read_only=False)

    # ─────────────────────── Clean AURN ───────────────────────
    df_aurn = con.execute("SELECT * FROM raw_aurn").df()
    cols_lower = {c.lower(): c for c in df_aurn.columns}

    # construct datetime
    if 'date' in cols_lower and 'time' in cols_lower:
        date_col = cols_lower['date']
        time_col = cols_lower['time']
        # drop rows lacking date or time
        df_aurn = df_aurn.dropna(subset=[date_col, time_col])
        df_aurn['datetime'] = pd.to_datetime(
            df_aurn[date_col].astype(str) + ' ' + df_aurn[time_col].astype(str),
            dayfirst=True,
            errors='coerce'
        )
        df_aurn = df_aurn.dropna(subset=['datetime'])
        # drop original date/time cols
        df_aurn.drop(columns=[date_col, time_col], inplace=True)
    elif 'datetime' in cols_lower:
        df_aurn['datetime'] = pd.to_datetime(df_aurn[cols_lower['datetime']], errors='coerce')
        df_aurn = df_aurn.dropna(subset=['datetime'])
    else:
        raise KeyError("raw_aurn missing both 'Date'/'time' and 'datetime' columns")

    # identify station identifier column
    if 'site_name' in df_aurn.columns:
        station_col = 'site_name'
    elif 'sitecode' in cols_lower:
        station_col = cols_lower['sitecode']
        df_aurn.rename(columns={station_col: 'site_name'}, inplace=True)
        station_col = 'site_name'
    else:
        raise KeyError("raw_aurn missing station identifier ('site_name' or 'SiteCode')")

    # sort and drop negative pollutant rows (keep NaNs for fill)
    df_aurn.sort_values([station_col, 'datetime'], inplace=True)
    df_aurn = df_aurn[~((df_aurn['no2'] < 0) | (df_aurn['pm25'] < 0))]

    # forward-fill within each site
    df_aurn = (
        df_aurn
        .set_index('datetime')
        .groupby(station_col, group_keys=False)
        .apply(lambda g: g.ffill(limit=max_gap_hours))
        .reset_index()
    )

    # write cleaned AURN
    con.execute("DROP TABLE IF EXISTS clean_aurn")
    con.register("df_aurn", df_aurn)
    con.execute("CREATE TABLE clean_aurn AS SELECT * FROM df_aurn")

    # ─────────────────── Clean Weather ───────────────────
    df_wx = con.execute("SELECT * FROM raw_weather").df()

    if not df_wx.empty:
        # datetime column should already be parsed correctly
        if 'datetime' in df_wx.columns:
            df_wx['datetime'] = pd.to_datetime(df_wx['datetime'], errors='coerce')
        else:
            raise KeyError("raw_weather missing 'datetime' column")

        # drop rows missing essential weather data
        df_wx = df_wx.dropna(subset=['temp', 'wind_speed', 'datetime'])
        df_wx.sort_values('datetime', inplace=True)
        # forward-fill globally
        df_wx = df_wx.set_index('datetime').ffill(limit=max_gap_hours).reset_index()
    else:
        # ensure empty clean_weather has correct schema
        df_wx = pd.DataFrame(columns=['datetime', 'temp', 'wind_speed'])

    # write cleaned weather
    con.execute("DROP TABLE IF EXISTS clean_weather")
    con.register("df_wx", df_wx)
    con.execute("CREATE TABLE clean_weather AS SELECT * FROM df_wx")

    con.close()

if __name__ == "__main__":
    clean()
    print("✅ Cleaning complete: clean_aurn & clean_weather created.")
