# prototype/cleaning/clean.py

import pandas as pd
import os

def clean():
    infile = "data/raw/AirQualityDataHourly.csv"
    outfile = "data/clean/clean_air_quality.csv"
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    df = pd.read_csv(infile, skiprows=10)
    # Clean up whitespace in col names
    df.columns = [c.strip() for c in df.columns]
    # Combine date/time to datetime
    df["datetime"] = pd.to_datetime(df["Date"] + " " + df["Time"], errors="coerce", dayfirst=True)

    # Keep only relevant columns
    keep_cols = [
        "datetime", 
        "Nitrogen dioxide", 
        "PM10 particulate matter (Hourly measured)", 
        "PM2.5 particulate matter (Hourly measured)"
    ]
    df = df[keep_cols].copy()
    df = df.rename(columns={
        "Nitrogen dioxide": "no2",
        "PM10 particulate matter (Hourly measured)": "pm10",
        "PM2.5 particulate matter (Hourly measured)": "pm25"
    })

    # Drop rows without datetime or all pollutants missing
    df = df.dropna(subset=["datetime"], how="any")
    df = df.dropna(subset=["no2", "pm10", "pm25"], how="all")

    df.to_csv(outfile, index=False)
    print(f"✅ Cleaned data written to {outfile}")

if __name__ == "__main__":
    clean()
