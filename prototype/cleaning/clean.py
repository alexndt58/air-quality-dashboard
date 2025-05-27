# prototype/cleaning/clean.py

import pandas as pd
from pathlib import Path

def clean(input_path="data/raw/AirQualityDataHourly.csv", output_path="data/clean/clean_air_quality.csv"):
    """
    Cleans the AirQualityDataHourly.csv:
    - Skips metadata/header rows
    - Combines 'Date' and 'Time' to a single 'datetime'
    - Keeps and renames NO2, PM10, PM2.5 columns
    - Saves cleaned file as output_path (CSV)
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # 1. Read skipping metadata (usually first 10 lines in UK-AIR CSVs)
    df = pd.read_csv(input_path, skiprows=11)
    
    # 2. Standardize columns (strip whitespace, fix names)
    df.columns = [col.strip() for col in df.columns]
    # Rename columns for consistency
    rename_dict = {
        "Date": "date",
        "Time": "time",
        "Nitrogen dioxide": "no2",
        "PM10 particulate matter (Hourly measured)": "pm10",
        "PM2.5 particulate matter (Hourly measured)": "pm25",
        # Status columns are not needed for main time series analysis
    }
    df = df.rename(columns=rename_dict)

    # 3. Drop 'Status' columns if present
    df = df[[c for c in df.columns if "Status" not in c]]

    # 4. Combine date & time into a single datetime column
    df["datetime"] = pd.to_datetime(df["date"] + " " + df["time"], errors="coerce", dayfirst=True)

    # 5. Keep relevant columns only (adjust as needed)
    keep_cols = ["datetime", "no2", "pm10", "pm25"]
    df_clean = df[keep_cols].copy()

    # 6. Drop rows with missing datetime or pollutant data (optional)
    df_clean = df_clean.dropna(subset=["datetime"])  # Drop rows without valid datetime

    # 7. Save cleaned data to CSV
    df_clean.to_csv(output_path, index=False)
    print(f"✅ Cleaned data saved to {output_path}")

if __name__ == "__main__":
    clean()


