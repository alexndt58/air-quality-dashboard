# prototype/cleaning/clean.py

import pandas as pd
from pathlib import Path

RAW_FILE = Path("data/raw/AirQualityDataHourly.csv")
CLEAN_FILE = Path("data/clean/clean_air_quality.csv")

def clean(infile: Path = RAW_FILE, outfile: Path = CLEAN_FILE, skiprows: int = 10) -> None:
    """
    Load raw hourly air‑quality readings, perform light cleaning/normalisation,
    and save a compact CSV ready for analysis.

    Steps
    -----
    1. Read the CSV (skipping the 10‑row metadata header that comes with LAQN downloads).
    2. Strip whitespace from column names.
    3. Combine the separate *Date* and *Time* columns into a single `datetime` column.
    4. Keep only NO₂, PM₁₀, PM₂.₅ and the timestamp, renaming them to snake‑case.
    5. Coerce pollutant readings to numeric and drop rows with no valid data.
    """
    if not infile.exists():
        raise FileNotFoundError(f"Input file not found: {infile.absolute()}")

    # read raw data
    df = pd.read_csv(infile, skiprows=skiprows, low_memory=False)
    df.columns = [c.strip() for c in df.columns]

    # Build timestamp
    df["datetime"] = pd.to_datetime(
        df["Date"].str.strip() + " " + df["Time"].str.strip(),
        errors="coerce",
        dayfirst=True,
    )

    # Select and rename
    col_map = {
        "datetime": "datetime",
        "Nitrogen dioxide": "no2",
        "PM10 particulate matter (Hourly measured)": "pm10",
        "PM2.5 particulate matter (Hourly measured)": "pm25",
    }
    df = df[list(col_map.keys())].rename(columns=col_map)

    # Ensure numeric values
    for col in ("no2", "pm10", "pm25"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop invalid rows
    df = (
        df.dropna(subset=["datetime"])
        .dropna(subset=["no2", "pm10", "pm25"], how="all")
        .sort_values("datetime")
        .reset_index(drop=True)
    )

    # Ensure output directory exists
    outfile.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outfile, index=False)
    print(f"✅ Cleaned data written to {outfile}")

if __name__ == "__main__":
    clean()
