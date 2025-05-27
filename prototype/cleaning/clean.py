# prototype/cleaning/clean.py
import pandas as pd
import duckdb

def clean(db_path="data/airquality.duckdb", output_csv="data/clean/air_quality_cleaned.csv"):
    # Read the data
    con = duckdb.connect(db_path)
    df = con.execute("SELECT * FROM air_quality_raw").df()
    con.close()

    # --- CLEANING LOGIC ---
    # Only keep the needed columns and non-null date/time
    keep_cols = ["Datetime", "Site", "NO2", "PM10", "PM2.5"]  # Update with actual column names
    df = df[keep_cols].copy()

    # Parse datetime
    df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")
    df = df.dropna(subset=["Datetime", "Site"])

    # Keep only relevant pollutants, filter negative values
    for pollutant in ["NO2", "PM10", "PM2.5"]:
        if pollutant in df.columns:
            df[pollutant] = pd.to_numeric(df[pollutant], errors="coerce")
            df = df[df[pollutant].isna() | (df[pollutant] >= 0)]

    # Save to CSV for use in Streamlit
    Path("data/clean").mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    print(f"✅ Cleaned data saved to {output_csv}")

if __name__ == "__main__":
    clean()

