import requests
from pathlib import Path

def fetch_defra_hourly_csv(save_path: Path):
    # Example DEFRA API endpoint for hourly aggregated data
    # (You’ll need to replace this with the real URL/parameters you want)
    url = (
        "https://uk-air.defra.gov.uk/data_api?"
        "module=HourlyData&"
        "region=LONDON&"
        "format=csv"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    save_path.write_bytes(resp.content)
    print(f"Saved latest DEFRA data to {save_path}")

if __name__ == "__main__":
    target = Path("data") / "raw" / "AirQualityDataHourly.csv"
    fetch_defra_hourly_csv(target)
