# Download raw data

import requests
from pathlib import Path

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def download_file(url, dest):
    dest = Path(dest)
    if dest.exists():
        print(f"✅ Already downloaded: {dest}")
        return
    print(f"Downloading {url} -> {dest}")
    r = requests.get(url, stream=True)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"✅ Downloaded: {dest}")

# 1. DEFRA air quality (London Bloomsbury NO2 for 2023)
# Direct download link for ratified hourly NO2 (adjust year as needed)
air_quality_url = "https://uk-air.defra.gov.uk/assets/data/atest/automatic-urban-and-rural-network/bloomhourly2023.csv"
air_quality_file = RAW_DIR / "london_bloomsbury_no2_2023.csv"

# 2. Met Office Heathrow weather (2023)
weather_url = "https://datahub.metoffice.gov.uk/dataset/6b7eb3bf-fd49-4b8e-8cba-b0f9c64981f5/resource/8b524c54-3be6-43ed-9a31-32d8be70d44e/download/heathrow-2023.csv"
weather_file = RAW_DIR / "heathrow-2023.csv"

download_file(air_quality_url, air_quality_file)
download_file(weather_url, weather_file)
