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

# Air Quality (AURN) - London Bloomsbury
air_url = "https://uk-air.defra.gov.uk/data_files/site_data/aurn_hourly_blo.csv"
air_file = RAW_DIR / "aurn_hourly_blo.csv"
download_file(air_url, air_file)






# Weather (Met Office, still using example from before)
wx_url = "https://data.ceda.ac.uk/badc/ukmo-midas-open/data/uk-hourly-weather-obs/hourly_2024.csv.gz"
wx_file = RAW_DIR / "metoffice_hourly_weather_2024.csv.gz"
download_file(wx_url, wx_file)
