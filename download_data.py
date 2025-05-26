# Download raw data

import requests
from pathlib import Path

def download_file(url, dest_folder, filename=None):
    dest_folder = Path(dest_folder)
    dest_folder.mkdir(parents=True, exist_ok=True)
    if filename is None:
        filename = url.split("/")[-1]
    dest_path = dest_folder / filename
    print(f"Downloading {url} -> {dest_path}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print("✅ Downloaded:", dest_path)

if __name__ == "__main__":
    files = [
        # Air Quality UCI - NEW LINK (Figshare)
        ("https://ndownloader.figshare.com/files/5976036", "AirQualityUCI.csv"),
        # Met Office Weather Example (Heathrow 2023)
        ("https://datahub.metoffice.gov.uk/dataset/6b7eb3bf-fd49-4b8e-8cba-b0f9c64981f5/resource/8b524c54-3be6-43ed-9a31-32d8be70d44e/download/heathrow-2023.csv", "heathrow-2023.csv"),
    ]
    for url, fname in files:
        download_file(url, "data/raw", filename=fname)
