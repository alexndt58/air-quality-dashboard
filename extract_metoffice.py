# extract_metoffice.py

import tarfile
from pathlib import Path

archive = Path("data/raw/metoffice_hourly_weather_2024.tar.gz")
target_dir = archive.parent

print(f"Opening archive {archive.name} in auto mode…")
with tarfile.open(archive, mode="r:*") as tar:
    tar.extractall(path=target_dir)

print("Extraction complete. Now data/raw contains:")
for p in sorted(target_dir.iterdir()):
    print("  ", p.name)

