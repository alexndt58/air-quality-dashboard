# run_pipeline.py

from prototype.ingestion.ingest import ingest
from prototype.cleaning.clean import clean

# Ingest raw CSV into DuckDB
ingest(raw_dir="data/raw", db_path="data/airquality.duckdb")
# Clean the data, store as clean_aurn table in DB
clean(db_path="data/airquality.duckdb", max_gap_hours=2)

print("✅ Pipeline complete!")

