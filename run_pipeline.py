# run_pipeline.py

from prototype.ingestion.ingest import ingest
from prototype.cleaning.clean import clean

if __name__ == "__main__":
    ingest(raw_dir="data/raw", db_path="data/airquality.duckdb")
    clean(db_path="data/airquality.duckdb", max_gap_hours=2)
    print("✅ Pipeline complete.")
