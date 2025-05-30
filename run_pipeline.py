# run_pipeline.py

from prototype.ingestion.ingest import ingest
from prototype.cleaning.clean import clean

RAW_DIR   = "data/raw"
DB_PATH   = "data/airquality.duckdb"
MAX_GAP   = 2  # hours

if __name__ == "__main__":
    ingest(raw_dir=RAW_DIR, db_path=DB_PATH)
    clean(db_path=DB_PATH, max_gap_hours=MAX_GAP)
    print("✅ Pipeline complete.")
