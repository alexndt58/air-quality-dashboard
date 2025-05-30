# run_pipeline.py
from prototype.ingestion.ingest import ingest
from prototype.cleaning.clean import clean_db

if __name__ == "__main__":
    # adjust paths as you like
    raw_dir = "data/raw"
    db_path = "data/airquality.duckdb"
    clean_dir = "data/clean"

    ingest(raw_dir=raw_dir, db_path=db_path)
    clean_db(db_path=db_path, output_dir=clean_dir)
    print("✅ Pipeline complete.")
