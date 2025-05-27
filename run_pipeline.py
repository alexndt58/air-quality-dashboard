# run_pipeline.py
from prototype.ingestion.ingest import ingest
from prototype.cleaning.clean import clean

if __name__ == "__main__":
    ingest()
    clean()
    print("✅ Pipeline complete.")


