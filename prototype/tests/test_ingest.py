import sys
from pathlib import Path
import duckdb
import pandas as pd
import pytest

# ensure project root is on the path
PROJECT_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from prototype.ingestion.ingest import ingest

@pytest.fixture
def sample_data(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    # Minimal AURN CSV
    aurn = pd.DataFrame({
        "datetime": ["2025-01-01T00:00:00"],
        "no2": [12.3],
        "pm25": [4.5],
        "site_name": ["TestSite"],
        "latitude": [51.5],
        "longitude": [-0.1]
    })
    aurn.to_csv(raw / "aurn_hourly_test.csv", index=False)
    # *No* weather CSV to trigger fallback
    return raw

def test_ingest_creates_tables_and_empty_weather(tmp_path, sample_data):
    db = tmp_path / "test.db"
    # Run ingest on sample_data (no weather CSV present)
    ingest(raw_dir=str(sample_data), db_path=str(db))

    con = duckdb.connect(str(db), read_only=True)
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert "raw_aurn" in tables
    assert "raw_weather" in tables

    # raw_aurn has 1 row
    assert con.execute("SELECT COUNT(*) FROM raw_aurn").fetchone()[0] == 1
    # raw_weather fallback is empty
    assert con.execute("SELECT COUNT(*) FROM raw_weather").fetchone()[0] == 0

    con.close()
