# prototype/tests/test_clean.py

import sys
from pathlib import Path
import duckdb
import pandas as pd
import pytest

# ensure project root is on path
PROJECT_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from prototype.ingestion.ingest import ingest
from prototype.cleaning.clean import clean

@pytest.fixture
def sample_db(tmp_path):
    # Prepare raw data folder
    raw = tmp_path / "raw"
    raw.mkdir()
    # AURN: two rows with a gap and a None
    aurn = pd.DataFrame({
        "datetime": ["2025-01-01T00:00:00", "2025-01-01T02:00:00"],
        "no2": [10.0, None],
        "pm25": [5.0, 7.0],
        "site_name": ["S1", "S1"],
        "latitude": [0, 0],
        "longitude": [0, 0],
    })
    aurn.to_csv(raw / "aurn.csv", index=False)
    # Weather: single valid row
    wx = pd.DataFrame({
        "datetime": ["2025-01-01T00:00:00"],
        "temp": [15.0],
        "wind_speed": [3.5],
    })
    wx.to_csv(raw / "met.csv", index=False)

    # Ingest into a fresh DB
    db = tmp_path / "test.db"
    ingest(raw_dir=str(raw), db_path=str(db))
    return db

def test_clean_behavior(sample_db):
    # Run cleaning
    clean(db_path=str(sample_db), max_gap_hours=2)

    con = duckdb.connect(str(sample_db), read_only=True)
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert "clean_aurn" in tables
    assert "clean_weather" in tables

    # AURN: missing no2 should be forward-filled to 10.0
    vals = con.execute("SELECT no2 FROM clean_aurn ORDER BY datetime").fetchall()
    assert [v[0] for v in vals] == [10.0, 10.0]

    # Weather: only one row, so count == 1
    cnt = con.execute("SELECT COUNT(*) FROM clean_weather").fetchone()[0]
    assert cnt == 1

    con.close()
