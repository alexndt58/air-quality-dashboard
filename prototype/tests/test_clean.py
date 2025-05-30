# prototype/tests/test_clean.py
import pandas as pd
import duckdb
import pytest
from prototype.ingestion.ingest import ingest
from prototype.cleaning.clean import clean

@pytest.fixture
def sample_db(tmp_path):
    """
    Prepare a DuckDB with two raw tables (aurn and met), then return its path.
    """
    raw = tmp_path / "raw"
    raw.mkdir()
    # AURN: two rows, one with a gap and a None
    aurn = pd.DataFrame({
        "datetime": ["2025-01-01T00:00:00", "2025-01-01T02:00:00"],
        "no2": [10.0, None],
        "pm25": [5.0, 7.0],
    })
    aurn.to_csv(raw / "aurn.csv", index=False)

    # MET: single valid row
    met = pd.DataFrame({
        "datetime": ["2025-01-01T00:00:00"],
        "temp": [15.0],
        "wind_speed": [3.5],
    })
    met.to_csv(raw / "met.csv", index=False)

    db = tmp_path / "test.db"
    ingest(raw_dir=str(raw), db_path=str(db))
    return str(db)

def test_clean_creates_clean_tables(sample_db):
    # Run the cleaning step
    clean(db_path=sample_db, max_gap_hours=2)

    con = duckdb.connect(sample_db, read_only=True)
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}

    # Should have created clean_<source> for each raw table
    assert "clean_aurn" in tables
    assert "clean_met" in tables

    # Spot-check aurn cleaning: datetime gaps >2h should be dropped,
    # and rows with all pollutants null dropped
    df_clean_aurn = con.execute("SELECT * FROM clean_aurn ORDER BY datetime").df()
    # The second row had no2=None but pm25=7→ should survive.
    assert len(df_clean_aurn) == 2

    # Spot-check met cleaning: original had 1 row → should still be 1
    df_clean_met = con.execute("SELECT * FROM clean_met").df()
    assert len(df_clean_met) == 1
