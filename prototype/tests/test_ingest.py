# prototype/tests/test_ingest.py
import pandas as pd
import duckdb
import pytest
from prototype.ingestion.ingest import ingest

def test_ingest_multiple_csv_files(tmp_path):
    # 1) Create a couple of tiny CSVs under raw_dir
    raw = tmp_path / "raw"
    raw.mkdir()
    df_a = pd.DataFrame({"x": [1,2,3], "y": ["a","b","c"]})
    df_b = pd.DataFrame({"foo": [9,8], "bar": [True, False]})
    df_a.to_csv(raw / "aurn.csv", index=False)
    df_b.to_csv(raw / "met.csv", index=False)

    # 2) Run ingest against an empty DB path
    db_path = tmp_path / "test.db"
    ingest(raw_dir=str(raw), db_path=str(db_path))

    # 3) Inspect the DuckDB file
    con = duckdb.connect(str(db_path), read_only=True)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    assert "aurn" in tables
    assert "met" in tables

    # 4) Verify the contents match the CSVs
    df_db_a = con.execute("SELECT * FROM aurn ORDER BY x").df()
    pd.testing.assert_frame_equal(df_a.reset_index(drop=True), df_db_a)

    df_db_b = con.execute("SELECT * FROM met ORDER BY foo").df()
    pd.testing.assert_frame_equal(df_b.reset_index(drop=True), df_db_b)
