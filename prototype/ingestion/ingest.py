import duckdb
import re
from pathlib import Path

def ingest(raw_dir: str = "data/raw", db_path: str = "data/airquality.duckdb"):
    raw_path = Path(raw_dir)
    con = duckdb.connect(database=db_path, read_only=False)

        # --- raw_aurn ingestion (skip metadata for real AURN files) ---
    # at the top, add:

# Replace your current AURN ingestion section with:
aurn_files = sorted(raw_path.glob("*aurn*.csv*"))
if aurn_files:
    con.execute("DROP TABLE IF EXISTS raw_aurn")
    for idx, f in enumerate(aurn_files):
        # skip 3 lines only for official AURN files named aurn_hourly_<year>
        skip = 3 if re.match(r"aurn_hourly_\d{4}", f.name) else 0
        sql = f"""
            {'CREATE' if idx == 0 else 'INSERT'} TABLE raw_aurn 
            { 'AS' if idx == 0 else '' }
            SELECT * FROM read_csv_auto(
              '{f}', 
              skip={skip},
              delim=',',
              ignore_errors=true,
              null_padding=true
            )
        """
        con.execute(sql)
else:
    print("⚠️  No AURN CSVs found in", raw_dir)



    # --- raw_weather ingestion (empty fallback) ---
    wx_files = sorted(raw_path.glob("metoffice*.csv*"))
    con.execute("DROP TABLE IF EXISTS raw_weather")
    if wx_files:
        con.execute(f"""
            CREATE TABLE raw_weather AS
            SELECT * FROM read_csv_auto('{wx_files[0]}')
        """)
        for f in wx_files[1:]:
            con.execute(f"""
                INSERT INTO raw_weather
                SELECT * FROM read_csv_auto('{f}')
            """)
    else:
        print("⚠️  No Met Office CSVs found in", raw_dir)
        # create empty table (same schema as raw_aurn, zero rows)
        con.execute("""
            CREATE TABLE raw_weather AS
            SELECT * FROM raw_aurn WHERE FALSE
        """)

    con.close()
    print("✅ Ingestion complete. Tables raw_aurn and raw_weather are in", db_path)


if __name__ == "__main__":
    ingest()

