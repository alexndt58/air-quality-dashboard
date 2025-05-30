# run_pipeline.py
"""
Orchestrate ingest → clean with CLI and logging.
"""
import subprocess
import sys
import click

@click.command()
@click.option(
    "--raw-dir",
    required=True,
    help="Path to raw CSV directory"
)
@click.option(
    "--db-path",
    default="data/airquality.duckdb",
    show_default=True,
    help="DuckDB database path"
)
@click.option(
    "--gap-hours",
    default=2,
    show_default=True,
    help="Max gap hours for cleaning"
)
def run_pipeline(raw_dir, db_path, gap_hours):
    """
    Run ingest and clean steps in sequence.
    Exits on first failure.
    """
    cmds = [
        f"python -m prototype.ingestion.ingest --raw-dir {raw_dir} --db-path {db_path}",
        f"python -m prototype.cleaning.clean --db-path {db_path} --max-gap-hours {gap_hours}",
    ]
    for cmd in cmds:
        print(f"▶ Running: {cmd}")
        result = subprocess.run(cmd, shell=True)
        if result.returncode != 0:
            print(f"Pipeline aborted at: {cmd}", file=sys.stderr)
            sys.exit(result.returncode)
    print("✅ Pipeline complete.")

if __name__ == "__main__":
    run_pipeline()