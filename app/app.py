# app/app.py
import streamlit as st
import pandas as pd
import re
from pathlib import Path

# ── Settings ──────────────────────────────────────────────────────────────────
DATA_PATH = Path("data/clean/clean_air_quality.csv")   # created by clean.py

# ── Helpers ───────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading data…")
def load_data(path: Path) -> pd.DataFrame:
    """Read the cleaned CSV and return a typed DataFrame."""
    df = pd.read_csv(path, parse_dates=["datetime"], dayfirst=True, low_memory=False)

    # Fail fast if the file is missing or empty
    if df.empty:
        raise ValueError("The cleaned dataset is empty. "
                         "Have you run `python run_pipeline.py`?")
    return df


def detect_pollutant_columns(df: pd.DataFrame) -> dict[str, str]:
    """
    Map pretty pollutant names → actual column names in *df*.

    Returns
    -------
    dict
        e.g. {"NO₂": "no2", "PM₁₀": "pm10", "PM₂.₅": "pm25"}
    """
    patterns = {
        "NO₂":   re.compile(r"^no[\s_\.]?2$",            re.I),
        "PM₁₀":  re.compile(r"^pm[\s_\.]?10$",           re.I),
        "PM₂.₅": re.compile(r"^pm[\s_\.]?2[\s_\.]?5$",   re.I),
    }
    result: dict[str, str] = {}
    for pretty, pat in patterns.items():
        match = next((col for col in df.columns if pat.match(col.strip())), None)
        if match:
            result[pretty] = match
    return result


# ── Main ──────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Air Quality Dashboard", layout="wide")
st.title("🌤️  Air Quality Dashboard")

# 1. Load data
try:
    df = load_data(DATA_PATH)
except Exception as e:
    st.error(f"❌ **Failed to load data:** {e}")
    st.stop()

# 2. Detect pollutant columns
pollutant_cols = detect_pollutant_columns(df)
if not pollutant_cols:
    st.error("❌ No pollutant columns recognised in the dataset. "
             "Columns present: " + ", ".join(df.columns))
    st.stop()

# 3. Sidebar – metadata & controls
with st.sidebar:
    st.header("Controls")

    # Show detected columns (handy for debugging)
    with st.expander("Columns in dataframe", expanded=False):
        st.write(list(df.columns))

    pollutant_pretty = st.selectbox("Pollutant", list(pollutant_cols.keys()))
    pollutant_col    = pollutant_cols[pollutant_pretty]

    # Optional date-range filter
    min_date, max_date = df["datetime"].min(), df["datetime"].max()
    start, end = st.date_input(
        "Date range",
        (min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )
    mask = df["datetime"].dt.date.between(start, end)
    df = df.loc[mask]

    # CSV download
    st.download_button(
        label="⬇️  Download filtered CSV",
        data=df.to_csv(index=False).encode(),
        file_name="air_quality_filtered.csv",
        mime="text/csv",
    )

# 4. Optional site / location info
site_name = next(
    (df[c].iloc[0] for c in ("Site", "Site Name", "site", "site_name") if c in df.columns),
    None,
)
if site_name:
    st.markdown(f"**Location:** {site_name}")

# 5. Time-series chart
st.subheader(f"Time series – {pollutant_pretty}")
st.line_chart(
    df.set_index("datetime")[pollutant_col],
    height=400,
    use_container_width=True,
)

# 6. Raw data preview
with st.expander("↕  Show raw data"):
    st.dataframe(df, use_container_width=True)
