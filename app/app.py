import streamlit as st
import pandas as pd
import altair as alt
import csv, re, warnings
from pathlib import Path

# ── Basic Streamlit setup ──────────────────────────────────────────
st.set_page_config("Air-Quality Dashboard", "🌍", layout="wide")
warnings.filterwarnings("ignore", category=UserWarning)

ROOT        = Path(__file__).resolve().parents[1]
DEFAULT_CSV = ROOT / "data" / "raw" / "AirQualityDataHourly.csv"

NUM_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?")  # matches dot or comma decimals

@st.cache_data(show_spinner="📊 Loading data…")
def load_and_clean(path):
    # Sniff delimiter from first line
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        sample = f.readline()
    sep = csv.Sniffer().sniff(sample, delimiters=",;").delimiter

    # Raw read to find header row
    raw = pd.read_csv(path, sep=sep, header=None, dtype=str, na_filter=False)
    hdr_idx = raw[raw.iloc[:, 0].str.strip().str.match(r"Date", case=False)].index
    if hdr_idx.empty:
        st.error("Could not locate the 'Date' header row.")
        st.stop()
    skip = hdr_idx[0]

    # Read with real header
    df = pd.read_csv(path, sep=sep, skiprows=skip, low_memory=False)
    df.columns = df.columns.str.strip()

    # Drop Status and Unnamed columns
    df = df.loc[:, ~df.columns.str.contains(r"Status|^Unnamed", case=False)]

    # Build Datetime
    if {"Date", "Time"}.issubset(df.columns):
        df.insert(0, "Datetime", pd.to_datetime(
            df["Date"].str.strip() + " " + df["Time"].str.strip(),
            dayfirst=True, errors="coerce"
        ))
        df = df.drop(columns=["Date", "Time"]).dropna(subset=["Datetime"])

    # Rename pollutant columns by substring
    rename = {}
    for col in df.columns:
        low = col.lower()
        if "nitrogen dioxide" in low:
            rename[col] = "Nitrogen dioxide"
        elif "pm10" in low:
            rename[col] = "PM10"
    df = df.rename(columns=rename)

    # Force pollutants to numeric (comma→dot)
    for pol in ["Nitrogen dioxide", "PM10"]:
        if pol in df.columns:
            df[pol] = (
                df[pol].astype(str)
                      .str.replace(",", ".", regex=False)
                      .pipe(pd.to_numeric, errors="coerce")
            )

    # Drop rows where both pollutant values are missing
    present = [pol for pol in ["Nitrogen dioxide", "PM10"] if pol in df.columns]
    if present:
        df = df.dropna(subset=present, how="all")

    # Sort chronologically
    if "Datetime" in df.columns:
        df = df.sort_values("Datetime")

    return df

# ── Load data ───────────────────────────────────────────────────────
upload = st.sidebar.file_uploader("Upload UK-Air CSV", type="csv")
if upload is not None:
    tmp = Path("_tmp.csv")
    tmp.write_bytes(upload.getbuffer())
    csv_path = str(tmp)
else:
    csv_path = str(DEFAULT_CSV)

# Read & clean
try:
    df = load_and_clean(csv_path)
except Exception as e:
    st.error(f"Error loading CSV: {e}")
    st.stop()

st.title("🌍 UK-Air Hourly Dashboard")
st.markdown(f"**Rows:** {len(df):,}")
st.dataframe(df.head(10), use_container_width=True)

if "Datetime" not in df.columns:
    st.error("No Datetime column parsed — cannot plot.")
    st.stop()

# ── Controls ───────────────────────────────────────────────────────
pollutants = [pol for pol in ["Nitrogen dioxide", "PM10"] if pol in df.columns]
counts = {pol: df[pol].notna().sum() for pol in pollutants}
choice = st.sidebar.selectbox(
    "Pollutant", [f"{pol} ({counts[pol]} valid)" for pol in pollutants]
)
pol = choice.split(" (")[0]

if counts[pol] == 0:
    st.error(f"No valid data for {pol}.")
    st.stop()

agg = st.sidebar.radio("Aggregate", ["raw", "hourly", "daily"], horizontal=True)
plot_df = df[["Datetime", pol]].copy()
if agg != "raw":
    rule = {"hourly": "h", "daily": "d"}[agg]
    plot_df = (
        plot_df.set_index("Datetime")[pol]
               .resample(rule).mean().interpolate()
               .reset_index()
    )

# ── Plot & Table ──────────────────────────────────────────────────
st.altair_chart(
    alt.Chart(plot_df)
       .mark_line(point=True)
       .encode(
           x=alt.X("Datetime:T"),
           y=alt.Y(f"{pol}:Q"),
           tooltip=[alt.Tooltip("Datetime:T"), alt.Tooltip(f"{pol}:Q")],
       )
       .interactive()
       .properties(height=400),
    use_container_width=True,
)
st.dataframe(plot_df, use_container_width=True)

st.caption("Metadata skipped; Status/Unnamed dropped; pollutants coerced; live refresh every 60 s.")
