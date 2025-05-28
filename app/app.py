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

NUM_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?")  # match both dot and comma decimals

@st.cache_data(show_spinner="📊 Loading data…")
def load_and_clean(path):
    # Detect delimiter
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        sample = f.readline()
    sep = csv.Sniffer().sniff(sample, delimiters=",;").delimiter

    # Find header row
    raw = pd.read_csv(path, sep=sep, header=None, dtype=str, na_filter=False)
    hdr_idx = raw[raw.iloc[:, 0].str.strip().str.match(r"Date", case=False)].index
    if hdr_idx.empty:
        st.error("Could not locate the 'Date' header row.")
        st.stop()
    skip = hdr_idx[0]

    # Read with real header
    df = pd.read_csv(path, sep=sep, skiprows=skip, low_memory=False)
    df.columns = df.columns.str.strip()

    # Drop Status and Unnamed
    df = df.loc[:, ~df.columns.str.contains(r"Status|^Unnamed", case=False)]

    # Build Datetime
    if {"Date", "Time"}.issubset(df.columns):
        df.insert(0, "Datetime", pd.to_datetime(
            df["Date"].str.strip() + " " + df["Time"].str.strip(),
            dayfirst=True, errors="coerce"
        ))
        df = df.drop(columns=["Date", "Time"]).dropna(subset=["Datetime"])

    # Rename pollutants by substring
    rename = {}
    for col in df.columns:
        L = col.lower()
        if "nitrogen dioxide" in L:
            rename[col] = "Nitrogen dioxide"
        elif "pm10" in L:
            rename[col] = "PM10"
        elif ("pm2.5" in L or "pm25" in L):
            rename[col] = "PM2.5"
    df = df.rename(columns=rename)

    # Force numeric conversion (comma→dot)
    for pol in ["Nitrogen dioxide", "PM10", "PM2.5"]:
        if pol in df.columns:
            df[pol] = (
                df[pol].astype(str)
                       .str.replace(",", ".", regex=False)
                       .pipe(pd.to_numeric, errors="coerce")
            )
    # Drop rows where all pollutant values are missing
    present = [pol for pol in ["Nitrogen dioxide", "PM10", "PM2.5"] if pol in df.columns]
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

df = load_and_clean(csv_path)

st.title("🌍 UK-Air Hourly Dashboard")
st.markdown(f"**Rows:** {len(df):,}")
st.dataframe(df.head(10), use_container_width=True)

if "Datetime" not in df.columns:
    st.error("No 'Datetime' column parsed — cannot plot.")
    st.stop()

# ── 1) Multi-pollutant overlay ──────────────────────────────────────
pollutants = [c for c in ["Nitrogen dioxide","PM10","PM2.5"] if c in df.columns]
selected = st.sidebar.multiselect(
    "Select pollutants to compare", pollutants, default=pollutants
)
if not selected:
    st.warning("Please select at least one pollutant.")
    st.stop()

# ── 2) Flexible rolling-window ─────────────────────────────────────
window = st.sidebar.slider(
    "Rolling window (hours)", min_value=1, max_value=168, value=24
)

# ── 3) Aggregation control ──────────────────────────────────────────
agg = st.sidebar.radio("Aggregate to…", ["raw","hourly","daily"], horizontal=True)
plot_df = df[["Datetime"] + selected].copy()
if agg != "raw":
    rule = {"hourly":"h","daily":"d"}[agg]
    plot_df = (
        plot_df.set_index("Datetime")[selected]
               .resample(rule).mean().interpolate()
               .reset_index()
    )

# Apply rolling window
if window > 1:
    plot_df = (
        plot_df.set_index("Datetime")[selected]
               .rolling(f"{window}h").mean()
               .reset_index()
    )

# ── Prepare data for plotting ───────────────────────────────────────
long_df = plot_df.melt(
    id_vars=["Datetime"], value_vars=selected,
    var_name="Pollutant", value_name="Value"
)

# ── Plot overlay chart ──────────────────────────────────────────────
chart = alt.Chart(long_df).mark_line(point=True).encode(
    x=alt.X("Datetime:T", title="Time"),
    y=alt.Y("Value:Q", title="Value"),
    color=alt.Color("Pollutant:N", title="Pollutant"),
    tooltip=["Datetime:T","Pollutant:N","Value:Q"],
).interactive().properties(height=400)

st.altair_chart(chart, use_container_width=True)

# ── Show processed table ────────────────────────────────────────────
st.dataframe(plot_df, use_container_width=True)

st.caption(
    "1️⃣ Multi-pollutant overlay · 2️⃣ Flexible rolling-window · "
    "aggregated raw/hourly/daily."
)
