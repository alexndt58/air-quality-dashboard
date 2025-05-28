import streamlit as st
import pandas as pd
import altair as alt
import csv, re, warnings, calendar
from pathlib import Path

# ── Basic Streamlit setup ──────────────────────────────────────────
st.set_page_config("Air-Quality Dashboard", "🌍", layout="wide")
warnings.filterwarnings("ignore", category=UserWarning)

ROOT        = Path(__file__).resolve().parents[1]
DEFAULT_CSV = ROOT / "data" / "raw" / "AirQualityDataHourly.csv"
NUM_RE      = re.compile(r"[-+]?\d+(?:[.,]\d+)?")

@st.cache_data(show_spinner="📊 Loading data…")
def load_and_clean(path):
    # Detect delimiter from first line
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

    # Drop Status and Unnamed filler columns
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
        elif "pm2.5" in low or "pm25" in low:
            rename[col] = "PM2.5"
    df = df.rename(columns=rename)

    # Force pollutant columns to numeric (comma→dot)
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

# Ensure datetime column exists
if "Datetime" not in df.columns:
    st.error("No 'Datetime' column parsed — cannot proceed.")
    st.stop()

# ── Sidebar Controls & Downloads ────────────────────────────────────
st.sidebar.download_button(
    "Download filtered raw data",
    df.to_csv(index=False).encode(),
    file_name="aq_raw_filtered.csv"
)

pollutants = [c for c in ["Nitrogen dioxide", "PM10", "PM2.5"] if c in df.columns]
selected = st.sidebar.multiselect(
    "Select pollutants to compare", pollutants, default=pollutants
)
if not selected:
    st.warning("Please select at least one pollutant.")
    st.stop()

window = st.sidebar.slider("Rolling window (hours)", 1, 168, 24)

thresholds = {}
for pol in selected:
    thresholds[pol] = st.sidebar.number_input(
        f"Alert threshold for {pol}",
        value=float(df[pol].median() or 0.0)
    )

agg = st.sidebar.radio("Aggregate to…", ["raw", "hourly", "daily"], horizontal=True)

# ── Data Processing ─────────────────────────────────────────────────
plot_df = df[["Datetime"] + selected].copy()
if agg != "raw":
    rule = {"hourly": "h", "daily": "d"}[agg]
    plot_df = (
        plot_df.set_index("Datetime")[selected]
               .resample(rule).mean().interpolate()
               .reset_index()
    )
if window > 1:
    plot_df = (
        plot_df.set_index("Datetime")[selected]
               .rolling(f"{window}h").mean()
               .reset_index()
    )

st.sidebar.download_button(
    "Download aggregated data",
    plot_df.to_csv(index=False).encode(),
    file_name="aq_agg_filtered.csv"
)

# ── KPI Cards & Alerts ──────────────────────────────────────────────
st.title("🌍 UK-Air Hourly Dashboard")
st.markdown(f"**Rows:** {len(df):,}")
st.dataframe(df.head(10), use_container_width=True)

cols = st.columns(len(selected))
for col, pol in zip(cols, selected):
    latest = plot_df[pol].iloc[-1]
    pct_above = (plot_df[pol] > thresholds[pol]).mean() * 100
    col.metric(f"Latest {pol}", f"{latest:.2f}")
    col.metric(f"% {pol} > {thresholds[pol]}", f"{pct_above:.1f}%")
    if latest > thresholds[pol]:
        st.error(f"Latest {pol} = {latest:.2f} above threshold {thresholds[pol]}!")

# ── Multi-Pollutant Overlay ─────────────────────────────────────────
long_df = plot_df.melt(
    id_vars=["Datetime"],
    value_vars=selected,
    var_name="Pollutant",
    value_name="Value"
)
chart = alt.Chart(long_df).mark_line(point=True).encode(
    x=alt.X("Datetime:T", title="Time"),
    y=alt.Y("Value:Q", title="Value"),
    color=alt.Color("Pollutant:N", title="Pollutant"),
    tooltip=["Datetime:T", "Pollutant:N", "Value:Q"]
).interactive().properties(height=400)
st.altair_chart(chart, use_container_width=True)

# ── Display Processed Table ─────────────────────────────────────────
st.dataframe(plot_df, use_container_width=True)

# ── Time-of-Day Heatmap ─────────────────────────────────────────────
p0 = selected[0]
df_h = df[["Datetime", p0]].dropna().copy()
df_h["hour"] = df_h["Datetime"].dt.hour
df_h["weekday"] = pd.Categorical(
    df_h["Datetime"].dt.day_name(),
    categories=list(calendar.day_name), ordered=True
)
h1 = df_h.groupby(["weekday", "hour"], observed=True)[p0].mean().reset_index()
heatmap1 = alt.Chart(h1).mark_rect().encode(
    x=alt.X("hour:O", title="Hour of Day"),
    y=alt.Y("weekday:N", title="Weekday", sort=list(calendar.day_name)),
    color=alt.Color(f"{p0}:Q", title=f"{p0} avg"),
    tooltip=["weekday", "hour", alt.Tooltip(f"{p0}:Q")]
).properties(height=250)
st.subheader(f"Heatmap: {p0} by Hour & Weekday")
st.altair_chart(heatmap1, use_container_width=True)

# ── Monthly Heatmap ────────────────────────────────────────────────
df_m = df_h.copy()
df_m["day"] = df_m["Datetime"].dt.day
df_m["month"] = pd.Categorical(
    df_m["Datetime"].dt.month_name(),
    categories=list(calendar.month_name)[1:], ordered=True
)
h2 = df_m.groupby(["month", "day"], observed=True)[p0].mean().reset_index()
heatmap2 = alt.Chart(h2).mark_rect().encode(
    x=alt.X("day:O", title="Day of Month"),
    y=alt.Y("month:N", title="Month", sort=list(calendar.month_name)[1:]),
    color=alt.Color(f"{p0}:Q", title=f"{p0} avg"),
    tooltip=["month", "day", alt.Tooltip(f"{p0}:Q")]
).properties(height=250)
st.subheader(f"Heatmap: {p0} by Day & Month")
st.altair_chart(heatmap2, use_container_width=True)

# ── Statistical Summary Table ───────────────────────────────────────
st.subheader("Statistical Summary")
st.table(
    df[selected].agg(["mean", "median", "min", "max", "std"]).T
)

# ── Station Map ─────────────────────────────────────────────────────
if "latitude" in df.columns and "longitude" in df.columns:
    st.subheader("Station Locations")
    pts = df[["latitude", "longitude"]].dropna().drop_duplicates()
    st.map(pts)
else:
    st.info("No latitude/longitude data available for station map.")

# ── Feature Caption ─────────────────────────────────────────────────
st.caption(
    "Features: downloads · overlay · rolling · thresholds/KPIs · heatmaps · stats summary · station map"
)
