import streamlit as st
import pandas as pd
import altair as alt
import pydeck as pdk
import csv, re, warnings, calendar
from pathlib import Path
from datetime import datetime

# ── Page config ─────────────────────────────────────────────────────
st.set_page_config("Air-Quality Dashboard", page_icon="🌍", layout="wide")
warnings.filterwarnings("ignore", category=UserWarning)

# ── Paths & Regex ────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parents[1]
DEFAULT_CSV = ROOT / "data" / "raw" / "AirQualityDataHourly.csv"
NUM_RE      = re.compile(r"[-+]?\d+(?:[.,]\d+)?")

# ── Data loader ──────────────────────────────────────────────────────
@st.cache_data(show_spinner="📊 Loading data…")
def load_and_clean(path: str) -> pd.DataFrame:
    # Detect delimiter
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        sample = f.readline()
    delim = csv.Sniffer().sniff(sample, delimiters=",;").delimiter

    # Read raw to find header row
    raw = pd.read_csv(path, sep=delim, header=None, dtype=str, na_filter=False)
    hdr = raw[raw.iloc[:,0].str.strip().str.match(r"Date", case=False)].index
    if hdr.empty:
        st.error("Could not find 'Date' header row in the CSV.")
        st.stop()
    skip = hdr[0]

    # Read with proper header
    df = pd.read_csv(path, sep=delim, skiprows=skip, low_memory=False)
    df.columns = df.columns.str.strip()
    df = df.loc[:, ~df.columns.str.contains(r"Status|^Unnamed", case=False)]

    # Combine Date + Time into Datetime
    if {"Date","Time"}.issubset(df.columns):
        df.insert(0, "Datetime", pd.to_datetime(
            df.pop("Date").str.strip() + " " + df.pop("Time").str.strip(),
            dayfirst=True, errors="coerce"
        ))
        df = df.dropna(subset=["Datetime"]).reset_index(drop=True)

    # Rename pollutant columns
    rename = {}
    for c in df.columns:
        lc = c.lower()
        if "nitrogen dioxide" in lc:
            rename[c] = "Nitrogen dioxide"
        elif "pm10" in lc:
            rename[c] = "PM10"
        elif "pm2.5" in lc or "pm25" in lc:
            rename[c] = "PM2.5"
    df = df.rename(columns=rename)

    # Convert pollutant values to numeric
    for p in ["Nitrogen dioxide","PM10","PM2.5"]:
        if p in df.columns:
            df[p] = (
                df[p].astype(str)
                     .str.replace(",", ".", regex=False)
                     .pipe(pd.to_numeric, errors="coerce")
            )
    # Drop rows where all selected pollutant columns are NaN
    pres = [p for p in ["Nitrogen dioxide","PM10","PM2.5"] if p in df.columns]
    if pres:
        df = df.dropna(subset=pres, how="all").reset_index(drop=True)

    # Sort chronologically
    df = df.sort_values("Datetime").reset_index(drop=True)
    return df

# ── Data source selection ─────────────────────────────────────────────

# Allow users to upload a CSV; otherwise fall back to default file
upload = st.sidebar.file_uploader("Upload UK-Air CSV", type="csv")
if upload is not None:
    # use uploaded file
    df = load_and_clean(upload)
    st.sidebar.success("Using uploaded CSV")
else:
    default = DEFAULT_CSV
    if default.exists():
        df = load_and_clean(str(default))
        file_time = datetime.fromtimestamp(default.stat().st_mtime)
        st.sidebar.markdown(f"**Last updated:** {file_time:%Y-%m-%d %H:%M:%S}")
        if st.sidebar.button("Refresh Data"):
            st.experimental_rerun()
    else:
        st.sidebar.error("No default data file found. Please upload a CSV.")
        st.stop()

# Ensure DataFrame loaded correctly
if df.empty:
    st.error("Loaded data is empty.")
    st.stop()(str(DEFAULT_CSV))

# ── Sidebar Controls ─────────────────────────────────────────────────
st.sidebar.download_button(
    "Download raw filtered data",
    df.to_csv(index=False).encode(),
    file_name="aq_raw_filtered.csv"
)

pollutants = [c for c in ["Nitrogen dioxide","PM10","PM2.5"] if c in df.columns]
selected   = st.sidebar.multiselect("Select pollutants", pollutants, default=pollutants)
if not selected:
    st.warning("Please select at least one pollutant.")
    st.stop()

window     = st.sidebar.slider("Rolling window (hrs)", 1, 168, 24)
thresholds = {p: st.sidebar.number_input(f"Threshold {p}", float(df[p].median() or 0)) for p in selected}
agg        = st.sidebar.radio("Aggregate to", ["raw","hourly","daily"], horizontal=True)
theme      = st.sidebar.radio("Theme", ["Light","Dark"], index=0)
palette    = st.sidebar.selectbox("Palette", ["Default","Viridis","Category10"], index=0)

scheme = None
if palette == "Viridis": scheme = "viridis"
elif palette == "Category10": scheme = "category10"

# ── Process data ────────────────────────────────────────────────────
plot_df = df[["Datetime"] + selected].copy()
if agg != "raw":
    rule = {"hourly":"h","daily":"d"}[agg]
    plot_df = plot_df.set_index("Datetime")[selected].resample(rule).mean().interpolate().reset_index()
if window > 1:
    plot_df = plot_df.set_index("Datetime")[selected].rolling(f"{window}h").mean().reset_index()

st.sidebar.download_button(
    "Download aggregated data",
    plot_df.to_csv(index=False).encode(),
    file_name="aq_agg_filtered.csv"
)

# ── KPI Cards ────────────────────────────────────────────────────────
st.title("🌍 Air Quality Dashboard")
st.markdown(f"Total records: {len(df):,}")
cols = st.columns(len(selected))
for col, p in zip(cols, selected):
    val = plot_df[p].iloc[-1]
    pct = (plot_df[p] > thresholds[p]).mean() * 100
    col.metric(f"Latest {p}", f"{val:.2f}")
    col.metric(f"% >{thresholds[p]}", f"{pct:.1f}%")
    if val > thresholds[p]:
        st.error(f"{p} {val:.2f} exceeds threshold {thresholds[p]}")

# ── Anomalies & Trends ──────────────────────────────────────────────
long_df = plot_df.melt(id_vars=["Datetime"], value_vars=selected,
                       var_name="Pollutant", value_name="Value")
z_th    = st.sidebar.slider("Anomaly z-score threshold", 1.0, 5.0, 2.0)
long_df["zscore"] = long_df.groupby("Pollutant")["Value"].transform(lambda x: (x - x.mean())/x.std())

base   = alt.Chart(long_df).encode(
    x="Datetime:T",
    y="Value:Q",
    color=alt.Color("Pollutant:N", scale=alt.Scale(scheme=scheme)) if scheme else alt.Color("Pollutant:N"),
    tooltip=["Datetime:T","Pollutant:N","Value:Q","zscore:Q"]
)
lines  = base.mark_line()
points = base.mark_point(color="red", size=60).transform_filter(f"abs(datum.zscore) > {z_th}")
trends = base.transform_regression("Datetime","Value", groupby=["Pollutant"]).mark_line(strokeDash=[5,5])
chart  = alt.layer(lines, trends, points).interactive().properties(height=350)
if theme == "Dark":
    chart = chart.configure_view(stroke="white").configure_axis(labelColor="white", titleColor="white")
st.altair_chart(chart, use_container_width=True)

# ── Data Table ─────────────────────────────────────────────────────
st.dataframe(plot_df, use_container_width=True)

# ── Heatmaps ───────────────────────────────────────────────────────
p0   = selected[0]
dh   = df[["Datetime", p0]].dropna().copy()
dh["hour"]    = dh["Datetime"].dt.hour
dh["weekday"] = pd.Categorical(dh["Datetime"].dt.day_name(), categories=list(calendar.day_name), ordered=True)

h1 = dh.groupby(["weekday","hour"], observed=True)[p0].mean().reset_index()
heatmap1 = alt.Chart(h1).mark_rect().encode(
    x="hour:O",
    y=alt.Y("weekday:N", sort=list(calendar.day_name)),
    color=alt.Color(f"{p0}:Q", scale=alt.Scale(scheme=scheme)) if scheme else alt.Color(f"{p0}:Q"),
    tooltip=["weekday","hour", alt.Tooltip(f"{p0}:Q")]
).properties(height=220)
st.subheader(f"Heatmap: {p0} by Hour & Weekday")
st.altair_chart(heatmap1, use_container_width=True)

# Monthly heatmap
df_m = dh.copy()
df_m["day"]   = df_m["Datetime"].dt.day
df_m["month"] = pd.Categorical(df_m["Datetime"].dt.month_name(), categories=list(calendar.month_name)[1:], ordered=True)
h2 = df_m.groupby(["month","day"], observed=True)[p0].mean().reset_index()
heatmap2 = alt.Chart(h2).mark_rect().encode(
    x="day:O",
    y=alt.Y("month:N", sort=list(calendar.month_name)[1:]),
    color=alt.Color(f"{p0}:Q", scale=alt.Scale(scheme=scheme)) if scheme else alt.Color(f"{p0}:Q"),
    tooltip=["month","day", alt.Tooltip(f"{p0}:Q")]
).properties(height=220)
st.subheader(f"Heatmap: {p0} by Day & Month")
st.altair_chart(heatmap2, use_container_width=True)

# ── Statistical Summary ─────────────────────────────────────────────
st.subheader("Statistical Summary")
st.table(df[selected].agg(["mean","median","min","max","std"]).T)

# ── Interactive Station Map ─────────────────────────────────────────
if all(col in df.columns for col in ["station","latitude","longitude"]):
    st.subheader("Station Map – Hover for latest values")
    last = df.sort_values("Datetime").groupby("station").last().reset_index()
    map_df = last[["station","latitude","longitude"] + selected]
    mid_lat = map_df["latitude"].mean()
    mid_lon = map_df["longitude"].mean()
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position=["longitude","latitude"],
        get_radius=300,
        pickable=True,
        auto_highlight=True,
    )
    view = pdk.ViewState(latitude=mid_lat, longitude=mid_lon, zoom=10)
    tooltip = {
        "html": "<b>{station}</b><br>" + "<br>".join([f"{p}: {{{p}}}" for p in selected]),
        "style": {"backgroundColor": "steelblue", "color": "white"}
    }
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tooltip))
else:
    st.info("No station coordinate columns ('station','latitude','longitude') found.")

# ── Footer ──────────────────────────────────────────────────────────
st.caption("Features: refresh · downloads · rolling · thresholds · anomalies · heatmaps · stats · interactive map")
