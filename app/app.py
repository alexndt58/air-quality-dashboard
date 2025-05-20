# app/app.py

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path

# — Page configuration —
st.set_page_config(page_title="Air Quality Dashboard", layout="wide")
st.title("Air Quality Dashboard")

# — Sidebar controls —
st.sidebar.header("Settings")
max_gap = st.sidebar.slider("Forward-fill gap (hrs)", 0, 6, 2)

# — Data loading with caching —
@st.cache_data
def load_data(db_path: str = "data/airquality.duckdb"):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path, read_only=True)
    # Load cleaned AURN and weather
    df_air = con.execute(
        "SELECT * FROM clean_aurn"
    ).df()
    df_wx = con.execute(
        "SELECT * FROM clean_weather"
    ).df()
    con.close()
    # Coerce datetime
    df_air = df_air.assign(datetime=pd.to_datetime(df_air["datetime"], errors="coerce"))
    df_wx = df_wx.assign(datetime=pd.to_datetime(df_wx["datetime"], errors="coerce"))
    return df_air, df_wx

# — Load data —
df_air, df_wx = load_data()

# If weather data is empty, notify and skip temp plotting
if df_wx.empty:
    st.warning("No weather data available to merge. Temperature plots will be hidden.")


# — Station selection —
st.subheader("Site Selection")
stations = sorted(df_air["site_name"].dropna().unique().tolist())
# Always include an 'All' option
stations.insert(0, "All")
site = st.selectbox("Select site:", stations)

# Filter data for selected site
if site == "All":
    df_site = df_air.sort_values("datetime")
else:
    df_site = df_air[df_air["site_name"] == site].sort_values("datetime")

# — Time Series Chart —
st.subheader(f"Time Series at {site}")
# Merge nearest weather readings
merged = pd.merge_asof(
    df_site,
    df_wx.sort_values("datetime"),
    on="datetime",
    direction="nearest"
)

# Select y columns: NO2 always, temp if available
y_cols = ["no2"]
if "temp" in merged.columns:
    y_cols.append("temp")

# Plot
fig = px.line(
    merged,
    x="datetime",
    y=y_cols,
    title=f"NO₂{' & Temperature' if 'temp' in y_cols else ''} at {site}",
    labels={"value": "Measurement", "variable": "Metric"}
)
st.plotly_chart(fig, use_container_width=True)
