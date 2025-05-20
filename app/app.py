# app/app.py

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path

# — Page Config —
st.set_page_config(page_title="Air Quality Dashboard", layout="wide")
st.title("Air Quality Dashboard")


# — Sidebar Controls —
st.sidebar.header("Filters")

# 1) Site selection (multi)
@st.cache_data
def get_sites(db="data/airquality.duckdb"):
    con = duckdb.connect(db, read_only=True)
    sites = con.execute("SELECT DISTINCT site_name FROM clean_aurn").df()["site_name"].tolist()
    con.close()
    return sorted(sites)

sites = get_sites()
selected_sites = st.sidebar.multiselect("Select site(s):", options=sites, default=sites[:3])

# 2) Date-range slider
@st.cache_data
def get_date_bounds(db="data/airquality.duckdb"):
    con = duckdb.connect(db, read_only=True)
    r = con.execute("SELECT MIN(datetime), MAX(datetime) FROM clean_aurn").fetchone()
    con.close()
    return pd.to_datetime(r[0]), pd.to_datetime(r[1])

min_dt, max_dt = get_date_bounds()
start_date, end_date = st.sidebar.slider(
    "Date range:",
    min_value=min_dt,
   .max_value=max_dt,
    value=(min_dt, max_dt),
    format="YYYY-MM-DD"
)

# 3) Pollutant toggles
pollutants = st.sidebar.multiselect(
    "Plot metrics:", ["no2", "pm25", "wind_speed", "temp"],
    default=["no2", "pm25"]
)

st.sidebar.markdown("---")


# — Data Loading —
@st.cache_data
def load_data(db="data/airquality.duckdb"):
    con = duckdb.connect(db, read_only=True)
    df_air = con.execute("SELECT site_name, datetime, no2, pm25 FROM clean_aurn").df()
    df_wx  = con.execute("SELECT datetime, temp, wind_speed FROM clean_weather").df()
    con.close()
    # ensure datetime type
    df_air["datetime"] = pd.to_datetime(df_air["datetime"])
    df_wx["datetime"]  = pd.to_datetime(df_wx["datetime"])
    return df_air, df_wx

df_air, df_wx = load_data()

# — Apply Filters —
mask = (
    df_air.site_name.isin(selected_sites) &
    (df_air.datetime >= start_date) &
    (df_air.datetime <= end_date)
)
df_air_f = df_air.loc[mask]

mask2 = (df_wx.datetime >= start_date) & (df_wx.datetime <= end_date)
df_wx_f = df_wx.loc[mask2]

# — 1) Time Series of selected metrics —
st.subheader("Time Series")
if not pollutants:
    st.warning("Select at least one metric above.")
else:
    df_ts = df_air_f[["site_name", "datetime"] + [p for p in pollutants if p in df_air_f]]
    if "wind_speed" in pollutants or "temp" in pollutants:
        df_ts = df_ts.merge(df_wx_f, on="datetime", how="left")
    fig_ts = px.line(
        df_ts,
        x="datetime",
        y=pollutants,
        color="site_name",
        facet_col_wrap=2,
        title="Time Series of Metrics"
    )
    st.plotly_chart(fig_ts, use_container_width=True)

# — 2) PM2.5 & NO₂ Correlation Scatter —
if "no2" in pollutants and "temp" in pollutants:
    st.subheader("NO₂ vs. Temperature")
    df_corr = df_air_f.merge(df_wx_f, on="datetime", how="inner")
    fig_corr = px.scatter(
        df_corr, x="temp", y="no2", color="site_name",
        trendline="ols", title="NO₂ vs Temperature"
    )
    st.plotly_chart(fig_corr, use_container_width=True)

# — 3) Hour-of-Day Heatmap for each pollutant —
st.subheader("Hourly Heatmap")
df_air_f["hour"] = df_air_f.datetime.dt.hour
heat = df_air_f.groupby(["hour"])[[p for p in pollutants if p in ["no2","pm25"]]].mean().reset_index()
if not heat.empty:
    fig_heat = px.imshow(
        heat.set_index("hour").T,
        labels=dict(x="Hour of Day", y="Metric", color="Avg value"),
        title="Avg Pollutant by Hour"
    )
    st.plotly_chart(fig_heat, use_container_width=True)

# — 4) Geospatial Map —
st.subheader("Site Map")
# assume metadata table exists in DuckDB with site_name, latitude, longitude
@st.cache_data
def load_meta(db="data/airquality.duckdb"):
    con = duckdb.connect(db, read_only=True)
    # you must have created a metadata table earlier
    df_meta = con.execute("SELECT site_name, latitude, longitude FROM metadata").df()
    con.close()
    return df_meta

try:
    df_meta = load_meta()
    # join with avg pollutant
    avg = df_air_f.groupby("site_name")[pollutants].mean().reset_index()
    df_map = avg.merge(df_meta, on="site_name", how="inner")
    fig_map = px.scatter_mapbox(
        df_map,
        lat="latitude", lon="longitude",
        size=pollutants[0],
        color=pollutants[0],
        hover_name="site_name",
        zoom=5,
        mapbox_style="carto-positron",
        title="Site Locations colored by " + pollutants[0]
    )
    st.plotly_chart(fig_map, use_container_width=True)
except Exception:
    st.info("Site metadata not found; map disabled.")

