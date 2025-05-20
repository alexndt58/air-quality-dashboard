# app/app.py

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime

st.set_page_config(page_title="Air Quality Dashboard", layout="wide")
st.title("Air Quality Dashboard")

# --- Sidebar Controls ---

st.sidebar.header("Filters")

# 1) Multi-site selector
@st.cache_data
def get_sites(db="data/airquality.duckdb"):
    con = duckdb.connect(db, read_only=True)
    sites = con.execute("SELECT DISTINCT site_name FROM clean_aurn").df()["site_name"].tolist()
    con.close()
    return sorted(sites)

sites = get_sites()
selected_sites = st.sidebar.multiselect(
    "Select site(s):",
    options=sites,
    default=sites[:3] if len(sites) >= 3 else sites
)

# 2) Date-range slider
@st.cache_data
def get_date_bounds(db="data/airquality.duckdb"):
    con = duckdb.connect(db, read_only=True)
    lo, hi = con.execute("SELECT MIN(datetime), MAX(datetime) FROM clean_aurn").fetchone()
    con.close()
    # Convert to native Python datetime
    return pd.to_datetime(lo).to_pydatetime(), pd.to_datetime(hi).to_pydatetime()

min_dt, max_dt = get_date_bounds()
start_date, end_date = st.sidebar.slider(
    "Date range:",
    min_value=min_dt,
    max_value=max_dt,
    value=(min_dt, max_dt),
    format="YYYY-MM-DD"
)

# 3) Pollutant toggles
all_metrics = ["no2", "pm25", "temp", "wind_speed"]
selected_metrics = st.sidebar.multiselect(
    "Plot metrics:", options=all_metrics, default=["no2", "pm25"]
)

st.sidebar.markdown("---")

# --- Load Data ---

@st.cache_data
def load_data(db="data/airquality.duckdb"):
    con = duckdb.connect(db, read_only=True)
    df_air = con.execute("SELECT site_name, datetime, no2, pm25 FROM clean_aurn").df()
    df_wx  = con.execute("SELECT datetime, temp, wind_speed FROM clean_weather").df()
    con.close()
    df_air["datetime"] = pd.to_datetime(df_air["datetime"], errors="coerce")
    df_wx["datetime"]  = pd.to_datetime(df_wx["datetime"], errors="coerce")
    return df_air, df_wx

df_air, df_wx = load_data()

# --- Apply Filters ---

mask_air = (
    df_air.site_name.isin(selected_sites) &
    (df_air.datetime >= start_date) &
    (df_air.datetime <= end_date)
)
df_air_f = df_air.loc[mask_air]

mask_wx = (df_wx.datetime >= start_date) & (df_wx.datetime <= end_date)
df_wx_f = df_wx.loc[mask_wx]

# --- 1) Time Series of selected metrics ---
st.subheader("Time Series of Selected Metrics")

if not selected_metrics:
    st.warning("Please select at least one metric to plot.")
else:
    # Merge weather if any metric needs it
    df_ts = df_air_f[["site_name", "datetime"] + [m for m in selected_metrics if m in df_air_f.columns]].copy()
    wx_needed = [m for m in ["temp", "wind_speed"] if m in selected_metrics]
    if wx_needed:
        df_ts = pd.merge_asof(
            df_ts.sort_values("datetime"),
            df_wx_f[["datetime"] + wx_needed].sort_values("datetime"),
            on="datetime", direction="nearest"
        )
    fig_ts = px.line(
        df_ts,
        x="datetime",
        y=[m for m in selected_metrics if m in df_ts.columns],
        color="site_name",
        facet_col="site_name" if len(selected_sites) > 1 else None,
        facet_col_wrap=2,
        title="Time Series"
    )
    st.plotly_chart(fig_ts, use_container_width=True)

# --- 2) NO₂ vs Temperature Scatter ---
if "no2" in selected_metrics and "temp" in selected_metrics:
    st.subheader("NO₂ vs. Temperature Correlation")
    df_corr = pd.merge_asof(
        df_air_f.sort_values("datetime"),
        df_wx_f[["datetime", "temp"]].sort_values("datetime"),
        on="datetime", direction="nearest"
    )
    fig_corr = px.scatter(
        df_corr, x="temp", y="no2", color="site_name",
        trendline="ols", title="NO₂ vs Temperature"
    )
    st.plotly_chart(fig_corr, use_container_width=True)

# --- 3) Hour-of-Day Heatmap for Air Pollutants ---
if any(m in ["no2", "pm25"] for m in selected_metrics):
    st.subheader("Hourly Heatmap of Air Pollutants")
    df_air_f["hour"] = df_air_f.datetime.dt.hour
    heat_df = df_air_f.groupby("hour")[[m for m in ["no2","pm25"] if m in selected_metrics]].mean().reset_index()
    if not heat_df.empty and len(heat_df.columns) > 1:
        fig_heat = px.imshow(
            heat_df.set_index("hour").T,
            labels=dict(x="Hour of Day", y="Metric", color="Average"),
            title="Average Pollutant by Hour"
        )
        st.plotly_chart(fig_heat, use_container_width=True)

# --- 4) Geospatial Map ---
st.subheader("Site Map")
@st.cache_data
def load_metadata(db="data/airquality.duckdb"):
    con = duckdb.connect(db, read_only=True)
    try:
        df_meta = con.execute("SELECT site_name, latitude, longitude FROM metadata").df()
    except Exception:
        df_meta = pd.DataFrame(columns=["site_name", "latitude", "longitude"])
    con.close()
    return df_meta

df_meta = load_metadata()
if not df_meta.empty and "site_name" in df_meta.columns:
    avg_vals = df_air_f.groupby("site_name")[selected_metrics].mean().reset_index()
    df_map = avg_vals.merge(df_meta, on="site_name", how="inner")
    if not df_map.empty:
        primary = selected_metrics[0]
        fig_map = px.scatter_mapbox(
            df_map,
            lat="latitude", lon="longitude",
            size=primary if primary in df_map else None,
            color=primary if primary in df_map else None,
            hover_name="site_name",
            zoom=5, mapbox_style="carto-positron",
            title=f"Site Locations colored by {primary}"
        )
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("No joined data to plot on map.")
else:
    st.info("Metadata table not found; map is disabled.")
