
# app/app.py

import streamlit as st
import pandas as pd
import re

# --- Settings ---
DATA_PATH = "data/clean/clean_air_quality.csv"

# --- Load Data ---
@st.cache_data
def load_data():
    return pd.read_csv(DATA_PATH, parse_dates=["datetime"], dayfirst=True)

df = load_data()

# --- Display columns found (for debug and robustness) ---
st.sidebar.write("Columns found in data:", list(df.columns))

# --- Robust pollutant column finder ---
def get_pollutant_cols(df):
    """
    Return a dict: {pretty_name: column_name_in_df}
    """
    lookup = {
        "NO₂": re.compile(r"no[\s\.]?2|nitrogendioxide", re.I),
        "PM₁₀": re.compile(r"pm[\s_\.]?10", re.I),
        "PM₂.₅": re.compile(r"pm[\s_\.]?2\.?5", re.I),
    }
    result = {}
    for pretty, pattern in lookup.items():
        match = next((col for col in df.columns if pattern.search(col.replace(" ", "").replace("_", ""))), None)
        if match:
            result[pretty] = match
    return result

pollutant_cols = get_pollutant_cols(df)

if not pollutant_cols:
    st.error("No known pollutant columns found! Columns present: " + ", ".join(df.columns))
    st.stop()

# --- Pick pollutant to plot ---
pollutant_pretty = st.sidebar.selectbox("Select pollutant", list(pollutant_cols.keys()))
pollutant_col = pollutant_cols[pollutant_pretty]

# --- Display site/location info if present (optional) ---
site = None
for name in ["Site", "site", "Site Name", "site_name"]:
    if name in df.columns:
        site = df[name].iloc[0]
        break

st.title("Air Quality Dashboard")
if site:
    st.write(f"**Location:** {site}")

# --- Plot time series ---
st.subheader(f"Time Series for {pollutant_pretty}")
st.line_chart(df.set_index("datetime")[pollutant_col])

# --- Show data preview ---
with st.expander("See raw data"):
    st.dataframe(df.head())

# --- (Optional) Add more charts and features as you like ---

