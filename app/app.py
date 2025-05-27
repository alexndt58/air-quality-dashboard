# app/app.py

import streamlit as st
import duckdb
import pandas as pd

st.title("London Air Quality Dashboard")

@st.cache_data
def load_data():
    con = duckdb.connect("data/airquality.duckdb", read_only=True)
    df = con.execute("SELECT * FROM clean_aurn").df()
    con.close()
    return df

df = load_data()

st.write(f"Data shape: {df.shape}")
st.write(df.head())

# Filter by site (if >1 site present)
if "site_name" in df.columns:
    site = st.selectbox("Select site", sorted(df["site_name"].unique()))
    st.write(df[df["site_name"] == site])

# Plot example: time series of NO2
if "NO2" in df.columns:
    st.line_chart(df.set_index("datetime")["NO2"])
