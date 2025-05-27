
# app/app.py
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Air Quality Dashboard", layout="wide")
st.title("London Air Quality Dashboard (Hourly Data)")

@st.cache_data
def load_data():
    df = pd.read_csv("data/clean/air_quality_cleaned.csv", parse_dates=["Datetime"])
    return df

df = load_data()

site_options = sorted(df["Site"].unique())
site = st.sidebar.selectbox("Select Monitoring Site", site_options)
pollutant = st.sidebar.selectbox("Pollutant", ["NO2", "PM10", "PM2.5"])

df_site = df[df["Site"] == site].dropna(subset=[pollutant])

st.subheader(f"{pollutant} - {site}")
st.line_chart(df_site.set_index("Datetime")[pollutant])

# Show data table
with st.expander("Show raw data"):
    st.write(df_site.head(100))
