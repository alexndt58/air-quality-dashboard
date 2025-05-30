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

############# Concentration ##############################

# ── Concentration Distributions ────────────────────────────────────────
st.subheader("Concentration Distributions")

# use the *aggregated* (or you could swap in df for the raw) DataFrame
dist_df = plot_df.melt(
    id_vars=[],
    value_vars=selected,
    var_name="Pollutant",
    value_name="Concentration"
).dropna()

# build a faceted histogram, one panel per pollutant, with independent y-scales
hist = (
    alt.Chart(dist_df)
    .mark_bar()
    .encode(
        x=alt.X("Concentration:Q", bin=alt.Bin(maxbins=50), title="Concentration"),
        y=alt.Y("count()", title="Frequency"),
        color=alt.Color("Pollutant:N", scale=alt.Scale(scheme=scheme)) if scheme else alt.Color("Pollutant:N"),
        tooltip=["Pollutant:N", "count()"]
    )
    .properties(width=200, height=200)
    .facet(column=alt.Column("Pollutant:N", title=None, header=alt.Header(labelFontSize=12)))
    .resolve_scale(y="independent")
)

if theme == "Dark":
    hist = (
        hist
        .configure_view(stroke="white")
        .configure_axis(labelColor="white", titleColor="white")
    )

st.altair_chart(hist, use_container_width=True)


################### Correlation scatter-plots ############################

# ── Correlation Overview ────────────────────────────────────────────────
st.subheader("Correlation Heatmap")
# Compute pairwise correlations
corr_mat = plot_df[selected].corr()

# Melt into long form
corr_src = (
    corr_mat
    .reset_index()
    .melt(id_vars="index", var_name="variable", value_name="correlation")
    .rename(columns={"index":"pollutant"})
)

# Altair heatmap
heat = (
    alt.Chart(corr_src)
       .mark_rect()
       .encode(
           x=alt.X("variable:N", sort=selected, title=""),
           y=alt.Y("pollutant:N", sort=selected, title=""),
           color=alt.Color(
               "correlation:Q",
               scale=alt.Scale(scheme=scheme or "blues"),
               legend=alt.Legend(title="r")
           ),
           tooltip=[
               alt.Tooltip("pollutant:N", title="Pollutant X"),
               alt.Tooltip("variable:N", title="Pollutant Y"),
               alt.Tooltip("correlation:Q", format=".2f")
           ]
       )
       .properties(width=350, height=350)
)

if theme == "Dark":
    heat = heat.configure_axis(labelColor="white", titleColor="white")

st.altair_chart(heat, use_container_width=False)


# ── Pick-and-Plot Scatter with Trendline ─────────────────────────────────
st.subheader("Pairwise Scatter + Trendline")
pair = st.sidebar.multiselect(
    "Choose two pollutants to compare",
    options=selected,
    default=selected[:2],
    help="Select exactly two"
)

if len(pair) == 2:
    x, y = pair
    scatter = (
        alt.Chart(plot_df)
           .mark_circle(size=50, opacity=0.4)
           .encode(
               x=alt.X(f"{x}:Q", title=x),
               y=alt.Y(f"{y}:Q", title=y),
               tooltip=["Datetime:T", f"{x}:Q", f"{y}:Q"]
           )
    )
    # Add a regression line
    trend = (
        alt.Chart(plot_df)
           .transform_regression(x, y, method="linear")
           .mark_line(color="firebrick", strokeWidth=2)
           .encode(x=x, y=y)
    )
    chart = (scatter + trend).properties(width=600, height=400).interactive()
    if theme == "Dark":
        chart = chart.configure_axis(labelColor="white", titleColor="white")
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("Please select exactly two pollutants for the scatter plot.")

########### Predictive model ###########################################

import numpy as np

# ── Simple Trend Forecast ──────────────────────────────────────────────
st.subheader("Forecast: Simple Linear Trend")

# 1) Sidebar controls
poll_fc = st.sidebar.selectbox(
    "Choose pollutant to forecast", options=selected, index=0
)
horizon = st.sidebar.number_input(
    "Forecast horizon (periods)", min_value=1, max_value=168, value=24
)

# 2) Prepare data
fc_df = plot_df[["Datetime", poll_fc]].dropna().reset_index(drop=True)
if len(fc_df) < 2:
    st.warning("Not enough data to build a forecast.")
else:
    # map dates to numeric (ordinal)
    x = fc_df["Datetime"].map(lambda dt: dt.toordinal()).to_numpy()
    y = fc_df[poll_fc].to_numpy()

    # 3) Fit linear trend
    coef = np.polyfit(x, y, 1)
    model = np.poly1d(coef)

    # 4) Build future dates
    # infer frequency: raw/hourly → 'h', daily → 'd'
    freq = {"raw":"h","hourly":"h","daily":"d"}[agg]
    last = fc_df["Datetime"].iloc[-1]
    future = pd.date_range(last, periods=horizon+1, freq=freq)[1:]

    # 5) Predict
    x_future = np.array([d.toordinal() for d in future])
    y_future = model(x_future)
    fcast_df = pd.DataFrame({"Datetime": future, poll_fc: y_future})

    # 6) Plot history + forecast
    hist = (
        alt.Chart(fc_df)
           .mark_line()
           .encode(x="Datetime:T", y=f"{poll_fc}:Q")
    )
    pred = (
        alt.Chart(fcast_df)
           .mark_line(color="orange", strokeDash=[4,4])
           .encode(x="Datetime:T", y=f"{poll_fc}:Q")
    )
    chart = (hist + pred).properties(
        width=700, height=300
    ).interactive()
    if theme == "Dark":
        chart = chart.configure_axis(labelColor="white", titleColor="white")
    st.altair_chart(chart, use_container_width=True)

    # 7) Show model equation & metrics
    st.markdown(f"**Trend line:** y = {coef[0]:.4e}·x + {coef[1]:.2f}")




###################################################################################################

# import streamlit as st
# import pandas as pd
# import re
# import datetime

# # --- Settings ---
# DATA_PATH = "data/clean/clean_air_quality.csv"

# # --- Load Data ---
# @st.cache_data
# def load_data():
#     df = pd.read_csv(DATA_PATH)
#     # Ensure the datetime column is real timestamps
#     df["datetime"] = pd.to_datetime(df["datetime"], dayfirst=True, errors="coerce")
#     return df

# df = load_data()

# # --- Display columns found (for debug and robustness) ---
# st.sidebar.write("Columns found in data:", list(df.columns))

# # --- Robust pollutant column finder ---
# def get_pollutant_cols(df):
#     """
#     Return a dict: {pretty_name: column_name_in_df}
#     """
#     lookup = {
#         "NO₂": re.compile(r"no[\s\.]?2|nitrogendioxide", re.I),
#         "PM₁₀": re.compile(r"pm[\s_\.]?10", re.I),
#         "PM₂.₅": re.compile(r"pm[\s_\.]?2\.?5", re.I),
#     }
#     result = {}
#     for pretty, pattern in lookup.items():
#         match = next(
#             (
#                 col
#                 for col in df.columns
#                 if pattern.search(col.replace(" ", "").replace("_", ""))
#             ),
#             None,
#         )
#         if match:
#             result[pretty] = match
#     return result

# pollutant_cols = get_pollutant_cols(df)

# if not pollutant_cols:
#     st.error("No known pollutant columns found! Columns present: " + ", ".join(df.columns))
#     st.stop()

# # --- Pick pollutant to plot ---
# pollutant_pretty = st.sidebar.selectbox("Select pollutant", list(pollutant_cols.keys()))
# pollutant_col = pollutant_cols[pollutant_pretty]

# # --- Display site/location info if present (optional) ---
# site = None
# for name in ["Site", "site", "Site Name", "site_name"]:
#     if name in df.columns:
#         site = df[name].iloc[0]
#         break

# st.title("Air Quality Dashboard")
# if site:
#     st.write(f"**Location:** {site}")

# # --- Date range picker using real dates ---
# min_date = df["datetime"].min().date()
# max_date = df["datetime"].max().date()

# start_date, end_date = st.sidebar.slider(
#     "Select date range",
#     min_value=min_date,
#     max_value=max_date,
#     value=(min_date, max_date),
# )

# # Filter dataframe to the selected date window
# mask = (
#     (df["datetime"].dt.date >= start_date)
#     & (df["datetime"].dt.date <= end_date)
# )
# df = df.loc[mask]

# # --- Plot time series ---
# st.subheader(f"Time Series for {pollutant_pretty}")
# st.line_chart(df.set_index("datetime")[pollutant_col])

# # --- Show data preview ---
# with st.expander("See raw data"):
#     st.dataframe(df.head())

# # --- (Optional) Add more charts and features below ---
# # For example, rolling average:
# window = st.sidebar.slider("Rolling window (hours)", min_value=1, max_value=72, value=24)
# rolling = (
#     df.set_index("datetime")[pollutant_col]
#     .rolling(f"{window}H")
#     .mean()
#     .dropna()
# )
# st.subheader(f"{window}-Hour Rolling Average for {pollutant_pretty}")
# st.line_chart(rolling)

# # Correlation scatter‐plot example
# if st.sidebar.checkbox("Show correlation scatter plot"):
#     other_pretties = [p for p in pollutant_cols if p != pollutant_pretty]
#     compare_pretty = st.sidebar.selectbox("Compare with", other_pretties)
#     compare_col = pollutant_cols[compare_pretty]
#     st.subheader(f"{pollutant_pretty} vs. {compare_pretty}")
#     st.plotly_chart(
#         pd.DataFrame({
#             pollutant_pretty: df[pollutant_col],
#             compare_pretty: df[compare_col],
#         }).reset_index(drop=True).pipe(
#             lambda d: __import__("plotly.express").express.scatter(
#                 d, x=pollutant_pretty, y=compare_pretty
#             )
#         )
#     )

