import streamlit as st
import polars as pl
import requests
from utils import load_data
from pathlib import Path

st.set_page_config(
    page_title="NYC Yellow Taxi Dashboard (Jan 2024)",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sidebar: choose year/month
year = st.sidebar.selectbox("Year", options=list(range(2022, 2025)), index=2)  # example defaults
month = st.sidebar.selectbox("Month", options=list(range(1, 13)), index=0)     # Jan default

st.title(f"NYC Yellow Taxi Dashboard ({year}-{month:02d})")

st.write(
    "This dashboard explores NYC Yellow Taxi trips for January 2024. "
    "Use the sidebar pages to view an overview of the cleaned dataset and the SQL-based analyses "
    "(busiest zones, fares by hour, payment types, tip %, and zone pairs)."
)

df = load_data(year, month)

# Key Metrics 

st.subheader("Key Summary Metrics")

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric(
    "Total Trips",
    f"{df.height:,}"
)

col2.metric(
    "Average Fare",
    f"${df['fare_amount'].mean():.2f}"
)

col3.metric(
    "Total Revenue",
    f"${df['total_amount'].sum():,.2f}"
)

col4.metric(
    "Average Distance",
    f"{df['trip_distance'].mean():.2f} miles"
)

col5.metric(
    "Average Duration",
    f"{df['trip_duration_minutes'].mean():.2f} mins"
)

st.divider()
st.subheader("Data Coverage")

min_date = df["tpep_pickup_datetime"].min()
max_date = df["tpep_pickup_datetime"].max()

c1, c2 = st.columns(2)
with c1:
    st.info(f"**Pickup datetime range:** {min_date} → {max_date}")

with c2:
    PAYMENT_MAP = {
        0: "Unknown", 1: "Credit Card", 2: "Cash", 3: "No Charge",
        4: "Dispute", 5: "Unknown", 6: "Voided Trip"
    }
    top_payment_code = int(df["payment_type"].value_counts().sort("count", descending=True)["payment_type"][0])
    st.info(f"**Most common payment:** {top_payment_code} - {PAYMENT_MAP.get(top_payment_code, 'Other')}")
