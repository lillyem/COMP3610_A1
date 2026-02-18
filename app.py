import streamlit as st
from utils import load_data

with st.sidebar:
    if st.button("Clear cache + rerun"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

st.set_page_config(
    page_title="NYC Yellow Taxi Dashboard (Jan 2024)",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title(f"NYC Yellow Taxi Dashboard (January 2024)")

st.write(
    "This dashboard explores NYC Yellow Taxi trips for January 2024. "
    "Use the sidebar pages to view an overview of the cleaned dataset "
    "(busiest zones, fares by hour, payment types, tip %, and zone pairs)."
)

# The dataset is programmatically downloaded and processed in utils.py,
# where cleaning and feature engineering are applied prior to visualization.

df = load_data()

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
