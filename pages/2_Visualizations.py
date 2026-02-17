import streamlit as st
import polars as pl
import plotly.express as px
from utils import load_data
from utils import load_lookup
#from pathlib import Path

st.title("Visualizations")

df = load_data()
zones = load_lookup()

zones = zones.with_columns(
    pl.col("LocationID").cast(pl.Int32)
)

@st.cache_data
def apply_filters(df, start_date, end_date, hour_min, hour_max, payments):
    filtered = df.filter(
        (pl.col("tpep_pickup_datetime").dt.date() >= pl.lit(start_date)) &
        (pl.col("tpep_pickup_datetime").dt.date() <= pl.lit(end_date)) &
        (pl.col("pickup_hour") >= hour_min) &
        (pl.col("pickup_hour") <= hour_max)
    )
    if payments:
        filtered = filtered.filter(pl.col("payment_type").is_in(payments))
    else:
        filtered = filtered.head(0)
    return filtered

@st.cache_data
def top10_pickup(filtered, zones):
    return (
        filtered.group_by("PULocationID")
        .agg(pl.len().alias("trip_count"))
        .join(zones.select(["LocationID", "Zone", "Borough"]),
              left_on="PULocationID", right_on="LocationID", how="left")
        .with_columns((pl.col("Borough") + " - " + pl.col("Zone")).alias("pickup_zone_label"))
        .sort("trip_count", descending=True)
        .head(10)
    )

# Sidebar filters 

st.sidebar.header("Filters")

min_dt = df["tpep_pickup_datetime"].min()
max_dt = df["tpep_pickup_datetime"].max()

date_range = st.sidebar.date_input(
    "Pickup date range",
    value=(min_dt.date(), max_dt.date()),
    min_value=min_dt.date(),
    max_value=max_dt.date(),
)

hour_range = st.sidebar.slider("Pickup hour range", 0, 23, (0, 23))

# --- Payment type mapping (code -> name) ---
PAYMENT_MAP = {
    0: "Unknown",
    1: "Credit Card",
    2: "Cash",
    3: "No Charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided Trip",
}

# Get available payment codes in the data as int
payment_codes = sorted([int(x) for x in df["payment_type"].unique().to_list()])

# Convert codes to display labels like "1 - Credit Card"
payment_labels = [f"{code} - {PAYMENT_MAP.get(code, 'Other')}" for code in payment_codes]

selected_labels = st.sidebar.multiselect(
    "Payment type",
    options=payment_labels,
    default=payment_labels,   
)

# Convert selected labels back to numeric codes 
selected_payments = [int(lbl.split(" - ")[0]) for lbl in selected_labels]


# Filters
filtered = apply_filters(
    df,
    date_range[0],
    date_range[1],
    hour_range[0],
    hour_range[1],
    selected_payments
)

st.sidebar.caption(f"Filtered trips: {filtered.height:,}")


# Helper: payment type names 

def payment_name_expr():
    return (
        pl.when(pl.col("payment_type") == 0).then(pl.lit("Unknown"))
        .when(pl.col("payment_type") == 1).then(pl.lit("Credit Card"))
        .when(pl.col("payment_type") == 2).then(pl.lit("Cash"))
        .when(pl.col("payment_type") == 3).then(pl.lit("No Charge"))
        .when(pl.col("payment_type") == 4).then(pl.lit("Dispute"))
        .when(pl.col("payment_type") == 5).then(pl.lit("Unknown"))
        .when(pl.col("payment_type") == 6).then(pl.lit("Voided Trip"))
        .otherwise(pl.lit("Other"))
    )

st.divider()

# r) Bar chart: Top 10 pickup zones by trip count

st.subheader("Top 10 Pickup Zones by Trip Count")

top10_pu = top10_pickup(filtered, zones)


fig_r = px.bar(
    top10_pu.to_pandas(),
    x="pickup_zone_label",
    y="trip_count",
    title="Top 10 Pickup Zones",
    labels={"pickup_zone_label": "Pickup Zone", "trip_count": "Trips"},
)
fig_r.update_layout(xaxis_tickangle=-35)
st.plotly_chart(fig_r, use_container_width=True)

st.caption(
    "Midtown Manhattan and Upper East Side zones dominate pickup activity, indicating strong demand in central business and residential areas. JFK and LaGuardia Airport also appear in the top 10, confirming that airport traffic is a major contributor to total NYC taxi volume."
)



st.divider()

# s) Line chart: Average fare by hour of day

st.subheader("Average Fare by Hour of Day")

avg_fare_by_hour = (
    filtered.group_by("pickup_hour")
    .agg(pl.col("fare_amount").mean().alias("avg_fare"))
    .sort("pickup_hour")
)

fig_s = px.line(
    avg_fare_by_hour.to_pandas(),
    x="pickup_hour",
    y="avg_fare",
    markers=True,
    title="Average Fare by Hour",
    labels={"pickup_hour": "Hour of Day", "avg_fare": "Average Fare ($)"},
)
st.plotly_chart(fig_s, use_container_width=True)

st.caption(
    "Average fares spike sharply around 5 AM, likely reflecting airport trips or longer early-morning rides. Fares are lowest between 2–4 AM, when demand and trip distances are typically shorter. A moderate increase in the evening suggests higher pricing during post-work and nightlife hours."
)



st.divider()

# t) Histogram: Distribution of trip distances

st.subheader("Distribution of Trip Distances")

# Capping outliers so the histogram is readable
dist_cap = st.sidebar.slider("Max distance to display (miles)", 5, 100, 50)

bin_size = 0.5
hist = (
    filtered
    .filter(pl.col("trip_distance") <= dist_cap)
    .with_columns(
        (pl.col("trip_distance") / bin_size).floor() * bin_size
        .alias("bin")
    )
    .group_by("bin")
    .agg(pl.len().alias("count"))
    .sort("bin")
)
fig_t = px.bar(hist.to_pandas(), x="bin", y="count")

st.plotly_chart(fig_t, use_container_width=True)

st.caption(
    "The vast majority of taxi trips are short-distance rides under 5 miles, indicating that taxis are primarily used for local travel within boroughs. The long right tail shows that longer trips do occur but are relatively rare, likely representing airport or inter-borough travel."
)



# u) Payment type breakdown (pie or bar)

st.subheader("Payment Type Breakdown")

pay_breakdown = (
    filtered.with_columns(payment_name_expr().alias("payment_type_name"))
    .group_by("payment_type_name")
    .agg(pl.len().alias("trips"))
    .with_columns((pl.col("trips") / pl.col("trips").sum() * 100).alias("percent"))
    .sort("trips", descending=True)
)

# Pie chart
fig_u = px.pie(
    pay_breakdown.to_pandas(),
    names="payment_type_name",
    values="trips",
    title="Payment Type Share",
)
st.plotly_chart(fig_u, use_container_width=True)

st.caption(
    "Credit card payments account for roughly 80% of all trips, showing a strong shift toward digital payments in NYC taxis. Cash represents a much smaller share, while disputes and no-charge trips are extremely rare. This suggests modern taxi usage is largely cashless."
)



st.divider()

# v) Heatmap: Trips by day of week and hour

st.subheader("Trips by Day of Week and Hour (Heatmap)")

day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

dow_hour = (
    filtered.group_by(["pickup_day_of_week", "pickup_hour"])
    .agg(pl.len().alias("trips"))
)

# Convert to pandas for Plotly heatmap pivot
dow_hour_pd = dow_hour.to_pandas()

# Pivot for heatmap matrix
pivot = dow_hour_pd.pivot(index="pickup_day_of_week", columns="pickup_hour", values="trips").fillna(0)

# Reindex day order
pivot = pivot.reindex(day_order)

fig_v = px.imshow(
    pivot,
    aspect="auto",
    title="Trips by Day of Week and Hour",
    labels=dict(x="Hour of Day", y="Day of Week", color="Trips"),
)
st.plotly_chart(fig_v, use_container_width=True)

st.caption(
    "Weekdays show clear commuting peaks in the morning around 8–9 AM and late afternoon around 4–6 PM, reflecting work-related travel. Weekend demand shifts toward later hours, particularly Friday and Saturday evenings, likely driven by social and nightlife activity. Early morning hours consistently show the lowest activity across all days."
)

