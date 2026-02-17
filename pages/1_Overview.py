import streamlit as st
import polars as pl
from utils import load_data

st.title("Overview")
st.markdown("A quick look at the cleaned + feature-engineered NYC Yellow Taxi dataset (Jan 2024).")

df = load_data()

# Metrics
st.subheader("Dataset at a Glance")

min_dt = df["tpep_pickup_datetime"].min()
max_dt = df["tpep_pickup_datetime"].max()

num_days = (max_dt.date() - min_dt.date()).days + 1

c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows", f"{df.height:,}")
c2.metric("Columns", f"{df.width:,}")
c3.metric("Date Coverage", f"{num_days} days")
c4.metric("Estimated Size", f"{df.estimated_size() / 1024**2:.1f} MB")

st.caption(f"Data spans from {min_dt.date()} to {max_dt.date()}.")

st.divider()

# Tabs
tab1, tab2, tab3 = st.tabs(["Statistics", "Data Sample", "Column Info"])

with tab1:
    st.subheader("Summary Statistics")
    numeric_cols = [c for c, t in zip(df.columns, df.dtypes) if t.is_numeric()]
    default_cols = [c for c in ["fare_amount","trip_distance","tip_amount","total_amount","trip_duration_minutes","trip_speed_mph"] if c in df.columns]

    selected = st.multiselect("Pick numeric columns:", numeric_cols, default=default_cols)

    if selected:
        st.dataframe(df.select(selected).describe().to_dicts(), use_container_width=True)
    else:
        st.warning("Select at least one numeric column.")

with tab2:
    st.subheader("Peek at the Data")
    n = st.slider("Rows to preview", min_value=5, max_value=200, value=20, step=5)

    cols = st.multiselect("Columns to show:", df.columns, default=df.columns[:6])
    if cols:
        st.dataframe(df.select(cols).head(n).to_dicts(), use_container_width=True)
    else:
        st.warning("Select at least one column.")

with tab3:
    st.subheader("Column Info")
    info_df = pl.DataFrame({
        "Column": df.columns,
        "Type": [str(t) for t in df.dtypes],
        "Non-Null": [(df.height - df[c].null_count()) for c in df.columns],
        "Null %": [round(df[c].null_count() / df.height * 100, 2) for c in df.columns],
    })
    st.dataframe(info_df.to_dicts(), use_container_width=True)


st.divider()

# Data Quality Checks
st.subheader("Data Quality Check")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Missing Values**")
    missing = pl.DataFrame({
        "Column": df.columns,
        "Missing": [df[c].null_count() for c in df.columns],
    }).with_columns(
        (pl.col("Missing") / df.height * 100).round(2).alias("Missing %")
    ).filter(pl.col("Missing") > 0).sort("Missing", descending=True)

    if missing.height > 0:
        st.dataframe(missing.to_dicts(), use_container_width=True)

    else:
        st.success("No missing values in this dataset.")

with col2:
    st.markdown("**Value Ranges (sanity check)**")
    range_cols = [c for c in ["fare_amount","trip_distance","tip_amount","passenger_count","trip_duration_minutes"] if c in df.columns]

    if range_cols:
        ranges = []
        for c in range_cols:
            ranges.append({
                "Column": c,
                "Min": df[c].min(),
                "Max": df[c].max(),
            })
        st.dataframe(pl.DataFrame(ranges).to_dicts(), use_container_width=True)
    else:
        st.info("No range-check columns found.")
