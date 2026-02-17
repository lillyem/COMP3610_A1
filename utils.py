import polars as pl
import requests
from pathlib import Path
import streamlit as st

RAW_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

@st.cache_data(show_spinner="Downloading and preparing data...")
def load_data():
    data_dir = Path("data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)

    data_path = data_dir / "yellow_tripdata_2024-01.parquet"

    if (not data_path.exists()) or (data_path.stat().st_size == 0):
        with requests.get(RAW_URL, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(data_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

    df = pl.read_parquet(str(data_path))

    # Ensure datetimes + correct dtypes
    df = df.with_columns([
        pl.col("tpep_pickup_datetime").cast(pl.Datetime, strict=False),
        pl.col("tpep_dropoff_datetime").cast(pl.Datetime, strict=False),
        pl.col("PULocationID").cast(pl.Int32),
        pl.col("DOLocationID").cast(pl.Int32),
        pl.col("payment_type").cast(pl.Int32),
    ])

    # Cleaning
    df = df.filter(
        pl.col("tpep_pickup_datetime").is_not_null() &
        pl.col("tpep_dropoff_datetime").is_not_null() &
        pl.col("PULocationID").is_not_null() &
        pl.col("DOLocationID").is_not_null() &
        pl.col("fare_amount").is_not_null() &
        (pl.col("trip_distance") > 0) &
        (pl.col("fare_amount") >= 0) &
        (pl.col("fare_amount") <= 500) &
        (pl.col("tpep_dropoff_datetime") >= pl.col("tpep_pickup_datetime"))
    )

    # Feature engineering
    df = df.with_columns([
        ((pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime"))
         .dt.total_seconds() / 60).alias("trip_duration_minutes"),
        pl.col("tpep_pickup_datetime").dt.hour().alias("pickup_hour"),
        pl.col("tpep_pickup_datetime").dt.strftime("%A").alias("pickup_day_of_week"),
        pl.col("tpep_pickup_datetime").dt.date().alias("pickup_date"),
    ])

    df = df.with_columns([
        pl.when(pl.col("trip_duration_minutes") > 0)
        .then(pl.col("trip_distance") / (pl.col("trip_duration_minutes") / 60))
        .otherwise(0)
        .alias("trip_speed_mph")
    ])

    return df


@st.cache_data(show_spinner="Loading taxi zone lookup...")
def load_lookup():
    lookup_dir = Path("data/raw")
    lookup_dir.mkdir(parents=True, exist_ok=True)

    lookup_path = lookup_dir / "taxi_zone_lookup.csv"

    if not lookup_path.exists():
        r = requests.get(LOOKUP_URL, timeout=60)
        r.raise_for_status()
        lookup_path.write_bytes(r.content)

    return pl.read_csv(str(lookup_path))
