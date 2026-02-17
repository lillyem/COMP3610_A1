import polars as pl
import requests
from pathlib import Path
import streamlit as st

RAW_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"


@st.cache_data(show_spinner="Downloading and preparing data...")
def load_data():
    """
    Downloads the NYC Yellow Taxi January 2024 dataset if missing,
    performs required cleaning and feature engineering,
    and returns a Polars DataFrame.
    """

    data_dir = Path("data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)

    data_path = data_dir / "yellow_tripdata_2024-01.parquet"

    # Download if missing or empty
    if (not data_path.exists()) or (data_path.stat().st_size == 0):
        with requests.get(RAW_URL, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(data_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

    # Load raw parquet
    df = pl.read_parquet(str(data_path))

    # Datetime handling 
    pickup_dtype = df.schema.get("tpep_pickup_datetime")
    dropoff_dtype = df.schema.get("tpep_dropoff_datetime")

    def coerce_datetime(col: str) -> pl.Expr:
        dtype = df.schema.get(col)

        # already datetime, keep as is
        if isinstance(dtype, pl.datatypes.Datetime):
            return pl.col(col)

        # string, parse
        if dtype == pl.Utf8:
            return pl.col(col).str.strptime(pl.Datetime, strict=False)

        if dtype in (pl.Int64, pl.Int32):
            return pl.from_epoch(pl.col(col), time_unit="us")

        return pl.col(col).cast(pl.Datetime, strict=False)

    df = df.with_columns([
    coerce_datetime("tpep_pickup_datetime").alias("tpep_pickup_datetime"),
    coerce_datetime("tpep_dropoff_datetime").alias("tpep_dropoff_datetime"),

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

    # Feature Engineering 
    df = df.with_columns([
        # i) Trip duration in minutes
        (
            (pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime"))
            .dt.total_seconds() / 60
        ).alias("trip_duration_minutes"),

        # k) Pickup hour
        pl.col("tpep_pickup_datetime").dt.hour().alias("pickup_hour"),

        # l) Pickup day of week
        pl.col("tpep_pickup_datetime").dt.strftime("%A").alias("pickup_day_of_week"),
    ])

    # j) Trip speed (must be after duration exists)
    df = df.with_columns([
        pl.when(pl.col("trip_duration_minutes") > 0)
        .then(pl.col("trip_distance") / (pl.col("trip_duration_minutes") / 60))
        .otherwise(0)
        .alias("trip_speed_mph")
    ])

    return df
