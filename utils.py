import polars as pl
import requests
from pathlib import Path
import streamlit as st

RAW_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

DATA_DIR = Path("data/raw")
PARQUET_PATH = DATA_DIR / "yellow_tripdata_2024-01.parquet"
LOOKUP_PATH = DATA_DIR / "taxi_zone_lookup.csv"


def _ensure_data_files() -> None:
    """Download raw files once (no caching). Keeps network + disk writes out of Streamlit cache."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Download trip parquet if missing/empty
    if (not PARQUET_PATH.exists()) or (PARQUET_PATH.stat().st_size == 0):
        with requests.get(RAW_URL, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(PARQUET_PATH, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

    # Download lookup CSV if missing/empty
    if (not LOOKUP_PATH.exists()) or (LOOKUP_PATH.stat().st_size == 0):
        r = requests.get(LOOKUP_URL, timeout=60)
        r.raise_for_status()
        LOOKUP_PATH.write_bytes(r.content)


@st.cache_data(show_spinner="Loading and preparing trip data...", ttl=60 * 60)
def load_data() -> pl.DataFrame:
    """Read parquet and do cleaning + feature engineering (cached)."""
    _ensure_data_files()

    df = pl.read_parquet(str(PARQUET_PATH))

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
        .otherwise(None)  # use None instead of 0 so averages aren't skewed
        .alias("trip_speed_mph")
    ])

    return df


@st.cache_data(show_spinner="Loading taxi zone lookup...", ttl=24 * 60 * 60)
def load_lookup() -> pl.DataFrame:
    """Load lookup table (cached)."""
    _ensure_data_files()
    return pl.read_csv(str(LOOKUP_PATH))
