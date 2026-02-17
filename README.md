# NYC Yellow Taxi Data Analysis Dashboard (January 2024)

This project analyzes NYC Yellow Taxi trip data for January 2024.  
It includes data ingestion, validation, cleaning, feature engineering, SQL analysis using DuckDB, and an interactive Streamlit dashboard.

---

## Project Overview

This repository contains:

- Programmatic data download (no manual downloads)
- Data validation checks
- Data cleaning and transformation
- Feature engineering (4 derived columns)
- SQL analysis using DuckDB
- Interactive Streamlit dashboard with filters and visualizations

Dataset: NYC Yellow Taxi Trip Data (January 2024)  
Source: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-
01.parquet

---

## Setup Instructions

### Clone the repository

- git clone repository-url
- cd repository-name

### Create and activate a virtual environment

Windows:
- python -m venv .venv
- .venv\Scripts\activate


Mac/Linux:
- python3 -m venv .venv
- source .venv/bin/activate

### Install dependencies
- pip install -r requirements.txt
  
---
### Run the Notebook (Data Processing)

Open:

- notebooks/assignment1.ipynb


Run all cells to:

- Download raw data programmatically

- Validate dataset structure

- Clean invalid records

- Create engineered features

- Perform SQL analysis using DuckDB

- Export processed dataset to: data/processed/taxi_cleaned_features.parquet

---
### Run the Streamlit Dashboard

- From the project root directory:

- streamlit run app.py
 
The dashboard will open automatically in your browser.

- Live Dashboard URL:

(insert link here)

