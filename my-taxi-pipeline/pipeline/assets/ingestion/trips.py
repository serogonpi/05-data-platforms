"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: bruin-project

materialization:
  type: table
  strategy: create+replace

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
  - name: taxi_type
    type: string
    description: "Yellow or green taxi"
  - name: passenger_count
    type: integer
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: payment_type
    type: integer
    description: "Payment type code"
  - name: fare_amount
    type: float
    description: "Meter fare amount"
  - name: tip_amount
    type: float
    description: "Tip amount"
  - name: tolls_amount
    type: float
    description: "Tolls amount"
  - name: total_amount
    type: float
    description: "Total charged to passenger"
  - name: pu_location_id
    type: integer
    description: "TLC pickup zone ID"
  - name: do_location_id
    type: integer
    description: "TLC dropoff zone ID"
  - name: rate_code_id
    type: integer
    description: "Rate code (1=standard, 2=JFK, etc.)"
  - name: congestion_surcharge
    type: float
    description: "NYC congestion surcharge"
  - name: vendor_id
    type: integer
    description: "TPEP provider code"
  - name: extracted_at
    type: timestamp
    description: "When this record was extracted"
@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
import os
import json
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
import requests


def _parse_date(s):
  # ensure pandas parses datetimes robustly
  return pd.to_datetime(s, utc=True)


def _month_range(start_date, end_date):
  # yields (year, month) tuples inclusive of start and exclusive of end's month if end at month boundary
  cur = pd.to_datetime(start_date)
  end = pd.to_datetime(end_date)
  cur = cur.replace(day=1)
  end = end.replace(day=1)
  while cur <= end:
    yield cur.year, cur.month
    cur = (cur + pd.DateOffset(months=1)).replace(day=1)


def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    # Read environment-provided dates/vars
    start = os.environ.get("BRUIN_START_DATE")
    end = os.environ.get("BRUIN_END_DATE")
    if not start or not end:
      raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be provided")

    vars_json = os.environ.get("BRUIN_VARS", "{}")
    try:
      vars_obj = json.loads(vars_json)
    except Exception:
      vars_obj = {}
    taxi_types = vars_obj.get("taxi_types", ["yellow"])

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    frames = []
    extracted_at = datetime.now(timezone.utc)

    for taxi_type in taxi_types:
      for year, month in _month_range(start, end):
        month_str = f"{month:02d}"
        filename = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"
        url = base_url + filename
        try:
          resp = requests.get(url, timeout=30)
          if resp.status_code != 200:
            # skip missing months but log via print for visibility
            print(f"skipping {url} (status {resp.status_code})")
            continue
          # read parquet bytes into pandas
          bio = BytesIO(resp.content)
          df = pd.read_parquet(bio)
          if df.empty:
            continue
          # normalize column names and cast common columns
          df = df.rename(columns=lambda c: c.strip())
          df["taxi_type"] = taxi_type
          # standardize datetime columns if present
          for col in ["pickup_datetime", "tpep_pickup_datetime", "lpep_pickup_datetime"]:
            if col in df.columns:
              df["pickup_datetime"] = _parse_date(df[col])
              break
          for col in ["dropoff_datetime", "tpep_dropoff_datetime", "lpep_dropoff_datetime"]:
            if col in df.columns:
              df["dropoff_datetime"] = _parse_date(df[col])
              break
          # map source columns to standardized names
          for src, dst in [
            ("payment_type", "payment_type"),
            ("fare_amount", "fare_amount"),
            ("trip_distance", "trip_distance"),
            ("passenger_count", "passenger_count"),
            ("tip_amount", "tip_amount"),
            ("tolls_amount", "tolls_amount"),
            ("total_amount", "total_amount"),
            ("PULocationID", "pu_location_id"),
            ("DOLocationID", "do_location_id"),
            ("RatecodeID", "rate_code_id"),
            ("congestion_surcharge", "congestion_surcharge"),
            ("VendorID", "vendor_id"),
          ]:
            if src in df.columns:
              df[dst] = df[src]
          df["extracted_at"] = extracted_at
          frames.append(df)
        except Exception as e:
          print(f"error fetching {url}: {e}")
          continue

    all_columns = [
      "pickup_datetime", "dropoff_datetime", "taxi_type",
      "passenger_count", "trip_distance", "payment_type",
      "fare_amount", "tip_amount", "tolls_amount", "total_amount",
      "pu_location_id", "do_location_id", "rate_code_id",
      "congestion_surcharge", "vendor_id", "extracted_at",
    ]

    if not frames:
      # return empty dataframe with expected schema
      return pd.DataFrame(columns=all_columns).astype(object)

    out = pd.concat(frames, ignore_index=True, sort=False)
    # coerce types for the columns declared above
    if "pickup_datetime" in out.columns:
      out["pickup_datetime"] = pd.to_datetime(out["pickup_datetime"], utc=True, errors="coerce")
    if "dropoff_datetime" in out.columns:
      out["dropoff_datetime"] = pd.to_datetime(out["dropoff_datetime"], utc=True, errors="coerce")
    for col in ["passenger_count", "rate_code_id", "vendor_id", "pu_location_id", "do_location_id"]:
      if col in out.columns:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    for col in ["trip_distance", "fare_amount", "tip_amount", "tolls_amount", "total_amount", "congestion_surcharge"]:
      if col in out.columns:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if "payment_type" in out.columns:
      out["payment_type"] = pd.to_numeric(out["payment_type"], errors="coerce").astype("Int64")

    # return DataFrame for Bruin to materialize (append strategy)
    return out[[c for c in all_columns if c in out.columns]]


