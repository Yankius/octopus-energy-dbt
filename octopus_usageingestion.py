import os
import requests
import duckdb
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, UTC

# --- Load env ---
load_dotenv("environment.env")

API_KEY = os.getenv("OCTOPUS_API_KEY")
MPAN = os.getenv("OCTOPUS_EMPAN")
SERIAL = os.getenv("OCTOPUS_ESERIAL")

if not all([API_KEY, MPAN, SERIAL]):
    raise ValueError("Missing env vars")

BASE_URL = f"https://api.octopus.energy/v1/electricity-meter-points/{MPAN}/meters/{SERIAL}/consumption/"

# --- Params ---
params = {
    "period_from": "2024-01-01T00:00:00Z",
    "period_to": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "group_by": "month",   # <-- key feature
    "order_by": "period",
    "page_size": 25000
}

# --- Fetch with pagination ---
results = []
url = BASE_URL

while url:
    response = requests.get(url, params=params, auth=(API_KEY, ""))
    response.raise_for_status()

    payload = response.json()

    results.extend(payload["results"])

    # next already includes params → reset params!
    url = payload["next"]
    params = None

# --- Transform ---
df = pd.DataFrame(results)

if df.empty:
    print("No data returned")
    exit()

df = df.rename(columns={
    "interval_start": "month_start",
    "interval_end": "month_end",
    "consumption": "kwh"
})

df["month_start"] = pd.to_datetime(df["month_start"], utc=True)
df["month_end"] = pd.to_datetime(df["month_end"], utc=True)
df["kwh"] = df["kwh"].astype(float)

df["month_start"] = df["month_start"].dt.tz_convert("UTC").dt.tz_localize(None)
df["month_end"] = df["month_end"].dt.tz_convert("UTC").dt.tz_localize(None)

df["month"] = df["month_start"].dt.to_period("M").dt.to_timestamp()

print(df.dtypes)
print(df.head())

# --- Load into DuckDB ---
con = duckdb.connect("octopus.duckdb")

con.execute("""
CREATE TABLE IF NOT EXISTS raw_monthly_consumption (
    month DATE PRIMARY KEY,
    kwh DOUBLE

)
""")

# Upsert pattern (DuckDB-safe)
con.execute("""
INSERT OR REPLACE INTO raw_monthly_consumption (month, kwh)
SELECT month, kwh FROM df
""")

con.close()

print(f"Loaded {len(df)} monthly records")