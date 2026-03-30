import os
import uuid
import requests
import duckdb
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# =========================
# 1. CONFIG
# =========================
load_dotenv("environment.env")

API_KEY = os.getenv("OCTOPUS_API_KEY")
DB_PATH = os.getenv("DUCKDB_PATH")

if not DB_PATH:
    raise ValueError("DUCKDB_PATH is not set")

run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)

con = duckdb.connect(DB_PATH)

# =========================
# 2. TABLES
# =========================
con.execute("""
CREATE TABLE IF NOT EXISTS raw_octopus_tariffs (
    tariff_code VARCHAR,
    product_code VARCHAR,
    valid_from_utc TIMESTAMP WITH TIME ZONE,
    valid_to_utc TIMESTAMP WITH TIME ZONE,
    valid_from_uk TIMESTAMP,
    valid_to_uk TIMESTAMP,
    unit_rate DOUBLE,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tariff_code, valid_from_utc)
)
""")

# =========================
# 3. GET AGREEMENTS
# =========================
agreements_df = con.execute("""
    SELECT DISTINCT tariff_code
    FROM raw_octopus_agreements
""").df()

if agreements_df.empty:
    raise ValueError("No agreements found. Run agreements ingestion first.")

# =========================
# 4. FETCH TARIFFS
# =========================
all_results = []

def extract_product_code(tariff_code):
    # Example: E-1R-INTELLI-VAR-22-10-14-A
    parts = tariff_code.split("-")
    return "-".join(parts[2:-1])  # remove E-1R and region

for _, row in agreements_df.iterrows():
    tariff_code = row["tariff_code"]
    product_code = extract_product_code(tariff_code)

    print(f"Fetching tariff: {tariff_code}")

    url = f"https://api.octopus.energy/v1/products/{product_code}/electricity-tariffs/{tariff_code}/standard-unit-rates/"

    params = {"page_size": 25000}

    while url:
        response = requests.get(url, params=params, auth=(API_KEY, ""))
        response.raise_for_status()

        payload = response.json()
        results = payload.get("results", [])

        for r in results:
            r["tariff_code"] = tariff_code
            r["product_code"] = product_code

        all_results.extend(results)

        url = payload.get("next")
        params = None

if not all_results:
    raise ValueError("No tariff data fetched")

# =========================
# 5. TRANSFORM
# =========================
df = pd.DataFrame(all_results)

df["valid_from_utc"] = pd.to_datetime(df["valid_from"], utc=True)
df["valid_to_utc"] = pd.to_datetime(df["valid_to"], utc=True)

df["valid_from_uk"] = (
    df["valid_from_utc"]
    .dt.tz_convert("Europe/London")
    .dt.tz_localize(None)
)

df["valid_to_uk"] = (
    df["valid_to_utc"]
    .dt.tz_convert("Europe/London")
    .dt.tz_localize(None)
)

df["unit_rate"] = df["value_inc_vat"].astype(float)

df_to_load = df[[
    "tariff_code",
    "product_code",
    "valid_from_utc",
    "valid_to_utc",
    "valid_from_uk",
    "valid_to_uk",
    "unit_rate"
]].drop_duplicates(subset=["tariff_code", "valid_from_utc"])

# =========================
# 6. LOAD
# =========================
before = con.execute("SELECT COUNT(*) FROM raw_octopus_tariffs").fetchone()[0]

con.execute("""
INSERT OR REPLACE INTO raw_octopus_tariffs (
    tariff_code,
    product_code,
    valid_from_utc,
    valid_to_utc,
    valid_from_uk,
    valid_to_uk,
    unit_rate
)
SELECT
    tariff_code,
    product_code,
    valid_from_utc,
    valid_to_utc,
    valid_from_uk,
    valid_to_uk,
    unit_rate
FROM df_to_load
""")

after = con.execute("SELECT COUNT(*) FROM raw_octopus_tariffs").fetchone()[0]

inserted = after - before
end_time = datetime.now(timezone.utc)

print("===================================")
print(f"Run ID: {run_id}")
print(f"Tariffs fetched: {len(df_to_load)}")
print(f"Inserted/Updated: {inserted}")
print("STATUS: SUCCESS")
print("===================================")

con.close()
