import os
import uuid
import requests
import duckdb
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# =========================
# 1. CONFIGURATION
# =========================
load_dotenv("environment.env")

API_KEY = os.getenv("OCTOPUS_API_KEY")
MPAN = os.getenv("OCTOPUS_EMPAN")
SERIAL = os.getenv("OCTOPUS_ESERIAL")

if not all([API_KEY, MPAN, SERIAL]):
    raise ValueError("Missing mandatory environment variables.")

DB_PATH = os.getenv("DUCKDB_PATH")

if not DB_PATH:
    raise ValueError("DUCKDB_PATH is not set")

BASE_URL = (
    f"https://api.octopus.energy/v1/electricity-meter-points/"
    f"{MPAN}/meters/{SERIAL}/consumption/"
)

# Unique run identifier
run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)

# =========================
# 2. DB INITIALISATION
# =========================
con = duckdb.connect(DB_PATH)

con.execute("""
CREATE TABLE IF NOT EXISTS raw_octopus_consumption (
    interval_start_utc TIMESTAMP WITH TIME ZONE PRIMARY KEY,
    interval_start_uk TIMESTAMP,
    kwh DOUBLE,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS ingestion_audit (
    run_id VARCHAR,
    source VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR,
    records_fetched INTEGER,
    records_inserted INTEGER,
    min_interval TIMESTAMP,
    max_interval TIMESTAMP,
    error_message VARCHAR
)
""")

# =========================
# 3. INGESTION LOGIC
# =========================
try:
    # --- Incremental with lookback ---
    last_ts = con.execute("""
        SELECT MAX(interval_start_utc)
        FROM raw_octopus_consumption
    """).fetchone()[0]

    if last_ts:
        # Lookback window (handles late-arriving corrections)
        period_from_dt = last_ts - timedelta(days=1)
        print(f"Incremental run with lookback from: {period_from_dt}")
    else:
        period_from_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        print("Initial load from 2024-01-01")

    period_from = period_from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "period_from": period_from,
        "order_by": "period",
        "page_size": 25000
    }

    results = []
    url = BASE_URL

    # --- Pagination loop ---
    while url:
        response = requests.get(url, params=params, auth=(API_KEY, ""))
        response.raise_for_status()

        payload = response.json()
        batch = payload.get("results", [])

        results.extend(batch)

        print(f"Fetched {len(batch)} records (total: {len(results)})")

        url = payload.get("next")
        params = None  # next already includes params

    if not results:
        raise ValueError("No data returned from API")

    # =========================
    # 4. TRANSFORMATION
    # =========================
    df = pd.DataFrame(results)

    ts_col = (
        "interval_start"
        if "interval_start" in df.columns
        else "consumption_start"
    )

    # ✅ Critical fix: normalise mixed timezones
    df["interval_start_utc"] = pd.to_datetime(df[ts_col], utc=True)

    # UK local time (for tariffs & reporting)
    df["interval_start_uk"] = (
        df["interval_start_utc"]
        .dt.tz_convert("Europe/London")
        .dt.tz_localize(None)
    )

    df["kwh"] = df["consumption"].astype(float)

    df_to_load = df[
        ["interval_start_utc", "interval_start_uk", "kwh"]
    ].drop_duplicates(subset=["interval_start_utc"])

    # =========================
    # 5. IDEMPOTENT LOAD
    # =========================
    before_count = con.execute("""
        SELECT COUNT(*) FROM raw_octopus_consumption
    """).fetchone()[0]

    con.execute("""
    INSERT OR REPLACE INTO raw_octopus_consumption (
        interval_start_utc,
        interval_start_uk,
        kwh
    )
    SELECT
        interval_start_utc,
        interval_start_uk,
        kwh
    FROM df_to_load
""")

    after_count = con.execute("""
        SELECT COUNT(*) FROM raw_octopus_consumption
    """).fetchone()[0]

    inserted = after_count - before_count

    end_time = datetime.now(timezone.utc)

    # =========================
    # 6. AUDIT SUCCESS
    # =========================
    con.execute("""
        INSERT INTO ingestion_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        run_id,
        "octopus_api",
        start_time,
        end_time,
        "SUCCESS",
        len(df_to_load),
        inserted,
        df_to_load["interval_start_utc"].min(),
        df_to_load["interval_start_utc"].max(),
        None
    ])

    print("===================================")
    print(f"Run ID: {run_id}")
    print(f"Fetched: {len(df_to_load)}")
    print(f"Inserted/Updated: {inserted}")
    print("STATUS: SUCCESS")
    print("===================================")

except Exception as e:
    end_time = datetime.now(timezone.utc)

    con.execute("""
        INSERT INTO ingestion_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        run_id,
        "octopus_api",
        start_time,
        end_time,
        "FAILED",
        0,
        0,
        None,
        None,
        str(e)
    ])

    print("===================================")
    print(f"Run ID: {run_id}")
    print("STATUS: FAILED")
    print(f"Error: {e}")
    print("===================================")

    raise

finally:
    con.close()
