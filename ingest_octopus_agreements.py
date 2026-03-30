import duckdb
import pandas as pd
from datetime import datetime, timezone
import uuid
import os
from dotenv import load_dotenv

# =========================
# 1. CONFIGURATION
# =========================
load_dotenv("environment.env")
DB_PATH = os.getenv("DUCKDB_PATH")

if not DB_PATH:
    raise ValueError("DUCKDB_PATH is not set")

run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)

# =========================
# 2. AGREEMENTS INPUT
# =========================
agreements = [
    {
        "tariff_code": "E-1R-VAR-21-05-19-A",
        "valid_from": "2021-06-29T00:00:00+01:00",
        "valid_to": "2022-12-05T00:00:00Z"
    },
    {
        "tariff_code": "E-1R-INTELLI-VAR-22-10-14-A",
        "valid_from": "2022-12-05T00:00:00Z",
        "valid_to": "2025-07-01T00:00:00+01:00"
    },
    {
        "tariff_code": "E-1R-INTELLI-VAR-24-10-29-A",
        "valid_from": "2025-07-01T00:00:00+01:00",
        "valid_to": None
    }
]

# =========================
# 3. DB INITIALISATION
# =========================
con = duckdb.connect(DB_PATH)

# --- Agreements table ---
con.execute("""
CREATE TABLE IF NOT EXISTS raw_octopus_agreements (
    tariff_code VARCHAR,
    valid_from_utc TIMESTAMP WITH TIME ZONE,
    valid_to_utc TIMESTAMP WITH TIME ZONE,
    valid_from_uk TIMESTAMP,
    valid_to_uk TIMESTAMP,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tariff_code, valid_from_utc)
)
""")

# --- Audit table (reuse pattern) ---
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
# 4. INGESTION LOGIC
# =========================
try:
    df = pd.DataFrame(agreements)

    # --- Transform ---
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

    df_to_load = df[[
        "tariff_code",
        "valid_from_utc",
        "valid_to_utc",
        "valid_from_uk",
        "valid_to_uk"
    ]].drop_duplicates(subset=["tariff_code", "valid_from_utc"])

    # =========================
    # 5. IDEMPOTENT LOAD
    # =========================
    before_count = con.execute("""
        SELECT COUNT(*) FROM raw_octopus_agreements
    """).fetchone()[0]

    con.execute("""
        INSERT OR REPLACE INTO raw_octopus_agreements (
            tariff_code,
            valid_from_utc,
            valid_to_utc,
            valid_from_uk,
            valid_to_uk
        )
        SELECT
            tariff_code,
            valid_from_utc,
            valid_to_utc,
            valid_from_uk,
            valid_to_uk
        FROM df_to_load
    """)

    after_count = con.execute("""
        SELECT COUNT(*) FROM raw_octopus_agreements
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
        "octopus_agreements",
        start_time,
        end_time,
        "SUCCESS",
        len(df_to_load),
        inserted,
        df_to_load["valid_from_utc"].min(),
        df_to_load["valid_to_utc"].max(),
        None
    ])

    print("===================================")
    print(f"Run ID: {run_id}")
    print(f"Loaded agreements: {len(df_to_load)}")
    print(f"Inserted/Updated: {inserted}")
    print("STATUS: SUCCESS")
    print("===================================")

except Exception as e:
    end_time = datetime.now(timezone.utc)

    con.execute("""
        INSERT INTO ingestion_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        run_id,
        "octopus_agreements",
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
