import os
import requests
import duckdb
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, UTC

# --- Load into DuckDB ---
con = duckdb.connect("octopus.duckdb")

con.execute("""
            DROP TABLE IF EXISTS raw_monthly_consumption
            """)

print(con.execute("DESCRIBE raw_monthly_consumption").fetchdf())