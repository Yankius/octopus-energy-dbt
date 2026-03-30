import os
import requests
import duckdb
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, UTC

# --- Load into DuckDB ---
con = duckdb.connect("octopus.duckdb")



print(con.execute("SELECT *  FROM raw_octopus_consumption LIMIT 10; ").fetchdf())