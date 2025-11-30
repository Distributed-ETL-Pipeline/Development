import os
import zipfile
import io
import requests
import duckdb
import pandas as pd

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "marta.duckdb")

# Download GTFS static zip
r = requests.get("https://itsmarta.com/google_transit_feed/google_transit.zip")
with zipfile.ZipFile(io.BytesIO(r.content)) as z:
    # Extract and load stops, trips, and routes
    conn = duckdb.connect(DB_PATH)
    for fname in ["stops.txt", "trips.txt", "routes.txt"]:
        with z.open(fname) as f:
            df = pd.read_csv(f)
            conn.register(fname.replace(".txt", ""), df)
            conn.execute(f"CREATE OR REPLACE TABLE {fname.replace('.txt','')} AS SELECT * FROM {fname.replace('.txt','')}")
    conn.close()