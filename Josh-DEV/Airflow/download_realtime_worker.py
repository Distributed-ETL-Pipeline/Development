import requests
import pandas as pd
import duckdb
from google.transit import gtfs_realtime_pb2
import io
import logging
import os
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)
DUCKDB_FILE = os.path.join(DATA_DIR, "marta_realtime.duckdb")

# GTFS Realtime URL
GTFS_REALTIME_URL = "https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/vehicle/vehiclepositions.pb"

def download_gtfs_realtime():
    logger.info(f"[1] Downloading GTFS-Realtime feed from {GTFS_REALTIME_URL}")
    response = requests.get(GTFS_REALTIME_URL, timeout=30)
    response.raise_for_status()
    return response.content

def parse_vehicle_positions(pb_data):
    logger.info("[2] Parsing GTFS-Realtime VehiclePositions.pb")
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(pb_data)
    rows = []
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle
            row = {
                "vehicle_id": v.vehicle.id,
                "trip_id": v.trip.trip_id if v.trip.trip_id else None,
                "route_id": v.trip.route_id if v.trip.route_id else None,
                "timestamp": datetime.fromtimestamp(v.timestamp) if v.timestamp else None,
                "latitude": v.position.latitude if v.position else None,
                "longitude": v.position.longitude if v.position else None,
                "bearing": v.position.bearing if v.position else None,
                "speed": v.position.speed if v.position else None,
            }
            rows.append(row)
    return pd.DataFrame(rows)

def load_to_duckdb(df):
    logger.info(f"[3] Loading {len(df)} rows into DuckDB at {DUCKDB_FILE}")
    con = duckdb.connect(DUCKDB_FILE)
    con.execute("""
        CREATE TABLE IF NOT EXISTS vehicles (
            vehicle_id VARCHAR,
            trip_id VARCHAR,
            route_id VARCHAR,
            timestamp TIMESTAMP,
            latitude DOUBLE,
            longitude DOUBLE,
            bearing DOUBLE,
            speed DOUBLE
        )
    """)
    con.execute("INSERT INTO vehicles SELECT * FROM df")
    con.close()

def main():
    try:
        pb_data = download_gtfs_realtime()
        df = parse_vehicle_positions(pb_data)
        load_to_duckdb(df)
        logger.info("Download and load complete.")
    except Exception as e:
        logger.exception("Error in download_realtime_worker")
        raise

if __name__ == "__main__":
    main()
