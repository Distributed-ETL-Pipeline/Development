import duckdb
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
DUCKDB_FILE = os.path.join(DATA_DIR, "marta_realtime.duckdb")

def main():
    try:
        logger.info("[1] Loading trips from DuckDB (vehicles table)")
        con = duckdb.connect(DUCKDB_FILE)

        # Example: derive trips from vehicle positions
        df = con.execute("""
            SELECT DISTINCT trip_id, route_id
            FROM vehicles
            WHERE trip_id IS NOT NULL
        """).df()

        logger.info(f"[2] Found {len(df)} trips, saving back to DuckDB")
        con.execute("""
            CREATE TABLE IF NOT EXISTS trips AS SELECT * FROM df
        """)
        con.close()
        logger.info("Trips load complete.")
    except Exception as e:
        logger.exception("Error in load_trips_worker")
        raise

if __name__ == "__main__":
    main()
