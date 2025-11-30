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
        logger.info("[1] Loading vehicle positions from DuckDB")
        con = duckdb.connect(DUCKDB_FILE)

        df = con.execute("SELECT * FROM vehicles").df()
        logger.info(f"[2] {len(df)} vehicle rows loaded")

        # Example transformation: keep only latest positions per vehicle
        df_latest = df.sort_values("timestamp").drop_duplicates("vehicle_id", keep="last")

        logger.info(f"[3] Saving latest positions into latest_vehicles table")
        con.execute("CREATE TABLE IF NOT EXISTS latest_vehicles AS SELECT * FROM df_latest")
        con.close()
        logger.info("Vehicles load complete.")
    except Exception as e:
        logger.exception("Error in load_vehicles_worker")
        raise

if __name__ == "__main__":
    main()
