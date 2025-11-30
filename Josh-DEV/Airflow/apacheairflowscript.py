from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging
import sys
import os

STATIC_GTFS_URL = "https://itsmarta.com/google_transit_feed/google_transit.zip"
STATIC_WORKER = "load_static_worker.py"

DAG_ID = "gtfs_realtime_duckdb"
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

default_args = {
    "owner": "you",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=2),
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="Download MARTA GTFS-Realtime and load into DuckDB safely",
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2025, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gtfs", "duckdb", "realtime"],
)

def run_worker(script_name, **context):
    """Run a worker script in a subprocess for safety on macOS ARM."""
    script_path = os.path.join(BASE_DIR, script_name)
    logging.info(f"Running worker script: {script_path}")
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            check=True,
        )
        logging.info(result.stdout)
        return "Success"
    except subprocess.CalledProcessError as e:
        logging.error("Subprocess failed:")
        logging.error(e.stdout)
        logging.error(e.stderr)
        raise

load_static_data = PythonOperator(
    task_id="load_static_data",
    python_callable=run_worker,
    op_kwargs={"script_name": STATIC_WORKER},
    dag=dag,
)

download_realtime = PythonOperator(
    task_id="download_realtime",
    python_callable=run_worker,
    op_kwargs={"script_name": "download_realtime_worker.py"},
    dag=dag,
)

load_trips_duckdb = PythonOperator(
    task_id="load_trips_duckdb",
    python_callable=run_worker,
    op_kwargs={"script_name": "load_trips_worker.py"},
    dag=dag,
)

load_vehicles_duckdb = PythonOperator(
    task_id="load_vehicles_duckdb",
    python_callable=run_worker,
    op_kwargs={"script_name": "load_vehicles_worker.py"},
    dag=dag,
)

# DAG dependencies
load_static_data >> download_realtime
download_realtime >> [load_trips_duckdb, load_vehicles_duckdb]
