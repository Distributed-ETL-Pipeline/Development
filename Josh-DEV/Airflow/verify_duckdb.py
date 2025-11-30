# verify_duckdb.py
import duckdb
import os

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")

# List of databases to check
databases = ["marta.duckdb", "marta_realtime.duckdb"]

for db_file in databases:
    db_path = os.path.join(DATA_DIR, db_file)
    print(f"\n=== Checking database: {db_file} ===")
    conn = duckdb.connect(db_path)
    
    # List tables
    tables = conn.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]
    print("Tables:", table_names)
    
    # Sample data from known tables
    for table in ["stops", "trips", "routes", "vehicles"]:
        if table in table_names:
            sample = conn.execute(f"SELECT * FROM {table} LIMIT 5").fetchdf()
            print(f"\nSample data from {table}:")
            print(sample)
        else:
            print(f"\nTable '{table}' not found in {db_file}.")
    
    conn.close()
