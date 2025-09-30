import pandas as pd
import glob
import duckdb
import time
import os

def connect_to_db(db_name="testDB.db"):
    connection = duckdb.connect(db_name)

    return connection

def query_db(connection, query):
    return connection.execute(query).df()

def create_tables_from_csv(connection, dataset_path):
    # Get all CSV files in the dataset folder
    csv_files = [f for f in os.listdir(dataset_path) if f.endswith('.csv')]

    for csv_file in csv_files:
        table_name = os.path.splitext(csv_file)[0]
        csv_path = os.path.join(dataset_path, csv_file)

        # Create table and load data from CSV
        connection.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')")
        print(f"Table '{table_name}' created for file '{csv_file}'")

if __name__ == "__main__":
    print("Running duckDbTest2.py")

    conn = connect_to_db("testDB.db")

    # # Path to the dataset folder
    # dataset_path = "Dataset"

    # # Create tables for all CSV files in the dataset folder
    # create_tables_from_csv(conn, dataset_path)

    # # Query to list all tables in the database
    # tables = query_db(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
    # print("Tables in the database:")
    # print(tables)

    example_table = "Sales_Product_Combined" 
    result = query_db(conn, f"SELECT * FROM {example_table} LIMIT 100")    
    print(f"Data from table '{example_table}':")
    print(result)

    dallas_results = query_db(conn, "SELECT Product FROM Sales_Product_Combined ORDER BY Product DESC LIMIT 10")
    print("Products from Dallas:")
    print(dallas_results)

    distinct_cities = query_db(conn, "SELECT DISTINCT City FROM Sales_Product_Combined")
    print("Distinct Cities in Sales_Product_Combined:")
    print(distinct_cities)

