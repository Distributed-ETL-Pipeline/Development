"""
GTFS Database Manager
Simple interface for managing GTFS data updates and queries.
"""

import os
import sys
from pathlib import Path
from gtfs_processor import GTFSProcessor
import duckdb

class GTFSManager:
    """
        Manager class for GTFS database operations.

    """
    
    def __init__(self, db_path: str = "gtfs_database.db"):
        self.db_path = db_path
        self.processor = GTFSProcessor(db_path)
    
    def initial_load(self, gtfs_folder: str):
        """
            Perform initial load of GTFS data.
            Used when setting up the database for the first time.
            If the database already exists, it will be overwritten.
        """
        print("Starting initial GTFS data load...")
        self.processor.load_gtfs_data(gtfs_folder)
        print("Initial load completed!")
    
    def update_data(self, gtfs_folder: str):
        """
            Update existing GTFS data with new files.
            Currently performs a full replacement of data.
            No difference from initial load in this implementation.
            Uses the same load_gtfs_data method.
        """
        print("Updating GTFS data...")
        self.processor.update_gtfs_data(gtfs_folder)
        print("Data update completed!")

    def show_stats(self):
        """Display database statistics."""
        print("\nDATABASE STATISTICS")
        print("=" * 50)
        
        stats = self.processor.get_database_stats()
        for table, count in stats.items():
            print(f"{table.ljust(15)}: {count:,} rows")
        
        # Show some sample queries
        print("\nSAMPLE DATA INSIGHTS")
        print("=" * 50)
        
        try:
            # Routes by type
            result = self.processor.conn.execute("""
                SELECT route_type, COUNT(*) as count 
                FROM routes 
                GROUP BY route_type 
                ORDER BY count DESC
            """).fetchall()
            
            print("Routes by type:")
            route_types = {0: "Tram/Light Rail", 1: "Subway/Metro", 2: "Rail", 3: "Bus", 4: "Ferry", 5: "Cable Tram", 6: "Aerial Lift", 7: "Funicular"}
            for route_type, count in result:
                type_name = route_types.get(route_type, f"Type {route_type}")
                print(f"  {type_name}: {count}")
            
            # Service date range
            result = self.processor.conn.execute("""
                SELECT MIN(start_date) as earliest, MAX(end_date) as latest 
                FROM calendar
            """).fetchone()
            
            if result and result[0]:
                print(f"\nService period: {result[0]} to {result[1]}")
            
            # Stop coverage
            result = self.processor.conn.execute("""
                SELECT 
                    MIN(stop_lat) as min_lat, MAX(stop_lat) as max_lat,
                    MIN(stop_lon) as min_lon, MAX(stop_lon) as max_lon
                FROM stops
            """).fetchone()
            
            if result:
                print(f"Geographic coverage: Lat {result[0]:.4f} to {result[1]:.4f}, Lon {result[2]:.4f} to {result[3]:.4f}")
            
        except Exception as e:
            print(f"Error generating insights: {e}")
    
    def validate_database(self):
        """Run validation checks on the database."""
        print("\nVALIDATION CHECKS")
        print("=" * 50)
        
        checks = []
        
        try:
            # Check for orphaned trips (trips without valid routes)
            result = self.processor.conn.execute("""
                SELECT COUNT(*) FROM trips t 
                LEFT JOIN routes r ON t.route_id = r.route_id 
                WHERE r.route_id IS NULL
            """).fetchone()
            checks.append(("Orphaned trips (no matching route)", result[0]))
            
            # Check for orphaned stop_times (stop_times without valid trips)
            result = self.processor.conn.execute("""
                SELECT COUNT(*) FROM stop_times st 
                LEFT JOIN trips t ON st.trip_id = t.trip_id 
                WHERE t.trip_id IS NULL
            """).fetchone()
            checks.append(("Orphaned stop times (no matching trip)", result[0]))
            
            # Check for stops with invalid coordinates
            result = self.processor.conn.execute("""
                SELECT COUNT(*) FROM stops 
                WHERE stop_lat NOT BETWEEN -90 AND 90 
                   OR stop_lon NOT BETWEEN -180 AND 180
            """).fetchone()
            checks.append(("Stops with invalid coordinates", result[0]))
            
            # Check for routes without trips
            result = self.processor.conn.execute("""
                SELECT COUNT(*) FROM routes r 
                LEFT JOIN trips t ON r.route_id = t.route_id 
                WHERE t.route_id IS NULL
            """).fetchone()
            checks.append(("Routes without trips", result[0]))
            
            for check_name, count in checks:
                status = "PASS" if count == 0 else f"  {count} issues"
                print(f"{check_name.ljust(40)}: {status}")
            
        except Exception as e:
            print(f"Error running validation: {e}")
    
    def run_custom_query(self, query: str):
        """Run a custom SQL query."""
        try:
            result = self.processor.conn.execute(query).fetchall()
            return result
        except Exception as e:
            print(f"Query error: {e}")
            return None
    
    def close(self):
        """Close database connection."""
        self.processor.close()


def main():
    """
        Mannual GTFS manager.
        Future implementations will have individual python scripts/modules for each function to be ran with Airflow.
    """
    
    # Default GTFS folder path
    default_gtfs_folder = r"INSERT PATH TO GTFS FOLDER HERE" # <------------------------------------------- Add GTFS folder path here
    
    manager = GTFSManager()
    
    try:
        while True:
            print("\n" + "="*60)
            print("GTFS DATABASE MANAGER")
            print("="*60)
            print("1. Initial load from GTFS files")
            print("2. Update database with new GTFS data")
            print("3. Show database statistics")
            print("4. Validate database")
            print("5. Run custom query")
            print("6. Exit")
            print("-" * 60)
            
            choice = input("Select option (1-6): ").strip()
            
            if choice == "1":
                folder = input(f"GTFS folder path (press Enter for default): ").strip()
                if not folder:
                    folder = default_gtfs_folder
                
                if os.path.exists(folder):
                    manager.initial_load(folder)
                else:
                    print(f"Folder not found: {folder}")
            
            elif choice == "2":
                folder = input(f"GTFS folder path (press Enter for default): ").strip()
                if not folder:
                    folder = default_gtfs_folder
                
                if os.path.exists(folder):
                    manager.update_data(folder)
                else:
                    print(f"Folder not found: {folder}")
            
            elif choice == "3":
                manager.show_stats()
            
            elif choice == "4":
                manager.validate_database()
            
            elif choice == "5":
                query = input("Enter SQL query: ").strip()
                if query:
                    result = manager.run_custom_query(query)
                    if result is not None:
                        print(f"\nQuery result ({len(result)} rows):")
                        for row in result[:10]:  # Show first 10 rows
                            print(row)
                        if len(result) > 10:
                            print(f"... and {len(result) - 10} more rows")
            
            elif choice == "6":
                break
            
            else:
                print("Invalid option. Please select 1-6.")
    
    except KeyboardInterrupt:
        print("\n\nProcess interrupted. Exiting...")
    
    finally:
        manager.close()


if __name__ == "__main__":
    main()