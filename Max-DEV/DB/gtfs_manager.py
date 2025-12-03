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
    """manages GTFS database operations"""
    
    def __init__(self, db_path: str = "gtfs_database.db"):
        self.db_path = db_path
        self.processor = GTFSProcessor(db_path)
    
    def initial_load(self, gtfs_folder: str):
        """load gtfs data into database"""
        print("Starting initial GTFS data load...")
        self.processor.load_gtfs_data(gtfs_folder)
        print("Initial load completed!")
    
    def update_data(self, gtfs_folder: str):
        """update database with new gtfs data"""
        print("Updating GTFS data...")
        self.processor.update_gtfs_data(gtfs_folder)
        print("Data update completed!")

    def show_stats(self):
        """show database statistics"""
        print("\nDATABASE STATISTICS")
        print("=" * 50)
        
        stats = self.processor.get_database_stats()
        for table, count in stats.items():
            print(f"{table.ljust(15)}: {count:,} rows")
        
        # sample insights
        print("\nSAMPLE DATA INSIGHTS")
        print("=" * 50)
        
        try:
            # routes by type
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
            
            # service date range
            result = self.processor.conn.execute("""
                SELECT MIN(start_date) as earliest, MAX(end_date) as latest 
                FROM calendar
            """).fetchone()
            
            if result and result[0]:
                print(f"\nService period: {result[0]} to {result[1]}")
            
            # stop coverage
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
        """run validation checks on database"""
        print("\nVALIDATION CHECKS")
        print("=" * 50)
        
        checks = []
        
        try:
            # orphaned trips - trips without valid routes
            result = self.processor.conn.execute("""
                SELECT COUNT(*) FROM trips t 
                LEFT JOIN routes r ON t.route_id = r.route_id 
                WHERE r.route_id IS NULL
            """).fetchone()
            checks.append(("Orphaned trips (no matching route)", result[0]))
            
            # orphaned stop_times - stop_times without valid trips
            result = self.processor.conn.execute("""
                SELECT COUNT(*) FROM stop_times st 
                LEFT JOIN trips t ON st.trip_id = t.trip_id 
                WHERE t.trip_id IS NULL
            """).fetchone()
            checks.append(("Orphaned stop times (no matching trip)", result[0]))
            
            # stops with invalid coordinates
            result = self.processor.conn.execute("""
                SELECT COUNT(*) FROM stops 
                WHERE stop_lat NOT BETWEEN -90 AND 90 
                   OR stop_lon NOT BETWEEN -180 AND 180
            """).fetchone()
            checks.append(("Stops with invalid coordinates", result[0]))
            
            # routes without trips
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
        """run a custom sql query"""
        try:
            result = self.processor.conn.execute(query).fetchall()
            return result
        except Exception as e:
            print(f"Query error: {e}")
            return None
    
    def close(self):
        """close database connection"""
        try:
            self.processor.close()
        except Exception as e:
            print(f"Warning: error while closing processor: {e}")


def query_menu(manager: GTFSManager):
    """query submenu with pre-made and custom queries"""
    
    import json
    
    # pre-made queries with column names
    queries = {
        'longest_delays': {
            'name': 'vehicles with longest delays',
            'columns': ['entity_id', 'vehicle_id', 'trip_id', 'route', 'stop_id', 'stop_name', 'distance_m', 'speed_kmh', 'scheduled_arrival', 'eta_minutes'],
            'query': """
            WITH vehicle_eta AS (
                SELECT 
                    vp.entity_id,
                    vp.vehicle_id,
                    vp.trip_id,
                    r.route_short_name,
                    st.stop_id,
                    s.stop_name,
                    ROUND(CASE 
                        WHEN vp.speed > 0 THEN 
                            (6371000 * 2 * ASIN(SQRT(
                                POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                            )))
                        ELSE 0
                    END, 0) AS distance_m,
                    vp.speed * 3.6 AS speed_kmh,
                    st.arrival_time AS scheduled_arrival,
                    ROUND((CASE 
                        WHEN vp.speed > 0 THEN 
                            (6371000 * 2 * ASIN(SQRT(
                                POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                            )))
                        ELSE 0
                    END / NULLIF(vp.speed, 0)) / 60, 1) AS eta_minutes,
                    ROW_NUMBER() OVER (PARTITION BY vp.entity_id ORDER BY st.stop_sequence ASC) AS rn
                FROM vehicle_positions vp
                JOIN trips t ON vp.trip_id = t.trip_id
                JOIN routes r ON vp.route_id = r.route_id
                JOIN stop_times st ON t.trip_id = st.trip_id 
                    AND st.stop_sequence > COALESCE(vp.current_stop_sequence, -1)
                JOIN stops s ON st.stop_id = s.stop_id
                WHERE vp.speed > 0 AND s.stop_id IS NOT NULL
            )
            SELECT entity_id, vehicle_id, trip_id, route_short_name, stop_id, stop_name, distance_m, speed_kmh, scheduled_arrival, eta_minutes
            FROM vehicle_eta
            WHERE rn = 1
            ORDER BY eta_minutes DESC
            LIMIT 10
            """
        },
        'shortest_delays': {
            'name': 'vehicles with shortest delays (on-time)',
            'columns': ['entity_id', 'vehicle_id', 'trip_id', 'route', 'stop_id', 'stop_name', 'distance_m', 'speed_kmh', 'scheduled_arrival', 'eta_minutes'],
            'query': """
            WITH vehicle_eta AS (
                SELECT 
                    vp.entity_id,
                    vp.vehicle_id,
                    vp.trip_id,
                    r.route_short_name,
                    st.stop_id,
                    s.stop_name,
                    ROUND(CASE 
                        WHEN vp.speed > 0 THEN 
                            (6371000 * 2 * ASIN(SQRT(
                                POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                            )))
                        ELSE 0
                    END, 0) AS distance_m,
                    vp.speed * 3.6 AS speed_kmh,
                    st.arrival_time AS scheduled_arrival,
                    ROUND((CASE 
                        WHEN vp.speed > 0 THEN 
                            (6371000 * 2 * ASIN(SQRT(
                                POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                            )))
                        ELSE 0
                    END / NULLIF(vp.speed, 0)) / 60, 1) AS eta_minutes,
                    ROW_NUMBER() OVER (PARTITION BY vp.entity_id ORDER BY st.stop_sequence ASC) AS rn
                FROM vehicle_positions vp
                JOIN trips t ON vp.trip_id = t.trip_id
                JOIN routes r ON vp.route_id = r.route_id
                JOIN stop_times st ON t.trip_id = st.trip_id 
                    AND st.stop_sequence > COALESCE(vp.current_stop_sequence, -1)
                JOIN stops s ON st.stop_id = s.stop_id
                WHERE vp.speed > 0 AND s.stop_id IS NOT NULL
            )
            SELECT entity_id, vehicle_id, trip_id, route_short_name, stop_id, stop_name, distance_m, speed_kmh, scheduled_arrival, eta_minutes
            FROM vehicle_eta
            WHERE rn = 1
            ORDER BY eta_minutes ASC
            LIMIT 10
            """
        },
        'vehicle_status_summary': {
            'name': 'vehicle status summary - count by delay range',
            'columns': ['delay_category', 'min_minutes', 'max_minutes', 'vehicle_count'],
            'query': """
            WITH vehicle_eta AS (
                SELECT 
                    vp.entity_id,
                    vp.vehicle_id,
                    ROUND((CASE 
                        WHEN vp.speed > 0 THEN 
                            (6371000 * 2 * ASIN(SQRT(
                                POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                            )))
                        ELSE 0
                    END / NULLIF(vp.speed, 0)) / 60, 1) AS eta_minutes,
                    ROW_NUMBER() OVER (PARTITION BY vp.entity_id ORDER BY st.stop_sequence ASC) AS rn
                FROM vehicle_positions vp
                JOIN trips t ON vp.trip_id = t.trip_id
                JOIN routes r ON vp.route_id = r.route_id
                JOIN stop_times st ON t.trip_id = st.trip_id 
                    AND st.stop_sequence > COALESCE(vp.current_stop_sequence, -1)
                JOIN stops s ON st.stop_id = s.stop_id
                WHERE vp.speed > 0 AND s.stop_id IS NOT NULL
            ),
            next_stop_only AS (
                SELECT entity_id, vehicle_id, eta_minutes
                FROM vehicle_eta
                WHERE rn = 1
            ),
            categorized AS (
                SELECT 
                    CASE 
                        WHEN eta_minutes < 0 THEN 'Early'
                        WHEN eta_minutes BETWEEN 0 AND 5 THEN 'On-Time (0-5 min)'
                        WHEN eta_minutes BETWEEN 5 AND 15 THEN 'Slightly Late (5-15 min)'
                        WHEN eta_minutes BETWEEN 15 AND 30 THEN 'Late (15-30 min)'
                        ELSE 'Very Late (30+ min)'
                    END AS delay_category,
                    CASE 
                        WHEN eta_minutes < 0 THEN -999
                        WHEN eta_minutes BETWEEN 0 AND 5 THEN 0
                        WHEN eta_minutes BETWEEN 5 AND 15 THEN 5
                        WHEN eta_minutes BETWEEN 15 AND 30 THEN 15
                        ELSE 30
                    END AS min_minutes,
                    CASE 
                        WHEN eta_minutes < 0 THEN -1
                        WHEN eta_minutes BETWEEN 0 AND 5 THEN 5
                        WHEN eta_minutes BETWEEN 5 AND 15 THEN 15
                        WHEN eta_minutes BETWEEN 15 AND 30 THEN 30
                        ELSE 999
                    END AS max_minutes
                FROM next_stop_only
            )
            SELECT delay_category, min_minutes, max_minutes, COUNT(*) as vehicle_count
            FROM categorized
            GROUP BY delay_category, min_minutes, max_minutes
            ORDER BY min_minutes ASC
            """
        },
        'routes_by_delays': {
            'name': 'routes by average delay',
            'columns': ['route', 'route_name', 'vehicle_count', 'avg_delay_minutes', 'max_delay_minutes'],
            'query': """
            WITH vehicle_eta AS (
                SELECT 
                    r.route_short_name,
                    r.route_long_name,
                    vp.entity_id,
                    ROUND((CASE 
                        WHEN vp.speed > 0 THEN 
                            (6371000 * 2 * ASIN(SQRT(
                                POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                            )))
                        ELSE 0
                    END / NULLIF(vp.speed, 0)) / 60, 1) AS eta_minutes,
                    ROW_NUMBER() OVER (PARTITION BY vp.entity_id ORDER BY st.stop_sequence ASC) AS rn
                FROM vehicle_positions vp
                JOIN trips t ON vp.trip_id = t.trip_id
                JOIN routes r ON vp.route_id = r.route_id
                JOIN stop_times st ON t.trip_id = st.trip_id 
                    AND st.stop_sequence > COALESCE(vp.current_stop_sequence, -1)
                JOIN stops s ON st.stop_id = s.stop_id
                WHERE vp.speed > 0 AND s.stop_id IS NOT NULL
            )
            SELECT 
                route_short_name,
                route_long_name,
                COUNT(DISTINCT entity_id) as vehicle_count,
                ROUND(AVG(eta_minutes), 1) as avg_delay_minutes,
                ROUND(MAX(eta_minutes), 1) as max_delay_minutes
            FROM vehicle_eta
            WHERE rn = 1
            GROUP BY route_short_name, route_long_name
            ORDER BY avg_delay_minutes DESC
            LIMIT 15
            """
        }
    }
    
    try:
        while True:
            print("\n" + "="*50)
            print("QUERY MENU")
            print("="*50)
            print("Pre-made queries:")
            for i, (key, query_info) in enumerate(queries.items(), 1):
                print(f"{i}. {query_info['name']}")
            print(f"{len(queries) + 1}. Run custom SQL query")
            print(f"{len(queries) + 2}. Back to main menu")
            print("-" * 50)
            
            choice = input("Select query: ").strip()
            
            query_keys = list(queries.keys())
            if choice.isdigit():
                choice_num = int(choice)
                
                if 1 <= choice_num <= len(queries):
                    # run pre-made query
                    key = query_keys[choice_num - 1]
                    query_info = queries[key]
                    print(f"\nRunning: {query_info['name']}...")
                    
                    result = manager.run_custom_query(query_info['query'])
                    if result:
                        # convert to JSON
                        columns = query_info['columns']
                        json_data = [dict(zip(columns, row)) for row in result]
                        
                        print(f"\nResult ({len(result)} rows):")
                        print("-" * 120)
                        print(json.dumps(json_data, indent=2))
                    else:
                        print("No results or error occurred")
                
                elif choice_num == len(queries) + 1:
                    # custom query
                    print("\nEnter your SQL query (type 'EXIT' on a new line to finish):")
                    lines = []
                    while True:
                        line = input()
                        if line.upper() == 'EXIT':
                            break
                        lines.append(line)
                    
                    if lines:
                        custom_query = '\n'.join(lines)
                        print(f"\nRunning custom query...")
                        result = manager.run_custom_query(custom_query)
                        if result:
                            # for custom queries, we don't have column names, so just show raw
                            print(f"\nResult ({len(result)} rows):")
                            print("-" * 120)
                            for i, row in enumerate(result[:20], 1):
                                print(f"{i}. {row}")
                            if len(result) > 20:
                                print(f"... and {len(result) - 20} more rows")
                        else:
                            print("No results or error occurred")
                
                elif choice_num == len(queries) + 2:
                    break
                else:
                    print("Invalid selection")
            else:
                print("Invalid input")
    
    except KeyboardInterrupt:
        print("\nReturning to main menu...")


def main():
    """manual gtfs manager interface"""
    
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
            print("6. Realtime menu (vehicle positions)")
            print("7. Exit")
            print("-" * 60)
            
            choice = input("Select option (1-7): ").strip()
            
            if choice == "1":
                folder = input("GTFS folder path (or press Enter to search current directory): ").strip()
                if not folder:
                    folder = "."
                
                gtfs_path = Path(folder)
                if not gtfs_path.exists():
                    print(f"Folder not found: {folder}")
                    continue
                
                # look for .txt files
                txt_files = list(gtfs_path.glob("*.txt")) + list(gtfs_path.glob("**/*.txt"))
                if not txt_files:
                    print(f"No GTFS .txt files found in {folder}")
                    continue
                
                print(f"Found {len(txt_files)} GTFS files in {folder}")

                # If the discovered .txt files are inside a subdirectory,
                # load from that directory instead of the user-supplied root.
                try:
                    parents = list({str(Path(f).parent) for f in txt_files})
                    # prefer a parent directory that contains several expected GTFS files
                    expected = [
                        'agency.txt', 'routes.txt', 'stops.txt', 'trips.txt', 'stop_times.txt',
                        'calendar.txt', 'calendar_dates.txt', 'shapes.txt'
                    ]
                    best_parent = None
                    best_count = -1
                    for p in parents:
                        try:
                            cnt = sum(1 for ef in expected if Path(p, ef).exists())
                        except Exception:
                            cnt = 0
                        if cnt > best_count:
                            best_count = cnt
                            best_parent = p

                    if best_parent and best_count > 0:
                        load_folder = best_parent
                        print(f"Using GTFS folder: {load_folder} (detected {best_count} core files)")
                    else:
                        # fallback to folder if it itself contains txt files, otherwise first parent
                        if any(str(Path(folder)) == p or Path(folder).resolve() == Path(p).resolve() for p in parents):
                            load_folder = folder
                        else:
                            load_folder = parents[0]
                            print(f"Multiple GTFS locations found; using: {load_folder}")

                    if os.path.exists(load_folder):
                        # require explicit confirmation for destructive full reload
                        confirm = input("Type \"I am sure I want to reload all data\" to confirm destructive reload: ").strip()
                        if confirm != "I am sure I want to reload all data":
                            print("Reload aborted by user.")
                            continue
                        manager.initial_load(load_folder)
                    else:
                        print(f"Folder not found: {load_folder}")
                except Exception as e:
                    print(f"Error determining GTFS folder: {e}")
            
            elif choice == "2":
                folder = input("GTFS folder path (or press Enter to search current directory): ").strip()
                if not folder:
                    folder = "."
                
                gtfs_path = Path(folder)
                if not gtfs_path.exists():
                    print(f"Folder not found: {folder}")
                    continue
                
                # look for .txt files
                txt_files = list(gtfs_path.glob("*.txt")) + list(gtfs_path.glob("**/*.txt"))
                if not txt_files:
                    print(f"No GTFS .txt files found in {folder}")
                    continue
                
                print(f"Found {len(txt_files)} GTFS files in {folder}")

                # Determine the folder containing the GTFS .txt files and use it for update
                try:
                    parents = list({str(Path(f).parent) for f in txt_files})
                    expected = [
                        'agency.txt', 'routes.txt', 'stops.txt', 'trips.txt', 'stop_times.txt',
                        'calendar.txt', 'calendar_dates.txt', 'shapes.txt'
                    ]
                    best_parent = None
                    best_count = -1
                    for p in parents:
                        try:
                            cnt = sum(1 for ef in expected if Path(p, ef).exists())
                        except Exception:
                            cnt = 0
                        if cnt > best_count:
                            best_count = cnt
                            best_parent = p

                    if best_parent and best_count > 0:
                        load_folder = best_parent
                        print(f"Using GTFS folder: {load_folder} (detected {best_count} core files)")
                    else:
                        if any(str(Path(folder)) == p or Path(folder).resolve() == Path(p).resolve() for p in parents):
                            load_folder = folder
                        else:
                            load_folder = parents[0]
                            print(f"Multiple GTFS locations found; using: {load_folder}")

                    if os.path.exists(load_folder):
                        # require explicit confirmation for destructive full update
                        confirm = input("Type \"I am sure I want to reload all data\" to confirm destructive reload: ").strip()
                        if confirm != "I am sure I want to reload all data":
                            print("Update aborted by user.")
                            continue
                        manager.update_data(load_folder)
                    else:
                        print(f"Folder not found: {load_folder}")
                except Exception as e:
                    print(f"Error determining GTFS folder: {e}")
            
            elif choice == "3":
                manager.show_stats()
            
            elif choice == "4":
                manager.validate_database()
            
            elif choice == "5":
                query_menu(manager)
            elif choice == "6":
                # realtime submenu
                realtime_menu(manager)
            
            elif choice == "7":
                print("Exiting...")
                sys.stdout.flush()
                break
            
            else:
                print("Invalid option. Please select 1-6.")
    
    except KeyboardInterrupt:
        print("\n\nProcess interrupted. Exiting...")
    
    finally:
        try:
            manager.close()
        except Exception as e:
            print(f"Error while closing manager: {e}")
        # ensure process terminates cleanly
        try:
            sys.exit(0)
        except SystemExit:
            # allow normal exit
            pass


def realtime_menu(manager: GTFSManager):
    """submenu for vehicle positions"""
    import json
    from pathlib import Path
    
    try:
        while True:
            print("\n" + "="*40)
            print("REALTIME (vehicle positions) MENU")
            print("="*40)
            print("1. Initialize/create realtime vehicle_positions table")
            print("2. Ingest vehiclepositions.pb and push to MotherDuck")
            print("3. Estimate ETA and delay for a vehicle")
            print("4. Show recent vehicle positions")
            print("5. Vehicles with longest delays")
            print("6. Vehicles with shortest delays (on-time)")
            print("7. Vehicle status summary - count by delay range")
            print("8. Routes by average delay")
            print("9. Back to main menu")
            choice = input("Select option (1-9): ").strip()

            if choice == '1':
                manager.processor.create_realtime_table()

            elif choice == '2':
                # Download latest vehiclepositions.pb from API and ingest
                import requests
                
                feed_url = "https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/vehicle/vehiclepositions.pb"
                pb_file = Path("GTFS") / "vehiclepositions.pb"
                
                try:
                    # Create GTFS folder if it doesn't exist
                    pb_file.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Download latest feed
                    print(f"Downloading latest vehicle positions from API...")
                    response = requests.get(feed_url, timeout=30)
                    response.raise_for_status()
                    
                    # Save to file (overwrite if exists)
                    with open(pb_file, 'wb') as f:
                        f.write(response.content)
                    print(f"Saved to {pb_file}")
                    
                    # Ingest the downloaded data
                    manager.processor.create_realtime_table()
                    records = manager.processor.parse_vehiclepositions_bytes(response.content)
                    if not records:
                        print("No records parsed from feed")
                        continue
                    manager.processor.upsert_vehicle_positions(records)
                    print(f"Ingested {len(records)} vehicle positions and pushed to MotherDuck")
                except requests.exceptions.RequestException as e:
                    print(f"Error downloading feed: {e}")
                except Exception as e:
                    print(f"Error ingesting vehiclepositions: {e}")

            elif choice == '3':
                ident = input("Enter entity_id or vehicle_id (prefix with 'v:' for vehicle_id, or press Enter to abort): ").strip()
                if not ident:
                    continue
                if ident.startswith('v:'):
                    vehicle_id = ident[2:]
                    entity_id = None
                else:
                    # assume entity_id
                    entity_id = ident
                    vehicle_id = None
                result = manager.processor.estimate_eta_for_vehicle(entity_id=entity_id, vehicle_id=vehicle_id)
                if not result:
                    print("No ETA estimate available for that vehicle.")
                else:
                    print("ETA / Delay estimate:")
                    for k, v in result.items():
                        print(f"  {k}: {v}")

            elif choice == '4':
                try:
                    n = int(input("Number of recent rows to show (default 20): ").strip() or 20)
                except Exception:
                    n = 20
                rows = manager.processor.get_recent_vehicle_positions(limit=n)
                if not rows:
                    print("No realtime data found. Initialize and ingest first.")
                    continue
                print(f"Showing {len(rows)} recent vehicle position rows:")
                for r in rows:
                    print(r)
            
            elif choice == '5':
                # longest delays
                query = """
                WITH vehicle_eta AS (
                    SELECT 
                        vp.entity_id,
                        vp.vehicle_id,
                        vp.trip_id,
                        r.route_short_name,
                        st.stop_id,
                        s.stop_name,
                        ROUND(CASE 
                            WHEN vp.speed > 0 THEN 
                                (6371000 * 2 * ASIN(SQRT(
                                    POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                    COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                    POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                                )))
                            ELSE 0
                        END, 0) AS distance_m,
                        vp.speed * 3.6 AS speed_kmh,
                        st.arrival_time AS scheduled_arrival,
                        ROUND((CASE 
                            WHEN vp.speed > 0 THEN 
                                (6371000 * 2 * ASIN(SQRT(
                                    POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                    COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                    POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                                )))
                            ELSE 0
                        END / NULLIF(vp.speed, 0)) / 60, 1) AS eta_minutes,
                        ROW_NUMBER() OVER (PARTITION BY vp.entity_id ORDER BY st.stop_sequence ASC) AS rn
                    FROM vehicle_positions vp
                    JOIN trips t ON vp.trip_id = t.trip_id
                    JOIN routes r ON vp.route_id = r.route_id
                    JOIN stop_times st ON t.trip_id = st.trip_id 
                        AND st.stop_sequence > COALESCE(vp.current_stop_sequence, -1)
                    JOIN stops s ON st.stop_id = s.stop_id
                    WHERE vp.speed > 0 AND s.stop_id IS NOT NULL
                )
                SELECT entity_id, vehicle_id, trip_id, route_short_name, stop_id, stop_name, distance_m, speed_kmh, scheduled_arrival, eta_minutes
                FROM vehicle_eta
                WHERE rn = 1
                ORDER BY eta_minutes DESC
                LIMIT 10
                """
                result = manager.run_custom_query(query)
                if result:
                    columns = ['entity_id', 'vehicle_id', 'trip_id', 'route', 'stop_id', 'stop_name', 'distance_m', 'speed_kmh', 'scheduled_arrival', 'eta_minutes']
                    json_data = [dict(zip(columns, row)) for row in result]
                    print(f"\nVehicles with longest delays ({len(result)} rows):")
                    print("-" * 120)
                    print(json.dumps(json_data, indent=2))
                else:
                    print("No results or error occurred")
            
            elif choice == '6':
                # shortest delays
                query = """
                WITH vehicle_eta AS (
                    SELECT 
                        vp.entity_id,
                        vp.vehicle_id,
                        vp.trip_id,
                        r.route_short_name,
                        st.stop_id,
                        s.stop_name,
                        ROUND(CASE 
                            WHEN vp.speed > 0 THEN 
                                (6371000 * 2 * ASIN(SQRT(
                                    POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                    COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                    POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                                )))
                            ELSE 0
                        END, 0) AS distance_m,
                        vp.speed * 3.6 AS speed_kmh,
                        st.arrival_time AS scheduled_arrival,
                        ROUND((CASE 
                            WHEN vp.speed > 0 THEN 
                                (6371000 * 2 * ASIN(SQRT(
                                    POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                    COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                    POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                                )))
                            ELSE 0
                        END / NULLIF(vp.speed, 0)) / 60, 1) AS eta_minutes,
                        ROW_NUMBER() OVER (PARTITION BY vp.entity_id ORDER BY st.stop_sequence ASC) AS rn
                    FROM vehicle_positions vp
                    JOIN trips t ON vp.trip_id = t.trip_id
                    JOIN routes r ON vp.route_id = r.route_id
                    JOIN stop_times st ON t.trip_id = st.trip_id 
                        AND st.stop_sequence > COALESCE(vp.current_stop_sequence, -1)
                    JOIN stops s ON st.stop_id = s.stop_id
                    WHERE vp.speed > 0 AND s.stop_id IS NOT NULL
                )
                SELECT entity_id, vehicle_id, trip_id, route_short_name, stop_id, stop_name, distance_m, speed_kmh, scheduled_arrival, eta_minutes
                FROM vehicle_eta
                WHERE rn = 1
                ORDER BY eta_minutes ASC
                LIMIT 10
                """
                result = manager.run_custom_query(query)
                if result:
                    columns = ['entity_id', 'vehicle_id', 'trip_id', 'route', 'stop_id', 'stop_name', 'distance_m', 'speed_kmh', 'scheduled_arrival', 'eta_minutes']
                    json_data = [dict(zip(columns, row)) for row in result]
                    print(f"\nVehicles with shortest delays ({len(result)} rows):")
                    print("-" * 120)
                    print(json.dumps(json_data, indent=2))
                else:
                    print("No results or error occurred")
            
            elif choice == '7':
                # vehicle status summary
                query = """
                WITH vehicle_eta AS (
                    SELECT 
                        vp.entity_id,
                        vp.vehicle_id,
                        ROUND((CASE 
                            WHEN vp.speed > 0 THEN 
                                (6371000 * 2 * ASIN(SQRT(
                                    POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                    COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                    POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                                )))
                            ELSE 0
                        END / NULLIF(vp.speed, 0)) / 60, 1) AS eta_minutes,
                        ROW_NUMBER() OVER (PARTITION BY vp.entity_id ORDER BY st.stop_sequence ASC) AS rn
                    FROM vehicle_positions vp
                    JOIN trips t ON vp.trip_id = t.trip_id
                    JOIN routes r ON vp.route_id = r.route_id
                    JOIN stop_times st ON t.trip_id = st.trip_id 
                        AND st.stop_sequence > COALESCE(vp.current_stop_sequence, -1)
                    JOIN stops s ON st.stop_id = s.stop_id
                    WHERE vp.speed > 0 AND s.stop_id IS NOT NULL
                ),
                next_stop_only AS (
                    SELECT entity_id, vehicle_id, eta_minutes
                    FROM vehicle_eta
                    WHERE rn = 1
                ),
                categorized AS (
                    SELECT 
                        CASE 
                            WHEN eta_minutes < 0 THEN 'Early'
                            WHEN eta_minutes BETWEEN 0 AND 5 THEN 'On-Time (0-5 min)'
                            WHEN eta_minutes BETWEEN 5 AND 15 THEN 'Slightly Late (5-15 min)'
                            WHEN eta_minutes BETWEEN 15 AND 30 THEN 'Late (15-30 min)'
                            ELSE 'Very Late (30+ min)'
                        END AS delay_category,
                        CASE 
                            WHEN eta_minutes < 0 THEN -999
                            WHEN eta_minutes BETWEEN 0 AND 5 THEN 0
                            WHEN eta_minutes BETWEEN 5 AND 15 THEN 5
                            WHEN eta_minutes BETWEEN 15 AND 30 THEN 15
                            ELSE 30
                        END AS min_minutes,
                        CASE 
                            WHEN eta_minutes < 0 THEN -1
                            WHEN eta_minutes BETWEEN 0 AND 5 THEN 5
                            WHEN eta_minutes BETWEEN 5 AND 15 THEN 15
                            WHEN eta_minutes BETWEEN 15 AND 30 THEN 30
                            ELSE 999
                        END AS max_minutes
                    FROM next_stop_only
                )
                SELECT delay_category, min_minutes, max_minutes, COUNT(*) as vehicle_count
                FROM categorized
                GROUP BY delay_category, min_minutes, max_minutes
                ORDER BY min_minutes ASC
                """
                result = manager.run_custom_query(query)
                if result:
                    columns = ['delay_category', 'min_minutes', 'max_minutes', 'vehicle_count']
                    json_data = [dict(zip(columns, row)) for row in result]
                    print(f"\nVehicle status summary ({len(result)} rows):")
                    print("-" * 120)
                    print(json.dumps(json_data, indent=2))
                else:
                    print("No results or error occurred")
            
            elif choice == '8':
                # routes by average delay
                query = """
                WITH vehicle_eta AS (
                    SELECT 
                        r.route_short_name,
                        r.route_long_name,
                        vp.entity_id,
                        ROUND((CASE 
                            WHEN vp.speed > 0 THEN 
                                (6371000 * 2 * ASIN(SQRT(
                                    POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                    COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                    POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                                )))
                            ELSE 0
                        END / NULLIF(vp.speed, 0)) / 60, 1) AS eta_minutes,
                        ROW_NUMBER() OVER (PARTITION BY vp.entity_id ORDER BY st.stop_sequence ASC) AS rn
                    FROM vehicle_positions vp
                    JOIN trips t ON vp.trip_id = t.trip_id
                    JOIN routes r ON vp.route_id = r.route_id
                    JOIN stop_times st ON t.trip_id = st.trip_id 
                        AND st.stop_sequence > COALESCE(vp.current_stop_sequence, -1)
                    JOIN stops s ON st.stop_id = s.stop_id
                    WHERE vp.speed > 0 AND s.stop_id IS NOT NULL
                )
                SELECT 
                    route_short_name,
                    route_long_name,
                    COUNT(DISTINCT entity_id) as vehicle_count,
                    ROUND(AVG(eta_minutes), 1) as avg_delay_minutes,
                    ROUND(MAX(eta_minutes), 1) as max_delay_minutes
                FROM vehicle_eta
                WHERE rn = 1
                GROUP BY route_short_name, route_long_name
                ORDER BY avg_delay_minutes DESC
                LIMIT 15
                """
                result = manager.run_custom_query(query)
                if result:
                    columns = ['route', 'route_name', 'vehicle_count', 'avg_delay_minutes', 'max_delay_minutes']
                    json_data = [dict(zip(columns, row)) for row in result]
                    print(f"\nRoutes by average delay ({len(result)} rows):")
                    print("-" * 120)
                    print(json.dumps(json_data, indent=2))
                else:
                    print("No results or error occurred")
            
            elif choice == '9':
                # Back to main menu
                return
            
            else:
                print("Invalid option. Please select 1-9.")

    except KeyboardInterrupt:
        print("\nReturning to main menu...")


if __name__ == "__main__":
    main()