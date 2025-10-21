"""
GTFS Data Processor for DuckDB
A comprehensive system for loading, validating, and cleaning GTFS data into DuckDB.
"""

import duckdb
import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List, Optional, Tuple, Any
import re

class GTFSProcessor:
    """Main class for processing GTFS data into DuckDB with validation and cleaning."""
    
    def __init__(self, db_path: str = "gtfs_database.db"):
        """Initialize the GTFS processor with database connection."""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.setup_logging()
        self.validation_errors = []
        self.cleaning_stats = {}
        
        # GTFS file specifications
        self.gtfs_files = {
            'agency': {
                'required_fields': ['agency_id', 'agency_name', 'agency_url', 'agency_timezone'],
                'optional_fields': ['agency_lang', 'agency_phone', 'agency_fare_url', 'agency_email']
            },
            'routes': {
                'required_fields': ['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type'],
                'optional_fields': ['route_desc', 'route_url', 'route_color', 'route_text_color', 'route_sort_order']
            },
            'stops': {
                'required_fields': ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'],
                'optional_fields': ['stop_code', 'stop_desc', 'zone_id', 'stop_url', 'location_type', 
                                  'parent_station', 'stop_timezone', 'wheelchair_boarding', 'level_id', 'platform_code']
            },
            'trips': {
                'required_fields': ['route_id', 'service_id', 'trip_id'],
                'optional_fields': ['trip_headsign', 'trip_short_name', 'direction_id', 'block_id', 'shape_id', 'wheelchair_accessible', 'bikes_allowed']
            },
            'stop_times': {
                'required_fields': ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence'],
                'optional_fields': ['stop_headsign', 'pickup_type', 'drop_off_type', 'shape_dist_traveled', 'timepoint']
            },
            'calendar': {
                'required_fields': ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date'],
                'optional_fields': []
            },
            'calendar_dates': {
                'required_fields': ['service_id', 'date', 'exception_type'],
                'optional_fields': []
            },
            'shapes': {
                'required_fields': ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence'],
                'optional_fields': ['shape_dist_traveled']
            }
        }
    
    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('gtfs_processor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def create_tables(self):
        """Create DuckDB tables with proper schema and constraints."""
        self.logger.info("Creating database tables...")
        
        # Drop tables if they exist to start fresh
        tables_to_drop = ['stop_times', 'trips', 'calendar_dates', 'shapes', 'routes', 'stops', 'calendar', 'agency']
        for table in tables_to_drop:
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")
            except:
                pass
        
        # Agency table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS agency (
                agency_id VARCHAR PRIMARY KEY,
                agency_name VARCHAR NOT NULL,
                agency_url VARCHAR NOT NULL,
                agency_timezone VARCHAR NOT NULL,
                agency_lang VARCHAR,
                agency_phone VARCHAR,
                agency_fare_url VARCHAR,
                agency_email VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Routes table (without foreign key constraint initially)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS routes (
                route_id VARCHAR PRIMARY KEY,
                agency_id VARCHAR NOT NULL,
                route_short_name VARCHAR,
                route_long_name VARCHAR,
                route_desc VARCHAR,
                route_type INTEGER NOT NULL,
                route_url VARCHAR,
                route_color VARCHAR,
                route_text_color VARCHAR,
                route_sort_order INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Stops table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stops (
                stop_id VARCHAR PRIMARY KEY,
                stop_code VARCHAR,
                stop_name VARCHAR NOT NULL,
                stop_desc VARCHAR,
                stop_lat DOUBLE NOT NULL,
                stop_lon DOUBLE NOT NULL,
                zone_id VARCHAR,
                stop_url VARCHAR,
                location_type INTEGER DEFAULT 0,
                parent_station VARCHAR,
                stop_timezone VARCHAR,
                wheelchair_boarding INTEGER,
                level_id VARCHAR,
                platform_code VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Calendar table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS calendar (
                service_id VARCHAR PRIMARY KEY,
                monday INTEGER NOT NULL,
                tuesday INTEGER NOT NULL,
                wednesday INTEGER NOT NULL,
                thursday INTEGER NOT NULL,
                friday INTEGER NOT NULL,
                saturday INTEGER NOT NULL,
                sunday INTEGER NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Calendar dates table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS calendar_dates (
                service_id VARCHAR NOT NULL,
                date DATE NOT NULL,
                exception_type INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (service_id, date)
            )
        """)
        
        # Trips table (without foreign key constraints initially)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trips (
                trip_id VARCHAR PRIMARY KEY,
                route_id VARCHAR NOT NULL,
                service_id VARCHAR NOT NULL,
                trip_headsign VARCHAR,
                trip_short_name VARCHAR,
                direction_id INTEGER,
                block_id VARCHAR,
                shape_id VARCHAR,
                wheelchair_accessible INTEGER,
                bikes_allowed INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Stop times table (without foreign key constraints initially)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stop_times (
                trip_id VARCHAR NOT NULL,
                arrival_time VARCHAR,
                departure_time VARCHAR,
                stop_id VARCHAR NOT NULL,
                stop_sequence INTEGER NOT NULL,
                stop_headsign VARCHAR,
                pickup_type INTEGER DEFAULT 0,
                drop_off_type INTEGER DEFAULT 0,
                shape_dist_traveled DOUBLE,
                timepoint INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (trip_id, stop_sequence)
            )
        """)
        
        # Shapes table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS shapes (
                shape_id VARCHAR NOT NULL,
                shape_pt_lat DOUBLE NOT NULL,
                shape_pt_lon DOUBLE NOT NULL,
                shape_pt_sequence INTEGER NOT NULL,
                shape_dist_traveled DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (shape_id, shape_pt_sequence)
            )
        """)
        
        self.logger.info("Database tables created successfully")
    
    def validate_gtfs_file(self, file_path: str, file_type: str) -> Tuple[pd.DataFrame, List[str]]:
        """Validate a GTFS file and return cleaned data with validation errors."""
        if not os.path.exists(file_path):
            return None, [f"File {file_path} does not exist"]
        
        errors = []
        
        try:
            # Read the file
            df = pd.read_csv(file_path)
            original_rows = len(df)
            self.logger.info(f"Began loading {file_type}.txt with {original_rows} rows")
            
            # Get file specifications
            file_spec = self.gtfs_files.get(file_type, {})
            required_fields = file_spec.get('required_fields', [])
            
            # Check required fields
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                errors.append(f"Missing required fields in {file_type}: {missing_fields}")
                return None, errors
            
            # Clean and validate data based on file type
            df, file_errors = self.clean_file_data(df, file_type)
            errors.extend(file_errors)
            
            cleaned_rows = len(df)
            self.cleaning_stats[file_type] = {
                'original_rows': original_rows,
                'cleaned_rows': cleaned_rows,
                'rows_removed': original_rows - cleaned_rows
            }
            
            return df, errors
            
        except Exception as e:
            errors.append(f"Error reading {file_type}.txt: {str(e)}")
            return None, errors
    
    def clean_file_data(self, df: pd.DataFrame, file_type: str) -> Tuple[pd.DataFrame, List[str]]:
        """Clean and validate data for specific GTFS file type."""
        errors = []
        
        if file_type == 'agency':
            df, file_errors = self.clean_agency_data(df)
        elif file_type == 'routes':
            df, file_errors = self.clean_routes_data(df)
        elif file_type == 'stops':
            df, file_errors = self.clean_stops_data(df)
        elif file_type == 'trips':
            df, file_errors = self.clean_trips_data(df)
        elif file_type == 'stop_times':
            df, file_errors = self.clean_stop_times_data(df)
        elif file_type == 'calendar':
            df, file_errors = self.clean_calendar_data(df)
        elif file_type == 'calendar_dates':
            df, file_errors = self.clean_calendar_dates_data(df)
        elif file_type == 'shapes':
            df, file_errors = self.clean_shapes_data(df)
        else:
            file_errors = []
        
        errors.extend(file_errors)
        return df, errors
    
    def clean_agency_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Clean agency data."""
        errors = []
        initial_count = len(df)
        
        # Remove rows with missing required fields
        df = df.dropna(subset=['agency_id', 'agency_name', 'agency_url', 'agency_timezone'])
        
        # Validate URLs
        url_pattern = re.compile(r'^https?://.+')
        df = df[df['agency_url'].str.match(url_pattern, na=False)]
        
        # Clean text fields
        df['agency_name'] = df['agency_name'].str.strip()
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid agency rows")
        
        return df, errors
    
    def clean_routes_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Clean routes data."""
        errors = []
        initial_count = len(df)
        
        # Remove rows with missing required fields (but be more lenient)
        df = df.dropna(subset=['route_id'])  # Only require route_id
        
        # Fill missing agency_id with default
        if 'agency_id' in df.columns:
            df['agency_id'] = df['agency_id'].fillna('1')  # Default agency_id
        else:
            df['agency_id'] = '1'
        
        # Validate route type (must be integer), default to 3 (bus) if missing
        if 'route_type' in df.columns:
            df['route_type'] = pd.to_numeric(df['route_type'], errors='coerce')
            df['route_type'] = df['route_type'].fillna(3)  # Default to bus
        else:
            df['route_type'] = 3
        
        df['route_type'] = df['route_type'].astype(int)
        
        # Convert ID fields to strings
        if 'route_id' in df.columns:
            df['route_id'] = df['route_id'].astype(str)
        
        # Validate colors (6-digit hex without #) - clean invalid ones instead of removing rows
        if 'route_color' in df.columns:
            # Convert to string first, then validate
            df['route_color'] = df['route_color'].astype(str).replace('nan', '')
            color_pattern = re.compile(r'^[0-9A-Fa-f]{6}$')
            invalid_colors = ~df['route_color'].str.match(color_pattern, na=False) | (df['route_color'] == '')
            df.loc[invalid_colors, 'route_color'] = None
        
        if 'route_text_color' in df.columns:
            # Convert to string first, handle integers that represent colors
            df['route_text_color'] = df['route_text_color'].astype(str).replace('nan', '')
            # Convert integer 0 to proper hex format
            df.loc[df['route_text_color'] == '0', 'route_text_color'] = '000000'
            color_pattern = re.compile(r'^[0-9A-Fa-f]{6}$')
            invalid_colors = ~df['route_text_color'].str.match(color_pattern, na=False) | (df['route_text_color'] == '')
            df.loc[invalid_colors, 'route_text_color'] = None
        
        # Ensure route names exist
        if 'route_short_name' not in df.columns:
            df['route_short_name'] = df['route_id']
        else:
            df['route_short_name'] = df['route_short_name'].fillna(df['route_id'])
            
        if 'route_long_name' not in df.columns:
            df['route_long_name'] = df['route_short_name']
        else:
            df['route_long_name'] = df['route_long_name'].fillna(df['route_short_name'])
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid route rows")
        
        return df, errors
    
    def clean_stops_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Clean stops data."""
        errors = []
        initial_count = len(df)
        
        # Remove rows with missing required fields
        df = df.dropna(subset=['stop_id', 'stop_name', 'stop_lat', 'stop_lon'])
        
        # Validate coordinates
        df = df[(df['stop_lat'] >= -90) & (df['stop_lat'] <= 90)]
        df = df[(df['stop_lon'] >= -180) & (df['stop_lon'] <= 180)]
        
        # Clean stop names
        df['stop_name'] = df['stop_name'].str.strip()
        
        # Handle numeric columns that might have NaN values
        numeric_columns = ['wheelchair_boarding', 'location_type', 'zone_id']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Validate wheelchair boarding values (0, 1, 2)
        if 'wheelchair_boarding' in df.columns:
            df.loc[~df['wheelchair_boarding'].isin([0, 1, 2]), 'wheelchair_boarding'] = None
        
        # Set default location_type to 0 (stop) if NaN
        if 'location_type' in df.columns:
            df['location_type'] = df['location_type'].fillna(0)
        
        # Convert stop_id and stop_code to string to avoid integer conversion issues
        if 'stop_id' in df.columns:
            df['stop_id'] = df['stop_id'].astype(str)
        if 'stop_code' in df.columns:
            df['stop_code'] = df['stop_code'].astype(str)
            df.loc[df['stop_code'] == 'nan', 'stop_code'] = None
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid stop rows")
        
        return df, errors
    
    def clean_trips_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Clean trips data."""
        errors = []
        initial_count = len(df)
        
        # Remove rows with missing required fields
        df = df.dropna(subset=['trip_id', 'route_id', 'service_id'])
        
        # Convert ID fields to strings
        for col in ['trip_id', 'route_id', 'service_id', 'block_id', 'shape_id']:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df.loc[df[col] == 'nan', col] = None
        
        # Validate direction_id (0 or 1)
        if 'direction_id' in df.columns:
            df['direction_id'] = pd.to_numeric(df['direction_id'], errors='coerce')
            df.loc[~df['direction_id'].isin([0, 1]), 'direction_id'] = None
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid trip rows")
        
        return df, errors
    
    def clean_stop_times_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Clean stop times data."""
        errors = []
        initial_count = len(df)
        
        # Remove rows with missing required fields
        df = df.dropna(subset=['trip_id', 'stop_id', 'stop_sequence'])
        
        # Clean time format (HH:MM:SS, can be > 24:00:00)
        time_pattern = re.compile(r'^\d{1,2}:\d{2}:\d{2}$')
        
        if 'arrival_time' in df.columns:
            df = df[df['arrival_time'].str.match(time_pattern, na=False) | df['arrival_time'].isna()]
        
        if 'departure_time' in df.columns:
            df = df[df['departure_time'].str.match(time_pattern, na=False) | df['departure_time'].isna()]
        
        # Validate stop_sequence (must be non-negative integer)
        df = df[df['stop_sequence'] >= 0]
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid stop time rows")
        
        return df, errors
    
    def clean_calendar_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Clean calendar data."""
        errors = []
        initial_count = len(df)
        
        # Remove rows with missing required fields
        required_cols = ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date']
        df = df.dropna(subset=required_cols)
        
        # Validate day columns (0 or 1)
        day_cols = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        for col in day_cols:
            df = df[df[col].isin([0, 1])]
        
        # Validate date format (YYYYMMDD)
        date_pattern = re.compile(r'^\d{8}$')
        df = df[df['start_date'].astype(str).str.match(date_pattern)]
        df = df[df['end_date'].astype(str).str.match(date_pattern)]
        
        # Convert dates to proper format
        df['start_date'] = pd.to_datetime(df['start_date'].astype(str), format='%Y%m%d')
        df['end_date'] = pd.to_datetime(df['end_date'].astype(str), format='%Y%m%d')
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid calendar rows")
        
        return df, errors
    
    def clean_calendar_dates_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Clean calendar dates data."""
        errors = []
        initial_count = len(df)
        
        # Remove rows with missing required fields
        df = df.dropna(subset=['service_id', 'date', 'exception_type'])
        
        # Validate exception type (1 or 2)
        df = df[df['exception_type'].isin([1, 2])]
        
        # Validate date format (YYYYMMDD)
        date_pattern = re.compile(r'^\d{8}$')
        df = df[df['date'].astype(str).str.match(date_pattern)]
        
        # Convert date to proper format
        df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d')
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid calendar date rows")
        
        return df, errors
    
    def clean_shapes_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Clean shapes data."""
        errors = []
        initial_count = len(df)
        
        # Remove rows with missing required fields
        df = df.dropna(subset=['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence'])
        
        # Validate coordinates
        df = df[(df['shape_pt_lat'] >= -90) & (df['shape_pt_lat'] <= 90)]
        df = df[(df['shape_pt_lon'] >= -180) & (df['shape_pt_lon'] <= 180)]
        
        # Validate sequence (non-negative integer)
        df = df[df['shape_pt_sequence'] >= 0]
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid shape rows")
        
        return df, errors
    
    def load_gtfs_data(self, gtfs_folder: str):
        """Load and process all GTFS files from a folder."""
        self.logger.info(f"Loading GTFS data from {gtfs_folder}")
        self.validation_errors = []
        self.cleaning_stats = {}
        
        # Create tables
        self.create_tables()
        
        # Process files in dependency order
        file_order = ['agency', 'calendar', 'routes', 'stops', 'trips', 'shapes', 'stop_times', 'calendar_dates']
        
        for file_type in file_order:
            start_time = datetime.now()
            self.logger.info(f"Processing {file_type}.txt...")
            file_path = os.path.join(gtfs_folder, f"{file_type}.txt")
            
            if not os.path.exists(file_path):
                self.logger.warning(f"Optional file {file_type}.txt not found, skipping...")
                continue
            
            # Validate and clean data
            df, errors = self.validate_gtfs_file(file_path, file_type)
            
            if df is not None and len(df) > 0:
                try:
                    # Clear existing data for this table
                    self.conn.execute(f"DELETE FROM {file_type}")
                    
                    # Insert new data using INSERT statements to avoid pandas to_sql issues
                    self.insert_dataframe_to_table(df, file_type)
                    self.logger.info(f"Loaded {len(df)} rows into {file_type} table")
                except Exception as e:
                    self.logger.error(f"Error inserting {file_type} data: {str(e)}")
                    errors.append(f"Error inserting {file_type} data: {str(e)}")
            else:
                self.logger.error(f"Failed to load {file_type} data")
            
            self.validation_errors.extend(errors)
            end_time = datetime.now()
            self.logger.info(f"Completed processing for {file_type}.txt")
            self.logger.info(f"Total time for loading {file_type}: {end_time - start_time}")

        # Clean up referential integrity issues after all data is loaded
        self.clean_referential_integrity()
        
        # Print summary
        self.print_load_summary()
    
    def insert_dataframe_to_table(self, df: pd.DataFrame, table_name: str):
        """Insert dataframe data using DuckDB INSERT statements."""
        if len(df) == 0:
            return
        
        # Replace NaN values with None for proper NULL handling
        df_clean = df.where(pd.notnull(df), None)
        
        # Get column names
        columns = list(df_clean.columns)
        placeholders = ', '.join(['?' for _ in columns])
        column_list = ', '.join(columns)
        
        insert_sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"
        
        # Convert dataframe to list of tuples for insertion
        data = df_clean.values.tolist()
        
        # Insert in batches to avoid memory issues
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            self.conn.executemany(insert_sql, batch)
    
    def clean_referential_integrity(self):
        """Remove rows that violate referential integrity constraints."""
        self.logger.info("Cleaning referential integrity...")
        
        try:
            # Count and remove trips that reference non-existent routes
            invalid_trips = self.conn.execute("""
                SELECT COUNT(*) FROM trips 
                WHERE route_id NOT IN (SELECT route_id FROM routes)
            """).fetchone()[0]
            
            if invalid_trips > 0:
                self.conn.execute("""
                    DELETE FROM trips 
                    WHERE route_id NOT IN (SELECT route_id FROM routes)
                """)
                self.logger.warning(f"Removed {invalid_trips} trips with invalid route references")
            
            # Count and remove trips that reference non-existent services
            invalid_service_trips = self.conn.execute("""
                SELECT COUNT(*) FROM trips 
                WHERE service_id NOT IN (
                    SELECT service_id FROM calendar 
                    UNION 
                    SELECT DISTINCT service_id FROM calendar_dates
                )
            """).fetchone()[0]
            
            if invalid_service_trips > 0:
                self.conn.execute("""
                    DELETE FROM trips 
                    WHERE service_id NOT IN (
                        SELECT service_id FROM calendar 
                        UNION 
                        SELECT DISTINCT service_id FROM calendar_dates
                    )
                """)
                self.logger.warning(f"Removed {invalid_service_trips} trips with invalid service references")
            
            # Count and remove stop_times that reference non-existent trips
            invalid_stop_times = self.conn.execute("""
                SELECT COUNT(*) FROM stop_times 
                WHERE trip_id NOT IN (SELECT trip_id FROM trips)
            """).fetchone()[0]
            
            if invalid_stop_times > 0:
                self.conn.execute("""
                    DELETE FROM stop_times 
                    WHERE trip_id NOT IN (SELECT trip_id FROM trips)
                """)
                self.logger.warning(f"Removed {invalid_stop_times} stop_times with invalid trip references")
            
            # Count and remove stop_times that reference non-existent stops
            invalid_stop_refs = self.conn.execute("""
                SELECT COUNT(*) FROM stop_times 
                WHERE stop_id NOT IN (SELECT stop_id FROM stops)
            """).fetchone()[0]
            
            if invalid_stop_refs > 0:
                self.conn.execute("""
                    DELETE FROM stop_times 
                    WHERE stop_id NOT IN (SELECT stop_id FROM stops)
                """)
                self.logger.warning(f"Removed {invalid_stop_refs} stop_times with invalid stop references")
                
        except Exception as e:
            self.logger.error(f"Error during referential integrity cleanup: {str(e)}")
    
    def print_load_summary(self):
        """Print a summary of the data loading process."""
        self.logger.info("\n" + "="*50)
        self.logger.info("GTFS DATA LOADING SUMMARY")
        self.logger.info("="*50)
        
        # Table counts
        for table in ['agency', 'routes', 'stops', 'trips', 'stop_times', 'calendar', 'calendar_dates', 'shapes']:
            try:
                result = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                count = result[0] if result else 0
                self.logger.info(f"{table.ljust(15)}: {count:,} rows")
            except:
                self.logger.info(f"{table.ljust(15)}: Table not found")
        
        # Cleaning statistics
        if self.cleaning_stats:
            self.logger.info("\nCLEANING STATISTICS:")
            for file_type, stats in self.cleaning_stats.items():
                removed = stats['rows_removed']
                if removed > 0:
                    self.logger.info(f"{file_type}: Removed {removed} invalid rows ({removed/stats['original_rows']*100:.1f}%)")
        
        # Validation errors
        if self.validation_errors:
            self.logger.warning(f"\nVALIDATION ERRORS ({len(self.validation_errors)}):")
            for error in self.validation_errors:
                self.logger.warning(f"  - {error}")
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics."""
        stats = {}
        
        tables = ['agency', 'routes', 'stops', 'trips', 'stop_times', 'calendar', 'calendar_dates', 'shapes']
        
        for table in tables:
            try:
                result = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                stats[table] = result[0] if result else 0
            except:
                stats[table] = 0
        
        return stats
    
    def update_gtfs_data(self, gtfs_folder: str):
        """
            Update the database with new GTFS data (full replacement).
            TODO: Implement incremental updates if needed.
        """
        start_time = datetime.now()
        self.logger.info(f"Starting GTFS data update at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Load new data
        self.load_gtfs_data(gtfs_folder)
        
        end_time = datetime.now()
        self.logger.info(f"GTFS data update completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Total update duration: {end_time - start_time}")

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")