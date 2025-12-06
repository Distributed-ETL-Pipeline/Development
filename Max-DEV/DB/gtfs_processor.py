import duckdb
import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List, Optional, Tuple, Any
import re

def load_env_file(env_path: str = ".env"):
    """load environment variables from .env file"""
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()

class GTFSProcessor:
    """processes GTFS data into DuckDB"""
    
    def __init__(self, db_path: str = "gtfs_database.db"):
        """initialize processor with database connection"""
        load_env_file()
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
        """setup logging configuration"""
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
        """create duckdb tables with schema"""
        self.logger.info("Creating database tables...")
        
        # drop existing tables
        tables_to_drop = ['stop_times', 'trips', 'calendar_dates', 'shapes', 'routes', 'stops', 'calendar', 'agency']
        for table in tables_to_drop:
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")
            except:
                pass
        
        # agency table
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
        
        # routes table
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
        
        # stops table
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
        
        # calendar table
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
        
        # calendar dates table
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
        
        # trips table
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
        
        # stop times table
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
        
        # shapes table
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
        """validate gtfs file and return cleaned data"""
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
        """clean and validate data for specific gtfs file type"""
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
        """clean agency data"""
        errors = []
        initial_count = len(df)
        
        # remove rows with missing required fields
        df = df.dropna(subset=['agency_id', 'agency_name', 'agency_url', 'agency_timezone'])
        
        # validate urls
        url_pattern = re.compile(r'^https?://.+')
        df = df[df['agency_url'].str.match(url_pattern, na=False)]
        
        # clean text fields
        df['agency_name'] = df['agency_name'].str.strip()
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid agency rows")
        
        return df, errors
    
    def clean_routes_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """clean routes data"""
        errors = []
        initial_count = len(df)
        
        # remove rows with missing required fields (lenient)
        df = df.dropna(subset=['route_id'])  # only require route_id
        
        # fill missing agency_id with default
        if 'agency_id' in df.columns:
            df['agency_id'] = df['agency_id'].fillna('1')  # default agency_id
        else:
            df['agency_id'] = '1'
        
        # validate route type, default to 3 (bus)
        if 'route_type' in df.columns:
            df['route_type'] = pd.to_numeric(df['route_type'], errors='coerce')
            df['route_type'] = df['route_type'].fillna(3)  # default to bus
        else:
            df['route_type'] = 3
        
        df['route_type'] = df['route_type'].astype(int)
        
        # convert id fields to strings
        if 'route_id' in df.columns:
            df['route_id'] = df['route_id'].astype(str)
        
        # validate colors (6-digit hex without #)
        if 'route_color' in df.columns:
            # convert to string and validate
            df['route_color'] = df['route_color'].astype(str).replace('nan', '')
            color_pattern = re.compile(r'^[0-9A-Fa-f]{6}$')
            invalid_colors = ~df['route_color'].str.match(color_pattern, na=False) | (df['route_color'] == '')
            df.loc[invalid_colors, 'route_color'] = None
        
        if 'route_text_color' in df.columns:
            # convert to string, handle integers
            df['route_text_color'] = df['route_text_color'].astype(str).replace('nan', '')
            # convert 0 to proper hex format
            df.loc[df['route_text_color'] == '0', 'route_text_color'] = '000000'
            color_pattern = re.compile(r'^[0-9A-Fa-f]{6}$')
            invalid_colors = ~df['route_text_color'].str.match(color_pattern, na=False) | (df['route_text_color'] == '')
            df.loc[invalid_colors, 'route_text_color'] = None
        
        # ensure route names exist
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
        """clean stops data"""
        errors = []
        initial_count = len(df)
        
        # remove rows with missing required fields
        df = df.dropna(subset=['stop_id', 'stop_name', 'stop_lat', 'stop_lon'])
        
        # validate coordinates
        df = df[(df['stop_lat'] >= -90) & (df['stop_lat'] <= 90)]
        df = df[(df['stop_lon'] >= -180) & (df['stop_lon'] <= 180)]
        
        # clean stop names
        df['stop_name'] = df['stop_name'].str.strip()
        
        # handle numeric columns that might have nan values
        numeric_columns = ['wheelchair_boarding', 'location_type', 'zone_id']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # validate wheelchair boarding values (0, 1, 2)
        if 'wheelchair_boarding' in df.columns:
            df.loc[~df['wheelchair_boarding'].isin([0, 1, 2]), 'wheelchair_boarding'] = None
        
        # set default location_type to 0 (stop) if nan
        if 'location_type' in df.columns:
            df['location_type'] = df['location_type'].fillna(0)
        
        # convert stop_id and stop_code to string
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
        """clean trips data"""
        errors = []
        initial_count = len(df)
        
        # remove rows with missing required fields
        df = df.dropna(subset=['trip_id', 'route_id', 'service_id'])
        
        # convert id fields to strings
        for col in ['trip_id', 'route_id', 'service_id', 'block_id', 'shape_id']:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df.loc[df[col] == 'nan', col] = None
        
        # validate direction_id (0 or 1)
        if 'direction_id' in df.columns:
            df['direction_id'] = pd.to_numeric(df['direction_id'], errors='coerce')
            df.loc[~df['direction_id'].isin([0, 1]), 'direction_id'] = None
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid trip rows")
        
        return df, errors
    
    def clean_stop_times_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """clean stop times data"""
        errors = []
        initial_count = len(df)
        
        # remove rows with missing required fields
        df = df.dropna(subset=['trip_id', 'stop_id', 'stop_sequence'])
        
        # clean time format (hh:mm:ss, can be > 24:00:00)
        time_pattern = re.compile(r'^\d{1,2}:\d{2}:\d{2}$')
        
        if 'arrival_time' in df.columns:
            df = df[df['arrival_time'].str.match(time_pattern, na=False) | df['arrival_time'].isna()]
        
        if 'departure_time' in df.columns:
            df = df[df['departure_time'].str.match(time_pattern, na=False) | df['departure_time'].isna()]
        
        # validate stop_sequence (must be non-negative integer)
        df = df[df['stop_sequence'] >= 0]
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid stop time rows")
        
        return df, errors
    
    def clean_calendar_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """clean calendar data"""
        errors = []
        initial_count = len(df)
        
        # remove rows with missing required fields
        required_cols = ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date']
        df = df.dropna(subset=required_cols)
        
        # validate day columns (0 or 1)
        day_cols = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        for col in day_cols:
            df = df[df[col].isin([0, 1])]
        
        # validate date format (yyyymmdd)
        date_pattern = re.compile(r'^\d{8}$')
        df = df[df['start_date'].astype(str).str.match(date_pattern)]
        df = df[df['end_date'].astype(str).str.match(date_pattern)]
        
        # convert dates to proper format
        df['start_date'] = pd.to_datetime(df['start_date'].astype(str), format='%Y%m%d')
        df['end_date'] = pd.to_datetime(df['end_date'].astype(str), format='%Y%m%d')
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid calendar rows")
        
        return df, errors
    
    def clean_calendar_dates_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """clean calendar dates data"""
        errors = []
        initial_count = len(df)
        
        # remove rows with missing required fields
        df = df.dropna(subset=['service_id', 'date', 'exception_type'])
        
        # validate exception type (1 or 2)
        df = df[df['exception_type'].isin([1, 2])]
        
        # validate date format (yyyymmdd)
        date_pattern = re.compile(r'^\d{8}$')
        df = df[df['date'].astype(str).str.match(date_pattern)]
        
        # convert date to proper format
        df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d')
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid calendar date rows")
        
        return df, errors
    
    def clean_shapes_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """clean shapes data"""
        errors = []
        initial_count = len(df)
        
        # remove rows with missing required fields
        df = df.dropna(subset=['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence'])
        
        # validate coordinates
        df = df[(df['shape_pt_lat'] >= -90) & (df['shape_pt_lat'] <= 90)]
        df = df[(df['shape_pt_lon'] >= -180) & (df['shape_pt_lon'] <= 180)]
        
        # validate sequence (non-negative integer)
        df = df[df['shape_pt_sequence'] >= 0]
        
        rows_removed = initial_count - len(df)
        if rows_removed > 0:
            errors.append(f"Removed {rows_removed} invalid shape rows")
        
        return df, errors
    
    def load_gtfs_data(self, gtfs_folder: str):
        """load and process all gtfs files from folder"""
        self.logger.info(f"Loading GTFS data from {gtfs_folder}")
        self.validation_errors = []
        self.cleaning_stats = {}
        
        # Create tables
        self.create_tables()
        
        # process files in dependency order
        file_order = ['agency', 'calendar', 'routes', 'stops', 'trips', 'shapes', 'stop_times', 'calendar_dates']
        
        for file_type in file_order:
            start_time = datetime.now()
            self.logger.info(f"Processing {file_type}.txt...")
            file_path = os.path.join(gtfs_folder, f"{file_type}.txt")
            
            if not os.path.exists(file_path):
                self.logger.warning(f"Optional file {file_type}.txt not found, skipping...")
                continue
            
            # validate and clean data
            df, errors = self.validate_gtfs_file(file_path, file_type)
            
            if df is not None and len(df) > 0:
                try:
                    # clear existing data for this table
                    self.conn.execute(f"DELETE FROM {file_type}")
                    
                    # insert new data
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

        # clean up referential integrity after all data loaded
        self.clean_referential_integrity()
        
        # print summary
        self.print_load_summary()
    
    def insert_dataframe_to_table(self, df: pd.DataFrame, table_name: str):
        """insert dataframe using duckdb insert statements"""
        if len(df) == 0:
            return
        
        # replace nan values with none for proper null handling
        df_clean = df.where(pd.notnull(df), None)
        
        # get column names
        columns = list(df_clean.columns)
        placeholders = ', '.join(['?' for _ in columns])
        column_list = ', '.join(columns)
        
        insert_sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"
        
        # convert dataframe to list of tuples for insertion
        data = df_clean.values.tolist()
        
        # insert in batches to avoid memory issues
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            self.conn.executemany(insert_sql, batch)
    
    def clean_referential_integrity(self):
        """remove rows that violate referential integrity"""
        self.logger.info("Cleaning referential integrity...")
        
        try:
            # remove trips that reference non-existent routes
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
            
            # remove trips that reference non-existent services
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
            
            # remove stop_times that reference non-existent trips
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
            
            # remove stop_times that reference non-existent stops
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
        """print summary of data loading"""
        self.logger.info("\n" + "="*50)
        self.logger.info("GTFS DATA LOADING SUMMARY")
        self.logger.info("="*50)
        
        # table counts
        for table in ['agency', 'routes', 'stops', 'trips', 'stop_times', 'calendar', 'calendar_dates', 'shapes']:
            try:
                result = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                count = result[0] if result else 0
                self.logger.info(f"{table.ljust(15)}: {count:,} rows")
            except:
                self.logger.info(f"{table.ljust(15)}: Table not found")
        
        # cleaning statistics
        if self.cleaning_stats:
            self.logger.info("\nCLEANING STATISTICS:")
            for file_type, stats in self.cleaning_stats.items():
                removed = stats['rows_removed']
                if removed > 0:
                    self.logger.info(f"{file_type}: Removed {removed} invalid rows ({removed/stats['original_rows']*100:.1f}%)")
        
        # validation errors
        if self.validation_errors:
            self.logger.warning(f"\nVALIDATION ERRORS ({len(self.validation_errors)}):")
            for error in self.validation_errors:
                self.logger.warning(f"  - {error}")

    # realtime helpers
    def create_realtime_table(self):
        """create vehicle_positions table for realtime gtfs data"""
        self.logger.info("Creating realtime vehicle_positions table if not exists...")
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS vehicle_positions (
                    entity_id VARCHAR,
                    vehicle_id VARCHAR,
                    vehicle_label VARCHAR,
                    trip_id VARCHAR,
                    route_id VARCHAR,
                    start_date VARCHAR,
                    direction_id INTEGER,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    bearing DOUBLE,
                    speed DOUBLE,
                    current_stop_sequence INTEGER,
                    current_status INTEGER,
                    stop_id VARCHAR,
                    timestamp BIGINT,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.logger.info("Realtime table ready")
        except Exception as e:
            self.logger.error(f"Error creating realtime table: {e}")

    def upsert_vehicle_positions(self, records: List[Dict[str, Any]]):
        """upsert batch of vehicle position records"""
        if not records:
            self.logger.info("No realtime records to upsert")
            return


        df = pd.DataFrame(records)
        # Ensure correct columns exist (match create_realtime_table)
        expected_cols = ['entity_id','vehicle_id','vehicle_label','trip_id','route_id','start_date','direction_id',
                         'latitude','longitude','bearing','speed','current_stop_sequence','current_status','stop_id','timestamp']
        # Keep only expected columns (if present)
        df = df[[c for c in expected_cols if c in df.columns]]

        # replace nan values with none
        df = df.where(pd.notnull(df), None)

        try:
            # Clear entire table for fresh snapshot (realtime data should only show current positions)
            self.conn.execute("DELETE FROM vehicle_positions")
            self.logger.info("Cleared vehicle_positions table for fresh snapshot")

            # insert new rows
            self.insert_dataframe_to_table(df, 'vehicle_positions')
            self.logger.info(f"Inserted {len(df)} realtime vehicle position rows")
            
            # push to motherduck after successful upsert
            self.push_vehicle_positions_to_motherduck()
        except Exception as e:
            self.logger.error(f"Error upserting realtime records: {e}")

    def push_vehicle_positions_to_motherduck(self):
        """push enriched vehicle_positions with calculated fields to motherduck"""
        import os
        
        md_token = os.environ.get("MOTHERDUCK_TOKEN")
        if not md_token:
            self.logger.warning("MOTHERDUCK_TOKEN not set; skipping motherduck push")
            return
        
        try:
            self.logger.info("creating enriched vehicle positions with calculated fields...")
            
            # Create enriched query with joins and calculations
            enriched_query = """
            WITH next_stop_calc AS (
                SELECT 
                    vp.entity_id,
                    vp.vehicle_id,
                    vp.vehicle_label,
                    vp.trip_id,
                    vp.route_id,
                    vp.start_date,
                    vp.direction_id,
                    vp.latitude,
                    vp.longitude,
                    vp.bearing,
                    vp.speed,
                    vp.current_stop_sequence,
                    vp.current_status,
                    vp.stop_id as current_stop_id,
                    vp.timestamp,
                    vp.ingested_at,
                    -- Join with routes for route info
                    r.route_short_name,
                    r.route_long_name,
                    r.route_type,
                    -- Join with trips for additional trip info
                    t.trip_headsign,
                    t.direction_id as trip_direction,
                    -- Calculate next stop info
                    st.stop_id as next_stop_id,
                    st.stop_sequence as next_stop_sequence,
                    st.arrival_time as next_stop_scheduled_arrival,
                    st.departure_time as next_stop_scheduled_departure,
                    s.stop_name as next_stop_name,
                    s.stop_lat as next_stop_lat,
                    s.stop_lon as next_stop_lon,
                    -- Calculate distance to next stop (Haversine formula in meters)
                    ROUND(
                        6371000 * 2 * ASIN(SQRT(
                            POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                            COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                            POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                        ))
                    , 1) AS distance_to_next_stop_m,
                    -- Calculate ETA in minutes (distance / speed)
                    ROUND(
                        CASE 
                            WHEN vp.speed > 0 THEN 
                                (6371000 * 2 * ASIN(SQRT(
                                    POWER(SIN(RADIANS(s.stop_lat - vp.latitude) / 2), 2) + 
                                    COS(RADIANS(vp.latitude)) * COS(RADIANS(s.stop_lat)) * 
                                    POWER(SIN(RADIANS(s.stop_lon - vp.longitude) / 2), 2)
                                ))) / NULLIF(vp.speed, 0) / 60
                            ELSE NULL
                        END
                    , 1) AS eta_minutes,
                    -- Speed in km/h for readability
                    ROUND(vp.speed * 3.6, 1) as speed_kmh,
                    ROW_NUMBER() OVER (PARTITION BY vp.entity_id ORDER BY st.stop_sequence ASC) AS rn
                FROM vehicle_positions vp
                LEFT JOIN trips t ON vp.trip_id = t.trip_id
                LEFT JOIN routes r ON vp.route_id = r.route_id
                LEFT JOIN stop_times st ON t.trip_id = st.trip_id 
                    AND st.stop_sequence > COALESCE(vp.current_stop_sequence, -1)
                LEFT JOIN stops s ON st.stop_id = s.stop_id
            )
            SELECT 
                entity_id,
                vehicle_id,
                vehicle_label,
                trip_id,
                route_id,
                route_short_name,
                route_long_name,
                CASE route_type
                    WHEN 0 THEN 'Tram/Light Rail'
                    WHEN 1 THEN 'Subway/Metro'
                    WHEN 2 THEN 'Rail'
                    WHEN 3 THEN 'Bus'
                    WHEN 4 THEN 'Ferry'
                    WHEN 5 THEN 'Cable Tram'
                    WHEN 6 THEN 'Aerial Lift'
                    WHEN 7 THEN 'Funicular'
                    ELSE 'Unknown'
                END as vehicle_type,
                trip_headsign,
                direction_id,
                latitude,
                longitude,
                bearing,
                speed,
                speed_kmh,
                current_stop_sequence,
                CASE current_status
                    WHEN 0 THEN 'Incoming'
                    WHEN 1 THEN 'Stopped'
                    WHEN 2 THEN 'In Transit'
                    ELSE 'Unknown'
                END as current_status_text,
                current_stop_id,
                next_stop_id,
                next_stop_name,
                next_stop_sequence,
                next_stop_scheduled_arrival,
                next_stop_scheduled_departure,
                next_stop_lat,
                next_stop_lon,
                distance_to_next_stop_m,
                eta_minutes,
                CASE 
                    WHEN eta_minutes IS NULL THEN 'No Data'
                    WHEN eta_minutes < 0 THEN 'Early'
                    WHEN eta_minutes BETWEEN 0 AND 2 THEN 'On Time'
                    WHEN eta_minutes BETWEEN 2 AND 5 THEN 'Slightly Late'
                    WHEN eta_minutes BETWEEN 5 AND 15 THEN 'Late'
                    ELSE 'Very Late'
                END as delay_status,
                timestamp,
                ingested_at
            FROM next_stop_calc
            WHERE rn = 1 OR rn IS NULL
            """
            
            enriched_df = self.conn.execute(enriched_query).df()
            row_count = len(enriched_df)
            self.logger.info(f"created enriched dataset with {row_count} rows")
            
            self.logger.info("pushing enriched data to shared MotherDuck database ETL_Realtime...")
            md_con = duckdb.connect("md:", config={"motherduck_token": md_token, "allow_unsigned_extensions": "true"})
            
            # Switch to the shared database
            md_con.execute("USE ETL_Realtime")
            
            md_con.register('vehicle_positions_enriched', enriched_df)
            md_con.execute("CREATE OR REPLACE TABLE vehicle_positions AS SELECT * FROM vehicle_positions_enriched")
            
            self.logger.info(f"successfully pushed {row_count} enriched vehicle_positions rows to ETL_Realtime.vehicle_positions")
            md_con.close()
        except Exception as e:
            self.logger.error(f"failed to push vehicle_positions to motherduck: {e}", exc_info=True)

    def ingest_and_push_vehiclepositions(self, pb_file_path: str) -> int:
        """Parse a local GTFS-Realtime vehiclepositions .pb file, upsert, and push to MotherDuck.

        Returns number of records ingested. Only affects the `vehicle_positions` table.
        """
        try:
            if not os.path.exists(pb_file_path):
                raise FileNotFoundError(f"vehiclepositions file not found: {pb_file_path}")

            self.create_realtime_table()

            with open(pb_file_path, 'rb') as f:
                data = f.read()

            records = self.parse_vehiclepositions_bytes(data)
            if not records:
                self.logger.info("No records parsed from feed; nothing to upsert")
                return 0

            self.upsert_vehicle_positions(records)
            return len(records)
        except Exception as e:
            self.logger.error(f"Failed ingest-and-push for vehiclepositions: {e}")
            return 0

    def get_recent_vehicle_positions(self, limit: int = 50):
        """get recent vehicle positions ordered by timestamp"""
        try:
            rows = self.conn.execute(f"SELECT * FROM vehicle_positions ORDER BY timestamp DESC NULLS LAST LIMIT {int(limit)}").fetchall()
            return rows
        except Exception as e:
            self.logger.error(f"Error fetching recent vehicle positions: {e}")
            return []

    def parse_vehiclepositions_bytes(self, data: bytes) -> List[Dict[str, Any]]:
        """parse gtfs-realtime vehiclepositions protobuf bytes"""
        try:
            # import locally to avoid hard dependency
            try:
                from google.transit import gtfs_realtime_pb2  # type: ignore
            except Exception:
                import gtfs_realtime_pb2  # type: ignore
            from google.protobuf.json_format import MessageToDict  # noqa: F401
        except Exception as e:
            raise RuntimeError("gtfs-realtime protobuf bindings not installed. Install with: pip install gtfs-realtime-bindings protobuf")

        last_err = None
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(data)
        except Exception as e:
            last_err = e
            # try gzip
            import gzip
            try:
                decompressed = gzip.decompress(data)
                feed.ParseFromString(decompressed)
            except Exception as e2:
                raise RuntimeError(f"Failed to parse protobuf feed: {e2}")

        records: List[Dict[str, Any]] = []
        for ent in feed.entity:
            # vehicle entity
            if not hasattr(ent, 'vehicle') or getattr(ent, 'vehicle') is None:
                continue
            v = ent.vehicle
            trip = getattr(v, 'trip', None)
            pos = getattr(v, 'position', None)
            veh = getattr(v, 'vehicle', None)

            rec: Dict[str, Any] = {
                'entity_id': getattr(ent, 'id', None),
                'vehicle_id': getattr(veh, 'id', None) if veh is not None else None,
                'vehicle_label': getattr(veh, 'label', None) if veh is not None else None,
                'trip_id': getattr(trip, 'trip_id', None) if trip is not None else None,
                'route_id': getattr(trip, 'route_id', None) if trip is not None else None,
                'start_date': getattr(trip, 'start_date', None) if trip is not None else None,
                'direction_id': getattr(trip, 'direction_id', None) if trip is not None else None,
                'latitude': getattr(pos, 'latitude', None) if pos is not None else None,
                'longitude': getattr(pos, 'longitude', None) if pos is not None else None,
                'bearing': getattr(pos, 'bearing', None) if pos is not None else None,
                'speed': getattr(pos, 'speed', None) if pos is not None else None,
                'current_stop_sequence': getattr(v, 'current_stop_sequence', None),
                'current_status': getattr(v, 'current_status', None),
                'stop_id': getattr(v, 'stop_id', None),
                'timestamp': getattr(v, 'timestamp', None)
            }
            records.append(rec)

        return records
        
    def estimate_eta_for_vehicle(self, entity_id: Optional[str] = None, vehicle_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """estimate next stop, eta and delay for a vehicle"""
        import math
        from datetime import datetime, timedelta

        if not entity_id and not vehicle_id:
            raise ValueError('Provide entity_id or vehicle_id')

        # use inline sql haversine expression

        # find latest vehicle row
        where_clause = ''
        if entity_id:
            where_clause = "v.entity_id = '" + str(entity_id).replace("'", "''") + "'"
        else:
            where_clause = "v.vehicle_id = '" + str(vehicle_id).replace("'", "''") + "'"

        try:
            # use inline sql haversine expression instead of python udf
            distance_expr = (
                "(6371000 * 2 * ASIN(SQRT("
                "POWER(SIN(RADIANS(s.stop_lat - v.latitude) / 2), 2) + "
                "COS(RADIANS(v.latitude)) * COS(RADIANS(s.stop_lat)) * "
                "POWER(SIN(RADIANS(s.stop_lon - v.longitude) / 2), 2)"
                ")) )"
            )

            # Use COALESCE on v.current_stop_sequence so that NULL values
            # are treated as -1 (so stop_sequence > -1 will match sequence 0+)
            q = f"""
            SELECT v.entity_id, v.vehicle_id, v.trip_id, v.route_id, v.start_date, v.direction_id,
                   v.latitude, v.longitude, v.timestamp, v.current_stop_sequence, v.speed,
                   st.stop_id, st.stop_sequence, st.arrival_time, s.stop_lat, s.stop_lon,
                   {distance_expr} AS distance_m
            FROM vehicle_positions v
            JOIN trips t ON v.trip_id = t.trip_id
            JOIN stop_times st ON st.trip_id = t.trip_id AND st.stop_sequence > COALESCE(v.current_stop_sequence, -1)
            JOIN stops s ON s.stop_id = st.stop_id
            WHERE {where_clause}
            ORDER BY st.stop_sequence ASC
            LIMIT 1
            """

            row = self.conn.execute(q).fetchone()
            if not row:
                self.logger.info('No next stop found for vehicle')
                return None

            (ent_id, veh_id, trip_id, route_id, start_date, direction_id,
             vlat, vlon, vtimestamp, current_seq, speed_m_s,
             stop_id, stop_seq, arrival_time_str, stop_lat, stop_lon, distance_m) = row

            # compute eta using speed if available and > 0
            eta_epoch = None
            eta_iso = None
            if speed_m_s and speed_m_s > 0 and distance_m is not None:
                eta_seconds = distance_m / float(speed_m_s)
                eta_epoch = int(float(vtimestamp) + eta_seconds) if vtimestamp else None
                if eta_epoch:
                    eta_iso = datetime.utcfromtimestamp(eta_epoch).isoformat() + 'Z'

            # compute scheduled arrival epoch by combining start_date and arrival_time
            scheduled_epoch = None
            scheduled_iso = None
            try:
                if start_date and arrival_time_str:
                    # start_date like 20251108
                    sd = datetime.strptime(str(start_date), '%Y%m%d')
                    # arrival_time may be >24:00:00, handle overflow
                    parts = arrival_time_str.split(':')
                    hh = int(parts[0])
                    mm = int(parts[1])
                    ss = int(parts[2])
                    days_add = hh // 24
                    hh = hh % 24
                    scheduled_local = datetime(sd.year, sd.month, sd.day, hh, mm, ss) + timedelta(days=days_add)

                    # determine agency timezone
                    agency_tz = None
                    try:
                        if route_id:
                            tz_row = self.conn.execute(
                                "SELECT a.agency_timezone FROM agency a JOIN routes r ON a.agency_id = r.agency_id WHERE r.route_id = ? LIMIT 1",
                                [route_id]
                            ).fetchone()
                            if tz_row:
                                agency_tz = tz_row[0]
                        if not agency_tz:
                            # fallback: take first agency timezone
                            tz_row = self.conn.execute("SELECT agency_timezone FROM agency LIMIT 1").fetchone()
                            if tz_row:
                                agency_tz = tz_row[0]
                    except Exception:
                        agency_tz = None

                    # convert local scheduled time to utc epoch
                    try:
                        # prefer zoneinfo (python 3.9+), fallback to pytz
                        try:
                            from zoneinfo import ZoneInfo
                            tz = ZoneInfo(agency_tz) if agency_tz else None
                            if tz:
                                scheduled_aware = scheduled_local.replace(tzinfo=tz)
                                scheduled_utc = scheduled_aware.astimezone(tz=datetime.timezone.utc)
                            else:
                                # treat as naive local, assume utc
                                scheduled_utc = scheduled_local
                        except Exception:
                            # fallback to pytz
                            import pytz
                            if agency_tz:
                                tz = pytz.timezone(agency_tz)
                                scheduled_aware = tz.localize(scheduled_local)
                                scheduled_utc = scheduled_aware.astimezone(pytz.utc)
                            else:
                                scheduled_utc = scheduled_local

                        # handle naive utc if couldn't localize
                        if scheduled_utc.tzinfo is None:
                            # treat as utc
                            scheduled_epoch = int(scheduled_utc.timestamp())
                            scheduled_iso = scheduled_utc.isoformat() + 'Z'
                        else:
                            scheduled_epoch = int(scheduled_utc.timestamp())
                            scheduled_iso = scheduled_utc.isoformat()
                    except Exception as e:
                        self.logger.warning(f'could not convert scheduled time to utc: {e}')
                        scheduled_epoch = None
                        scheduled_iso = None
            except Exception as e:
                self.logger.warning(f'could not parse scheduled arrival time: {e}')

            delay_s = None
            if eta_epoch and scheduled_epoch:
                delay_s = eta_epoch - scheduled_epoch
            elif vtimestamp and scheduled_epoch and not eta_epoch:
                # fallback: use vehicle timestamp as actual time
                delay_s = int(vtimestamp) - scheduled_epoch

            result = {
                'entity_id': ent_id,
                'vehicle_id': veh_id,
                'trip_id': trip_id,
                'route_id': route_id,
                'next_stop_id': stop_id,
                'next_stop_sequence': stop_seq,
                'distance_m': distance_m,
                'eta_epoch': eta_epoch,
                'eta_iso': eta_iso,
                'scheduled_epoch': scheduled_epoch,
                'scheduled_iso': scheduled_iso,
                'delay_s': delay_s,
                'vehicle_timestamp': vtimestamp,
                'speed_m_s': speed_m_s
            }

            return result
        except Exception as e:
            self.logger.error(f'Error estimating ETA: {e}')
            return None
    
    def get_database_stats(self) -> Dict[str, Any]:
        """get comprehensive database statistics"""
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
        """update database with new gtfs data (full replacement)"""
        start_time = datetime.now()
        self.logger.info(f"Starting GTFS data update at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Load new data
        self.load_gtfs_data(gtfs_folder) # <-- This currently does a full replacement. 
        
        end_time = datetime.now()
        self.logger.info(f"GTFS data update completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Total update duration: {end_time - start_time}")

    def close(self):
        """close database connection"""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")