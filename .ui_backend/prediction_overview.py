import json
import time
import psycopg2
import psycopg2.extras
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

class MultiMachinePredictionMonitor:
    def __init__(self, source_db_config: Dict, target_db_config: Dict, prediction_json_file: str = "prediction.json"):
        self.source_db_config = source_db_config
        self.target_db_config = target_db_config
        self.source_db = None
        self.target_db = None
        self.machines = ["mc17", "mc18", "mc19", "mc20", "mc21", "mc22", 
                         "mc25", "mc26", "mc27", "mc28", "mc29", "mc30"]
        self.prediction_points = {
            "Quality": ["tp61", "tp62", "tp69","tp71"],
            "Breakdown": [],
            "Productivity": []
        }
        self.prediction_json = self.load_prediction_json(prediction_json_file)
        self.connect_to_dbs()

    def connect_to_dbs(self, max_retries: int = 5, initial_delay: int = 5):
        """Establish or re-establish connections to both source and target databases."""
        # Connect to source database (remote)
        if self.source_db is None or self.source_db.closed:
            for attempt in range(max_retries):
                try:
                    self.source_db = psycopg2.connect(**self.source_db_config)
                    print(f"Connected to source database at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    break
                except psycopg2.Error as e:
                    print(f"Failed to connect to source database (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        delay = initial_delay * (2 ** attempt)
                        print(f"Retrying source connection in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        print("Max retries reached for source database.")
                        self.source_db = None

        # Connect to target database (local)
        if self.target_db is None or self.target_db.closed:
            for attempt in range(max_retries):
                try:
                    self.target_db = psycopg2.connect(**self.target_db_config)
                    print(f"Connected to target database at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    break
                except psycopg2.Error as e:
                    print(f"Failed to connect to target database (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        delay = initial_delay * (2 ** attempt)
                        print(f"Retrying target connection in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        print("Max retries reached for target database.")
                        self.target_db = None

    def check_db_connection(self):
        """Check if both database connections are valid, reconnect if necessary."""
        if self.source_db is None or self.source_db.closed:
            print("Source database connection is closed, attempting to reconnect...")
            self.connect_to_dbs()
        if self.target_db is None or self.target_db.closed:
            print("Target database connection is closed, attempting to reconnect...")
            self.connect_to_dbs()
        return (self.source_db is not None and not self.source_db.closed and
                self.target_db is not None and not self.target_db.closed)

    def load_prediction_json(self, filename: str) -> Dict[str, str]:
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    print(f"Successfully loaded prediction JSON from {filename}")
                    return data
            else:
                print(f"Warning: {filename} not found. Using default empty mapping.")
                return {}
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON file {filename}: {e}")
            return {}
        except Exception as e:
            print(f"Error loading prediction JSON file {filename}: {e}")
            return {}

    def parse_json_column(self, json_data) -> Optional[Dict]:
        try:
            if json_data is None:
                return None
            if isinstance(json_data, dict):
                return json_data
            if isinstance(json_data, str) and json_data.strip():
                return json.loads(json_data)
            return None
        except (json.JSONDecodeError, TypeError):
            return None

    def get_latest_active_column(self, machine: str, columns: List[str]) -> Optional[Tuple[str, Dict]]:
        if not self.check_db_connection():
            print(f"Cannot query {machine}: No valid database connection")
            return None

        cursor = self.source_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            table_name = f"{machine}_tp_status"
            column_list = ", ".join(columns)
            query = f"SELECT {column_list} FROM {table_name}"
            cursor.execute(query)
            result = cursor.fetchone()

            if not result:
                return None

            active_columns = []
            for column in columns:
                json_data = self.parse_json_column(result[column])
                if json_data and json_data.get('active') == 1:
                    timestamp_str = json_data.get('timestamp')
                    if timestamp_str:
                        try:
                            if timestamp_str.endswith('Z'):
                                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            else:
                                timestamp = datetime.fromisoformat(timestamp_str)
                            active_columns.append((column, json_data, timestamp))
                        except ValueError:
                            continue

            if not active_columns:
                return None

            latest_column = max(active_columns, key=lambda x: x[2])
            return latest_column[0], latest_column[1]

        except psycopg2.Error as e:
            print(f"Database error for {machine} on source database: {e}")
            if self.source_db is not None:
                self.source_db.close()
                self.source_db = None
            return None
        finally:
            cursor.close()

    def check_prediction_point(self, machine: str, point_name: str) -> Dict[str, str]:
        columns = self.prediction_points.get(point_name, [])
        if not columns:
            return {"status": "ok", "description": "OK"}

        latest_active = self.get_latest_active_column(machine, columns)
        if latest_active:
            column_name, json_data = latest_active
            description = self.prediction_json.get(column_name, "Unknown issue detected")
            return {"status": "fault", "description": description}
        else:
            return {"status": "ok", "description": "OK"}

    def get_machine_prediction_overview(self, machine: str) -> Dict:
        overview = {}
        for point_name in self.prediction_points.keys():
            overview[point_name] = self.check_prediction_point(machine, point_name)
        return overview

    def get_all_machines_prediction_overview(self) -> Dict:
        all_machines_overview = {}
        for machine in self.machines:
            print(f"Checking {machine}...")
            all_machines_overview[machine] = self.get_machine_prediction_overview(machine)
        return all_machines_overview

    def update_prediction_overview_table(self, all_machines_overview: Dict):
        if not self.check_db_connection():
            print("Cannot update prediction_overview: No valid database connection")
            return

        cursor = self.target_db.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM prediction_overview")
            count = cursor.fetchone()[0]

            if count > 0:
                for machine, overview in all_machines_overview.items():
                    overview_json = json.dumps(overview)
                    update_query = f"UPDATE prediction_overview SET {machine} = %s"
                    cursor.execute(update_query, (overview_json,))
            else:
                columns = ", ".join(self.machines)
                placeholders = ", ".join(["%s"] * len(self.machines))
                values = [json.dumps(all_machines_overview[machine]) for machine in self.machines]
                insert_query = f"INSERT INTO prediction_overview ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, values)

            self.target_db.commit()
            print(f"Updated prediction overview in target database at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        except psycopg2.Error as e:
            print(f"Error updating prediction_overview table in target database: {e}")
            self.target_db.rollback()
            if self.target_db is not None:
                self.target_db.close()
                self.target_db = None
        finally:
            cursor.close()

    def monitor_continuously(self, interval_seconds: int = 5):
        print(f"Starting continuous monitoring for all machines with {interval_seconds} second intervals...")
        print(f"Monitoring machines: {', '.join(self.machines)}")
        print("-" * 80)

        while True:
            try:
                start_time = time.time()
                all_machines_overview = self.get_all_machines_prediction_overview()
                self.update_prediction_overview_table(all_machines_overview)

                print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Current Status:")
                for machine, overview in all_machines_overview.items():
                    print(f"\n{machine.upper()}:")
                    for point, status in overview.items():
                        status_symbol = "fault" if status['status'] == 'fault' else "OK"
                        print(f"  {status_symbol} {point}: {status['status']} - {status['description']}")

                processing_time = time.time() - start_time
                print(f"\nProcessing completed in {processing_time:.2f} seconds")
                print("=" * 80)
                time.sleep(interval_seconds)

            except psycopg2.OperationalError as e:
                print(f"Database error in monitoring loop: {e}")
                if self.source_db is not None and self.source_db.closed:
                    self.check_db_connection()
                if self.target_db is not None and self.target_db.closed:
                    self.check_db_connection()
                print(f"Retrying in {interval_seconds} seconds...")
                time.sleep(interval_seconds)
            except KeyboardInterrupt:
                print("\n\nMonitoring stopped by user")
                break
            except Exception as e:
                print(f"Critical error in monitoring loop: {e}")
                if self.source_db is not None and not self.source_db.closed:
                    self.source_db.close()
                if self.target_db is not None and not self.target_db.closed:
                    self.target_db.close()
                sys.exit(1)

    def single_check(self):
        all_machines_overview = self.get_all_machines_prediction_overview()
        self.update_prediction_overview_table(all_machines_overview)
        return all_machines_overview

    def check_table_exists(self, table_name: str, db_type: str = "source") -> bool:
        if not self.check_db_connection():
            print(f"Cannot check table {table_name}: No valid database connection")
            return False

        db = self.source_db if db_type == "source" else self.target_db
        cursor = db.cursor()
        try:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name,))
            return cursor.fetchone()[0]
        except psycopg2.Error as e:
            print(f"Error checking table {table_name} in {db_type} database: {e}")
            if db is not None:
                db.close()
                if db_type == "source":
                    self.source_db = None
                else:
                    self.target_db = None
            return False
        finally:
            cursor.close()

    def validate_tables(self):
        missing_tables = []
        # Check source database for machine tables
        for machine in self.machines:
            table_name = f"{machine}_tp_status"
            if not self.check_table_exists(table_name, db_type="source"):
                missing_tables.append(f"{table_name} (source)")

        # Check target database for prediction_overview table
        if not self.check_table_exists("prediction_overview", db_type="target"):
            missing_tables.append("prediction_overview (target)")

        if missing_tables:
            print(f"Warning: Missing tables: {', '.join(missing_tables)}")
            return False

        print("All required tables found!")
        return True

def load_config(config_file: str = "prediction_config.json") -> Dict:
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as file:
                config = json.load(file)
                print(f"Successfully loaded configuration from {config_file}")
                return config
        else:
            print(f"Error: Configuration file {config_file} not found!")
            return {}
    except json.JSONDecodeError as e:
        print(f"Error parsing configuration file {config_file}: {e}")
        return {}
    except Exception as e:
        print(f"Error loading configuration file {config_file}: {e}")
        return {}

def main():
    config = load_config("prediction_config.json")
    if not config:
        print("Failed to load configuration. Exiting...")
        sys.exit(1)

    source_db_config = config.get("source_database", {})
    target_db_config = config.get("target_database", {})
    if not source_db_config or not target_db_config:
        print("Source or target database configuration not found in prediction_config.json. Exiting...")
        sys.exit(1)

    SOURCE_DB_CONFIG = {
        'host': source_db_config.get('host', '100.103.195.124'),
        'database': source_db_config.get('dbname', 'hul'),
        'user': source_db_config.get('user', 'postgres'),
        'password': source_db_config.get('password', ''),
        'port': int(source_db_config.get('port', 5432)),
        'keepalives': 1,
        'keepalives_idle': 30,
        'keepalives_interval': 10,
        'keepalives_count': 5
    }

    TARGET_DB_CONFIG = {
        'host': target_db_config.get('host', 'localhost'),
        'database': target_db_config.get('dbname', 'hul'),
        'user': target_db_config.get('user', 'postgres'),
        'password': target_db_config.get('password', ''),
        'port': int(target_db_config.get('port', 5432)),
        'keepalives': 1,
        'keepalives_idle': 30,
        'keepalives_interval': 10,
        'keepalives_count': 5
    }

    print(f"Connecting to source database: {SOURCE_DB_CONFIG['database']} at {SOURCE_DB_CONFIG['host']}:{SOURCE_DB_CONFIG['port']}")
    print(f"Connecting to target database: {TARGET_DB_CONFIG['database']} at {TARGET_DB_CONFIG['host']}:{TARGET_DB_CONFIG['port']}")

    monitor = None
    try:
        monitor = MultiMachinePredictionMonitor(SOURCE_DB_CONFIG, TARGET_DB_CONFIG, "prediction.json")
        if not monitor.validate_tables():
            print("Please ensure all required tables exist before running the monitor.")
            sys.exit(1)

        print("Performing single check for all machines...")
        result = monitor.single_check()
        print("Result:", json.dumps(result, indent=2))

        print("\nStarting continuous monitoring...")
        monitor.monitor_continuously(interval_seconds=5)

    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)
    finally:
        if monitor:
            if monitor.source_db is not None and not monitor.source_db.closed:
                monitor.source_db.close()
                print("Source database connection closed")
            if monitor.target_db is not None and not monitor.target_db.closed:
                monitor.target_db.close()
                print("Target database connection closed")

if __name__ == "__main__":
    main()
