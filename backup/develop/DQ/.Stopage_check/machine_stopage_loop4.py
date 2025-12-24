import psycopg2
from psycopg2 import extras
from datetime import datetime
import pytz
import time
import os
import json
import logging
import sys
from typing import Dict, List, Optional
from dataclasses import dataclass
from pathlib import Path


# -------------------------------------------------
#  SETUP: Daily dated logger → machine_stopage_loop4.log
# -------------------------------------------------
def setup_dated_logger() -> logging.Logger:
    """
    Creates a logger that writes to:
        /home/ai4m/develop/ui_backend/logs/<YYYY-MM-DD>/machine_stopage_loop4.log
    Creates the directory if it doesn't exist.
    """
    log_root = Path("/home/ai4m/develop/ui_backend/logs")
    today_str = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d")
    log_dir = log_root / today_str
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "machine_stopage_loop4.log"

    logger = logging.getLogger("MachineStopageLoop4")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    # File handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Console handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


# Initialize the dated logger
log = setup_dated_logger()

# Monkey-patch root logging so existing logging.xxx() calls work
logging.debug = log.debug
logging.info = log.info
logging.warning = log.warning
logging.error = log.error
logging.critical = log.critical
logging.exception = log.exception
logging.log = log.log


# ---------------- IST Utility ---------------------
def get_ist_now():
    tz = pytz.timezone('Asia/Kolkata')
    return datetime.now(tz).replace(microsecond=0)


@dataclass
class DatabaseConfig:
    dbname: str
    user: str
    password: str
    host: str
    port: str

    def as_dict(self) -> dict:
        return {
            'dbname': self.dbname,
            'user': self.user,
            'password': self.password,
            'host': self.host,
            'port': self.port,
            'connect_timeout': 5
        }


class FaultCodeManager:
    def __init__(self, fault_codes_directory: str):
        self.fault_codes_directory = fault_codes_directory
        self.fault_messages = {
            'mc25': {},
            'mc27': {},
            'mc30': {}
        }
        self.machine_to_fault_file = {
            'mc25': 'mc25',
            'mc26': 'mc25',
            'mc27': 'mc27',
            'mc30': 'mc30'
        }
        self.load_all_fault_codes()

    def load_all_fault_codes(self) -> None:
        for fault_file in set(self.machine_to_fault_file.values()):
            self.load_fault_codes(fault_file)

    def load_fault_codes(self, fault_file: str) -> None:
        try:
            file_path = os.path.join(self.fault_codes_directory, f"{fault_file}.json")
            with open(file_path, 'r') as f:
                fault_data = json.load(f)
                self.fault_messages[fault_file] = {
                    item['fault_code']: item['message']
                    for item in fault_data['faults']
                }
                logging.info(f"Loaded {len(self.fault_messages[fault_file])} fault codes from {file_path} successfully")
        except FileNotFoundError:
            logging.error(f"Fault codes file not found: {file_path}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing fault codes JSON from {file_path}: {str(e)}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"Unexpected error loading fault codes from {file_path}: {str(e)}")
            sys.exit(1)

    def get_message(self, machine: str, code: int) -> str:
        try:
            code = int(code)
            machine = machine.lower()
            fault_file = self.machine_to_fault_file.get(machine)
            if not fault_file or fault_file not in self.fault_messages:
                return f"Unknown machine: {machine}, Fault code: {code}"
            message = self.fault_messages[fault_file].get(code, f"Unknown fault code: {code} for machine {machine}")
            logging.debug(f"Machine: {machine}, Using fault file: {fault_file}, Fault code: {code}, Message: {message}")
            print(f"Machine: {machine}, Using fault file: {fault_file}, Fault code: {code}, Message: {message}")
            return message
        except (ValueError, TypeError):
            return f"Invalid fault code: {code} for machine {machine}"


class EventTracker:
    def __init__(self, tracker_file: str):
        self.tracker_file = tracker_file
        self.unresolved_events: Dict = {}
        self.load_events()

    def load_events(self) -> None:
        if os.path.exists(self.tracker_file):
            try:
                with open(self.tracker_file, 'r') as f:
                    self.unresolved_events = json.load(f)
            except Exception as e:
                logging.error(f"Error loading event tracker: {str(e)}")
                sys.exit(1)
        else:
            logging.warning(f"Tracker file {self.tracker_file} does not exist, starting with empty events")
            self.unresolved_events = {}

    def save_events(self) -> None:
        try:
            with open(self.tracker_file, 'w') as f:
                json.dump(self.unresolved_events, f, indent=4)
        except Exception as e:
            logging.error(f"Error saving event tracker: {str(e)}")
            sys.exit(1)


class DualDatabaseManager:
    def __init__(self, source_config: DatabaseConfig):
        self.source_config = source_config
        self.source_conn = None
        self.connect()

    def connect(self):
        try:
            self.source_conn = psycopg2.connect(**self.source_config.as_dict())
            logging.info("Database connections established successfully")
        except Exception as e:
            logging.error(f"Failed to establish database connections: {str(e)}")
            sys.exit(1)

    def close(self):
        if self.source_conn and not self.source_conn.closed:
            self.source_conn.close()
        logging.info("Database connections closed")

    def check_connections(self):
        try:
            with self.source_conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except Exception as e:
            logging.error(f"Connection check failed: {str(e)}")
            return False

    def reconnect(self):
        self.close()
        try:
            self.connect()
        except Exception as e:
            logging.error(f"Reconnection failed: {str(e)}")
            sys.exit(1)

    def execute_source_query(self, query: str, params: tuple = None) -> Optional[List]:
        try:
            if self.source_conn.closed:
                logging.warning("Source connection closed, attempting to reconnect")
                self.reconnect()
            with self.source_conn.cursor(cursor_factory=extras.DictCursor) as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    return cursor.fetchall()
                return None
        except Exception as e:
            logging.error(f"Source database error: {str(e)}")
            return None


class MachineMonitor:
    def __init__(self, source_config: DatabaseConfig, fault_codes_directory: str, machines: List[str]):
        self.tables = machines
        self.db = DualDatabaseManager(source_config)
        self.fault_manager = FaultCodeManager(fault_codes_directory)
        self.event_tracker = EventTracker('loop4.json')

    def generate_event_id(self, machine: str, status: int, ist_timestamp: datetime) -> str:
        time_str = ist_timestamp.strftime("%Y%m%d_%H%M%S")
        return f"{machine.upper()}_{time_str}_{status}"

    def insert_event(self, machine: str, status: int, fault: int) -> bool:
        if machine in self.event_tracker.unresolved_events:
            return False
        ist_now = get_ist_now()
        event_id = self.generate_event_id(machine, status, ist_now)
        machine_name = machine.lower()
        fault_message = self.fault_manager.get_message(machine_name, int(fault))
        query = """
            INSERT INTO event_table
            (timestamp, event_id, zone, camera_id, event_type, alert_type)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            ist_now,
            event_id,
            "PLC",
            machine,
            fault_message,
            "Breakdown"
        )
        logging.info(f"Inserting event: timestamp={ist_now}, event_id={event_id}, camera_id={machine}, event_type={fault_message}, alert_type=Breakdown")
        try:
            self.db.execute_source_query(query, params)
            self.event_tracker.unresolved_events[machine] = {
                "event_id": event_id,
                "timestamp": ist_now.isoformat()
            }
            self.event_tracker.save_events()
            logging.info(f"Machine {machine} stopped - Fault Code: {fault}")
            logging.info(f"Issue: {fault_message}")
            return True
        except psycopg2.Error as e:
            logging.error(f"Failed to insert event for machine {machine}: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error inserting event for machine {machine}: {e}")
            return False

    def resolve_event(self, machine: str) -> bool:
        if machine not in self.event_tracker.unresolved_events:
            return False
        event_id = self.event_tracker.unresolved_events[machine]["event_id"]
        ist_now = get_ist_now()
        query = """
            UPDATE event_table
            SET resolution_time = %s
            WHERE event_id = %s
        """
        logging.info(f"Updating event data: event_id = {event_id}, resolution_time = {ist_now}")
        try:
            self.db.execute_source_query(query, (ist_now, event_id))
            prev_time = datetime.fromisoformat(
                self.event_tracker.unresolved_events[machine]["timestamp"]
            )
            downtime = ist_now - prev_time
            logging.info(f"Machine {machine} resumed - Downtime: {downtime}")
            del self.event_tracker.unresolved_events[machine]
            self.event_tracker.save_events()
            return True
        except psycopg2.Error as e:
            logging.error(f"Failed to update event for machine {machine}: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error updating event for machine {machine}: {e}")
            return False

    def get_current_statuses(self) -> Optional[List]:
        query = """
        WITH latest_status AS (
            {}
        )
        SELECT * FROM latest_status
        ORDER BY timestamp DESC;
        """.format(
            " UNION ALL ".join(
                f"""
                SELECT
                    '{table}' as table_name,
                    status,
                    fault,
                    timestamp
                FROM {table}
                WHERE timestamp = (
                    SELECT MAX(timestamp)
                    FROM {table}
                )
                AND status IS NOT NULL
                AND fault IS NOT NULL
                """
                for table in self.tables
            )
        )
        return self.db.execute_source_query(query)

    def monitor_changes(self) -> None:
        previous_statuses = {}
        first_run = True
        logging.info("Starting machine monitoring...")
        while True:
            try:
                if not self.db.check_connections():
                    logging.warning("Database connection issues detected")
                    self.db.reconnect()
                current_statuses = self.get_current_statuses()
                if current_statuses:
                    for row in current_statuses:
                        machine = row['table_name'].upper()
                        status = row['status']
                        fault = row['fault']
                        # IGNORE row['timestamp'] - always use IST time for events below
                        if first_run:
                            if status != 1 and machine not in self.event_tracker.unresolved_events:
                                self.insert_event(machine, status, fault)
                        if machine in previous_statuses:
                            prev_status = previous_statuses[machine]
                            if prev_status == 1 and status != 1:
                                self.insert_event(machine, status, fault)
                            elif prev_status != 1 and status == 1:
                                self.resolve_event(machine)
                        previous_statuses[machine] = status
                    first_run = False
                time.sleep(6)
            except KeyboardInterrupt:
                logging.info("Monitoring stopped by user")
                break
            except Exception as e:
                logging.error(f"Error in monitoring loop: {str(e)}")
                time.sleep(6)


def load_config(config_path: str) -> Dict:
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing config JSON: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error loading config: {e}")
        sys.exit(1)


def main():
    try:
        config_path = 'config.json'
        config = load_config(config_path)
        source_config = DatabaseConfig(
            dbname=config['source_database']['dbname'],
            user=config['source_database']['user'],
            password=config['source_database']['password'],
            host=config['source_database']['host'],
            port=config['source_database']['port']
        )
        machines = config['loop4_machines']
        print("\n Machines = ", machines)
        fault_codes_directory = '/home/ai4m/develop/backup/develop/DQ/Stopage_check'
    except KeyError as e:
        logging.error(f"Missing configuration key: {e}")
        sys.exit(1)

    if not os.path.exists(fault_codes_directory):
        try:
            os.makedirs(fault_codes_directory)
            logging.info(f"Created fault codes directory: {fault_codes_directory}")
        except Exception as e:
            logging.error(f"Failed to create fault codes directory: {str(e)}")
            sys.exit(1)

    monitor = None
    max_retries = 5
    retry_delay = 10
    for attempt in range(max_retries):
        try:
            monitor = MachineMonitor(source_config, fault_codes_directory, machines)
            monitor.monitor_changes()
            break
        except Exception as e:
            logging.error(f"Program error (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error("Max retries reached. Exiting.")
                sys.exit(1)
        finally:
            if monitor:
                monitor.db.close()


if __name__ == "__main__":
    main()
