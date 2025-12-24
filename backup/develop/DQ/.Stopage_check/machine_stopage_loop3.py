import psycopg2
from psycopg2 import extras
from datetime import datetime
import pytz
import time
import os
import json
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from pathlib import Path
import sys

def setup_dated_logger() -> logging.Logger:
    """
    Returns a logger that writes to:
        /home/ai4m/develop/ui_backend/logs/<YYYY-MM-DD>/machine_stopage_loop3.log
    Directory is created if it does not exist.
    """
    log_root = Path("/home/ai4m/develop/ui_backend/logs")
    today_str = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d")
    log_dir = log_root / today_str
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "machine_stopage_loop3.log"

    logger = logging.getLogger("MachineStopageLoop3")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(file_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


log = setup_dated_logger()

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
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 5
        }


class FaultCodeManager:
    def __init__(self, fault_codes_path: str):
        self.fault_codes_path = fault_codes_path
        self.fault_messages = {}
        self.load_fault_codes()

    def load_fault_codes(self) -> None:
        try:
            with open(self.fault_codes_path, 'r') as f:
                fault_data = json.load(f)
                self.fault_messages = {
                    int(item['fault_code']): item['message']
                    for item in fault_data['faults']
                }
                logging.info(f"Loaded {len(self.fault_messages)} fault codes successfully")
        except FileNotFoundError:
            logging.error(f"Fault codes file not found: {self.fault_codes_path}")
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing fault codes JSON: {e}")
        except Exception as e:
            logging.error(f"Unexpected error loading fault codes: {e}")

    def get_message(self, code: int) -> str:
        try:
            code = int(code)
            message = self.fault_messages.get(code, f"Unknown fault code: {code}")
            logging.debug(f"Fault code: {code}, Message: {message}")
            print(f"Fault code: {code}, Message: {message}")
            return message
        except (ValueError, TypeError):
            return f"Invalid fault code: {code}"


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
                logging.error(f"Error loading event tracker: {e}")
                self.unresolved_events = {}

    def save_events(self) -> None:
        try:
            with open(self.tracker_file, 'w') as f:
                json.dump(self.unresolved_events, f, indent=4)
        except Exception as e:
            logging.error(f"Error saving event tracker: {e}")


class DualDatabaseManager:
    def __init__(self, source_config: DatabaseConfig):
        self.source_config = source_config
        self.source_conn = None
        self.connections_closed = False
        self.initialize_connections()

    def initialize_connections(self, max_retries=5, initial_delay=5):
        for attempt in range(max_retries):
            try:
                self.source_conn = psycopg2.connect(**self.source_config.as_dict())
                logging.info("Database connections established successfully")
                return
            except psycopg2.Error as e:
                logging.error(f"Failed to connect (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    logging.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logging.error("Max retries reached for database connections")
                    self.close_connections()
                    sys.exit(1)

    def close_connections(self):
        if self.connections_closed:
            return
        self.connections_closed = True
        try:
            if self.source_conn and not self.source_conn.closed:
                self.source_conn.close()
                logging.info("Database source connection closed")
        except Exception as e:
            logging.error(f"Error closing connections: {e}")

    def check_and_reconnect(self, connection, config, retries=3, initial_delay=5):
        if connection is None or connection.closed:
            logging.warning(f"Connection to {config['dbname']} is None or closed, attempting to reconnect")
        else:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                return connection
            except psycopg2.Error:
                logging.warning(f"Connection check failed for {config['dbname']}, attempting to reconnect")
                try:
                    connection.close()
                except:
                    pass
        for attempt in range(retries):
            try:
                new_conn = psycopg2.connect(**config.as_dict())
                logging.info(f"Successfully reconnected to {config['dbname']}")
                return new_conn
            except psycopg2.Error as e:
                if attempt < retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    logging.error(f"Reconnect attempt {attempt + 1}/{retries} failed for {config['dbname']}: {e}. Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logging.error(f"Failed to reconnect to {config['dbname']} after {retries} attempts: {e}")
                    raise

    def execute_source_query(self, query: str, params: tuple = None) -> Optional[List]:
        try:
            self.source_conn = self.check_and_reconnect(self.source_conn, self.source_config)
            with self.source_conn.cursor(cursor_factory=extras.DictCursor) as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    return cursor.fetchall()
                self.source_conn.commit()
                return None
        except psycopg2.Error as e:
            logging.error(f"Source database error: {e}")
            if self.source_conn is not None:
                try:
                    self.source_conn.rollback()
                    self.source_conn.close()
                except:
                    pass
                self.source_conn = None
            raise
        except Exception as e:
            logging.error(f"Unexpected source database error: {e}")
            raise


class MachineMonitor:
    def __init__(self, source_config: DatabaseConfig, fault_codes_path: str, machines: List[str]):
        self.tables = machines
        self.db = DualDatabaseManager(source_config)
        self.fault_manager = FaultCodeManager(fault_codes_path)
        self.event_tracker = EventTracker('loop3.json')

    def __del__(self):
        if hasattr(self, 'db'):
            self.db.close_connections()

    def generate_event_id(self, machine: str, status: int, timestamp: datetime) -> str:
        time_str = timestamp.strftime("%Y%m%d_%H%M%S")
        return f"{machine.upper()}_{time_str}_{status}"

    def insert_event(self, machine: str, status: int, status_code: int) -> bool:
        if machine in self.event_tracker.unresolved_events:
            return False
        ist_now = get_ist_now()
        event_id = self.generate_event_id(machine, status, ist_now)
        fault_message = self.fault_manager.get_message(int(status_code))
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
        logging.info(f"Inserting event: timestamp={ist_now}, event_id={event_id}, zone={machine}, event_type={fault_message}, alert_type=Breakdown")
        try:
            if self.db.execute_source_query(query, params) is not None:
                self.event_tracker.unresolved_events[machine] = {
                    "event_id": event_id,
                    "timestamp": ist_now.isoformat()
                }
                self.event_tracker.save_events()
                logging.info(f"Machine {machine} stopped - Fault Code: {status_code}")
                logging.info(f"Issue: {fault_message}")
                return True
        except:
            return False
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
            if self.db.execute_source_query(query, (ist_now, event_id)) is not None:
                prev_time = datetime.fromisoformat(
                    self.event_tracker.unresolved_events[machine]["timestamp"]
                )
                downtime = ist_now - prev_time
                logging.info(f"Machine {machine} resumed - Downtime: {downtime}")
                del self.event_tracker.unresolved_events[machine]
                self.event_tracker.save_events()
                return True
        except:
            return False
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
                    status_code,
                    timestamp
                FROM {table}
                WHERE timestamp = (
                    SELECT MAX(timestamp)
                    FROM {table}
                )
                """
                for table in self.tables
            )
        )
        return self.db.execute_source_query(query)

    def monitor_changes(self) -> None:
        previous_statuses = {}
        first_run = True
        consecutive_failures = 0
        max_failures = 5
        logging.info("Starting machine monitoring...")
        try:
            while True:
                try:
                    current_statuses = self.get_current_statuses()
                    if current_statuses:
                        consecutive_failures = 0
                        for row in current_statuses:
                            machine = row['table_name'].upper()
                            status = row['status']
                            status_code = row['status_code']
                            # IGNORE row['timestamp'] for event actions, always use system IST below.
                            if first_run:
                                if status != 1 and machine not in self.event_tracker.unresolved_events:
                                    self.insert_event(machine, status, status_code)
                            if machine in previous_statuses:
                                prev_status = previous_statuses[machine]
                                if prev_status == 1 and status != 1:
                                    self.insert_event(machine, status, status_code)
                                elif prev_status != 1 and status == 1:
                                    self.resolve_event(machine)
                            previous_statuses[machine] = status
                        first_run = False
                    else:
                        consecutive_failures += 1
                        logging.warning(f"Failed to fetch statuses, attempt {consecutive_failures}/{max_failures}")
                        if consecutive_failures >= max_failures:
                            logging.error(f"Reached maximum consecutive failures ({max_failures})")
                            raise RuntimeError("Persistent database failure")
                    time.sleep(6)
                except psycopg2.Error as e:
                    consecutive_failures += 1
                    logging.error(f"Database error in monitoring loop, attempt {consecutive_failures}/{max_failures}: {e}")
                    if consecutive_failures >= max_failures:
                        logging.error(f"Reached maximum consecutive failures ({max_failures})")
                        raise
                    time.sleep(10)
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Critical error in monitoring loop: {e}")
            self.db.close_connections()
            sys.exit(1)
        finally:
            self.db.close_connections()


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
    config_path = 'config.json'
    fault_codes_path = 'loop3.json'
    config = load_config(config_path)
    try:
        source_config = DatabaseConfig(
            dbname=config['source_database']['dbname'],
            user=config['source_database']['user'],
            password=config['source_database']['password'],
            host=config['source_database']['host'],
            port=config['source_database']['port']
        )
        machines = config['machines']
    except KeyError as e:
        logging.error(f"Missing configuration key: {e}")
        sys.exit(1)
    if not os.path.exists(fault_codes_path):
        logging.error(f"Fault codes file not found: {fault_codes_path}")
        sys.exit(1)
    monitor = None
    try:
        monitor = MachineMonitor(source_config, fault_codes_path, machines)
        monitor.monitor_changes()
    except psycopg2.Error as e:
        logging.error(f"Database initialization error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Program error: {e}")
        sys.exit(1)
    finally:
        if monitor:
            monitor.db.close_connections()


if __name__ == "__main__":
    main()
