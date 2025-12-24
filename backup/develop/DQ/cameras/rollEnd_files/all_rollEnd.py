import psycopg2
from datetime import datetime, timedelta
from uuid import uuid4
import json
import time
import logging
import threading
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)s] - %(levelname)s - %(message)s'
)

class MachineRollEndMonitor:
    """Monitor roll end sensor for a single machine - each instance has dedicated DB connections."""

    def __init__(self, machine_id: str, config: Dict[str, Any], db_config: Dict):
        self.machine_id = machine_id
        self.machine_config = config
        self.db_config = db_config

        # Each machine gets its own dedicated database connections (thread-safe)
        self.read_conn = None
        self.read_cursor = None
        self.write_conn = None
        self.write_cursor = None
        self.connect_databases()

        # Build queries specific to this machine
        self.fetch_mq_query = f"SELECT timestamp, status FROM {self.machine_config['status_table']} ORDER BY timestamp DESC LIMIT 1;"
        self.fetch_rq_query = f'''
            SELECT timestamp, {self.machine_config['checkpoint_column']} ->> '{self.machine_config['sensor_key']}' AS roll_end
            FROM {self.machine_config['checkpoint_table']}
            WHERE {self.machine_config['checkpoint_column']} ->> '{self.machine_config['sensor_key']}' IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 2;
        '''
        self.insert_event_query = """INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s);"""

        # State tracking for this machine
        self.roll_end_triggered = False
        self.rq_true_start_time = None
        self.previous_new_roll_running = None
        self.previous_rq = None
        self.previous_is_problem = None
        self.new_roll_added = False
        self.first_run = True

        logging.info(f"[{self.machine_id}] Monitor initialized")

    def connect_databases(self):
        """Initialize or reconnect to both read and write databases."""
        self.close_databases()

        # Connect to read database (localhost)
        try:
            self.read_conn = psycopg2.connect(**self.db_config["read_db"])
            self.read_cursor = self.read_conn.cursor()
            logging.info(f"[{self.machine_id}] Successfully connected to read database")
        except psycopg2.Error as e:
            logging.error(f"[{self.machine_id}] Failed to connect to read database: {e}")
            raise

        # Connect to write database (remote)
        try:
            self.write_conn = psycopg2.connect(**self.db_config["write_db"])
            self.write_cursor = self.write_conn.cursor()
            logging.info(f"[{self.machine_id}] Successfully connected to write database")
        except psycopg2.Error as e:
            logging.error(f"[{self.machine_id}] Failed to connect to write database: {e}")
            raise

    def ensure_db_connection(self, conn, cursor, db_type):
        """Ensure the database connection and cursor are valid; reconnect if necessary."""
        try:
            if conn is None or conn.closed != 0 or cursor is None or cursor.closed:
                logging.info(f"[{self.machine_id}] {db_type} database connection lost. Reconnecting...")
                if db_type == "read":
                    self.read_conn = psycopg2.connect(**self.db_config["read_db"])
                    self.read_cursor = self.read_conn.cursor()
                    logging.info(f"[{self.machine_id}] Reconnected to read database")
                else:
                    self.write_conn = psycopg2.connect(**self.db_config["write_db"])
                    self.write_cursor = self.write_conn.cursor()
                    logging.info(f"[{self.machine_id}] Reconnected to write database")
                return True
            # Test connection with a simple query
            cursor.execute("SELECT 1")
            return True
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            logging.error(f"[{self.machine_id}] {db_type} database connection error: {e}")
            if db_type == "read":
                self.read_conn = psycopg2.connect(**self.db_config["read_db"])
                self.read_cursor = self.read_conn.cursor()
                logging.info(f"[{self.machine_id}] Reconnected to read database")
            else:
                self.write_conn = psycopg2.connect(**self.db_config["write_db"])
                self.write_cursor = self.write_conn.cursor()
                logging.info(f"[{self.machine_id}] Reconnected to write database")
            return True
        except psycopg2.Error as e:
            logging.error(f"[{self.machine_id}] {db_type} database error during connection check: {e}")
            return False

    def close_databases(self):
        """Close both database connections and cursors."""
        try:
            if hasattr(self, 'read_cursor') and self.read_cursor:
                self.read_cursor.close()
            if hasattr(self, 'read_conn') and self.read_conn:
                self.read_conn.close()
            if hasattr(self, 'write_cursor') and self.write_cursor:
                self.write_cursor.close()
            if hasattr(self, 'write_conn') and self.write_conn:
                self.write_conn.close()
            logging.info(f"[{self.machine_id}] Database connections closed")
        except psycopg2.Error as e:
            logging.error(f"[{self.machine_id}] Error closing database connections: {e}")

    @staticmethod
    def format_event_data(is_problem):
        """Format event data for TP status update."""
        timestamp = datetime.now().isoformat()
        return {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": timestamp,
            "color_code": 3 if is_problem else 1
        }

    def insert_or_update_tp(self, column, event_data):
        """Insert or update TP status for this machine."""
        check_query = f"SELECT {column} FROM {self.machine_config['tp_status_table']};"
        update_query = f"UPDATE {self.machine_config['tp_status_table']} SET {column} = %s;"
        insert_query = f"INSERT INTO {self.machine_config['tp_status_table']} ({column}) VALUES (%s);"

        try:
            if not self.ensure_db_connection(self.write_conn, self.write_cursor, "write"):
                raise psycopg2.Error("Write database connection failed")

            self.write_cursor.execute(check_query)
            exists = self.write_cursor.fetchone()
            if exists:
                logging.debug(f"[{self.machine_id}] Updating {column} with event data: {event_data}")
                self.write_cursor.execute(update_query, (json.dumps(event_data),))
            else:
                logging.info(f"[{self.machine_id}] Inserting new {column} with event data: {event_data}")
                self.write_cursor.execute(insert_query, (json.dumps(event_data),))
            self.write_conn.commit()
        except psycopg2.Error as e:
            logging.error(f"[{self.machine_id}] Error updating/inserting {column}: {e}")
            self.write_conn.rollback()

    def monitor(self):
        """Main monitoring loop for this machine."""
        logging.info(f"[{self.machine_id}] Starting roll end monitoring")

        while True:
            try:
                # Ensure database connections
                if not self.ensure_db_connection(self.read_conn, self.read_cursor, "read"):
                    logging.error(f"[{self.machine_id}] Read database connection failed, retrying in 5 seconds")
                    time.sleep(5)
                    continue
                if not self.ensure_db_connection(self.write_conn, self.write_cursor, "write"):
                    logging.error(f"[{self.machine_id}] Write database connection failed, retrying in 5 seconds")
                    time.sleep(5)
                    continue

                # Fetch machine status (MQ)
                self.read_cursor.execute(self.fetch_mq_query)
                mq_row = self.read_cursor.fetchone()
                mq = int(mq_row[1]) if mq_row and mq_row[1] is not None else None

                # Fetch roll end sensor data (RQ)
                self.read_cursor.execute(self.fetch_rq_query)
                rq_rows = self.read_cursor.fetchall()
                logging.debug(f"[{self.machine_id}] RQ rows fetched: {rq_rows}")

                rq = None
                if rq_rows and len(rq_rows) > 0 and rq_rows[0][1] is not None:
                    rq = 1 if rq_rows[0][1].lower() == 'true' else 0
                else:
                    self.previous_rq = None

                logging.info(f"[{self.machine_id}] MQ={mq}, RQ={rq}, Previous RQ={self.previous_rq}, Roll End Triggered={self.roll_end_triggered}, New Roll Added={self.new_roll_added}, First Run={self.first_run}")

                # Skip processing if mq or rq is None
                if mq is None or rq is None:
                    logging.warning(f"[{self.machine_id}] MQ or RQ is None, skipping iteration")
                    self.previous_rq = rq
                    time.sleep(1)
                    self.first_run = False
                    continue

                # Detect "roll end" condition
                is_problem = False
                if mq == 1 and rq == 0 and self.previous_rq == 1:
                    logging.info(f"[{self.machine_id}] Roll end detected: mq={mq}, rq={rq}, previous_rq={self.previous_rq}")
                    is_problem = True
                    self.roll_end_triggered = True
                    self.new_roll_added = False
                    logging.warning(f"[{self.machine_id}] Roll end condition detected")

                # Event for roll ending
                if is_problem and is_problem != self.previous_is_problem:
                    event_data = self.format_event_data(is_problem=True)
                    data = (
                        datetime.now(), str(uuid4()), self.machine_config['event_zone'],
                        self.machine_config['machine_id'], self.machine_config['event_roll_end'],
                        self.machine_config['alert_type']
                    )
                    logging.info(f"[{self.machine_id}] Inserting roll-end event: {data}")
                    self.read_cursor.execute(self.insert_event_query, data)
                    self.read_conn.commit()
                    self.insert_or_update_tp(self.machine_config['tp_roll_end'], event_data)
                    # Set active=0 for new roll TP
                    tp_new_roll_event_data = self.format_event_data(is_problem=False)
                    self.insert_or_update_tp(self.machine_config['tp_new_roll'], tp_new_roll_event_data)

                # Detect "new roll running" condition
                new_roll_running = self.new_roll_added and mq == 1 and rq == 1
                if new_roll_running and new_roll_running != self.previous_new_roll_running:
                    data = (
                        datetime.now(), str(uuid4()), self.machine_config['event_zone'],
                        self.machine_config['machine_id'], self.machine_config['event_new_roll_running'],
                        self.machine_config['alert_type']
                    )
                    logging.info(f"[{self.machine_id}] New roll running: {data}")
                    # Uncomment if you want to insert this event
                    # self.read_cursor.execute(self.insert_event_query, data)
                    # self.read_conn.commit()

                # Check for new laminate roll condition
                wait_minutes = self.machine_config.get('new_roll_wait_minutes', 3)
                if (self.roll_end_triggered or (mq != 1 and rq != 1)) and not self.new_roll_added and mq != 1 and rq == 1:
                    if self.rq_true_start_time is None:
                        self.rq_true_start_time = datetime.now()
                        logging.info(f"[{self.machine_id}] Started tracking MQ!=1, RQ=1 for new laminate roll...")

                    elif datetime.now() - self.rq_true_start_time >= timedelta(minutes=wait_minutes):
                        event_data = self.format_event_data(is_problem=True)
                        data = (
                            datetime.now(), str(uuid4()), self.machine_config['event_zone'],
                            self.machine_config['machine_id'], self.machine_config['event_new_roll'],
                            self.machine_config['alert_type']
                        )
                        logging.info(f"[{self.machine_id}] Inserting new roll event: {data}")
                        self.read_cursor.execute(self.insert_event_query, data)
                        self.read_conn.commit()
                        self.insert_or_update_tp(self.machine_config['tp_new_roll'], event_data)
                        # Set active=0 for roll end TP
                        tp_roll_end_event_data = self.format_event_data(is_problem=False)
                        self.insert_or_update_tp(self.machine_config['tp_roll_end'], tp_roll_end_event_data)

                        self.roll_end_triggered = False
                        self.rq_true_start_time = None
                        self.new_roll_added = True
                        logging.info(f"[{self.machine_id}] Reset roll end trigger and set new_roll_added=True")

                elif not (mq != 1 and rq == 1):
                    self.rq_true_start_time = None
                    logging.debug(f"[{self.machine_id}] Reset rq_true_start_time due to condition break")

                self.previous_is_problem = is_problem
                self.previous_new_roll_running = new_roll_running
                self.previous_rq = rq
                self.first_run = False
                time.sleep(1)

            except psycopg2.Error as e:
                logging.error(f"[{self.machine_id}] Database error: {e}")
                try:
                    self.read_conn.rollback()
                    self.write_conn.rollback()
                except:
                    pass
                time.sleep(5)  # Wait longer before retrying
            except Exception as e:
                logging.error(f"[{self.machine_id}] Unexpected error: {e}")
                time.sleep(5)

    def __del__(self):
        self.close_databases()


class Loop3RollEndMonitor:
    """Main orchestrator for monitoring all Loop3 machines."""

    def __init__(self, config_file):
        try:
            with open(config_file) as f:
                content = f.read()
                logging.info(f"Config file content loaded from: {config_file}")
                self.config = json.loads(content)
        except FileNotFoundError:
            logging.error(f"Config file {config_file} not found")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in {config_file}: {e}")
            raise

        # Machine monitors
        self.machine_monitors = {}
        self.threads = []

    def run(self):
        """Start monitoring all machines in separate threads."""
        logging.info("Starting Loop3 Roll End Monitor for all machines")

        # DB config to pass to each monitor
        db_config = {
            'read_db': self.config['read_db'],
            'write_db': self.config['write_db']
        }

        # Create and start monitor threads for each machine
        for machine_id, machine_config in self.config['machines'].items():
            # Each monitor gets its own dedicated DB connections
            monitor = MachineRollEndMonitor(machine_id.upper(), machine_config, db_config)
            self.machine_monitors[machine_id] = monitor

            thread = threading.Thread(
                target=monitor.monitor,
                name=f"{machine_id.upper()}_Monitor",
                daemon=False
            )
            thread.start()
            self.threads.append(thread)
            logging.info(f"Started monitor thread for {machine_id.upper()}")

        # Wait for all threads
        try:
            for thread in self.threads:
                thread.join()
        except KeyboardInterrupt:
            logging.info("Shutting down Loop3 Roll End Monitor...")
            # Close all connections
            for monitor in self.machine_monitors.values():
                monitor.close_databases()


if __name__ == '__main__':
    monitor = Loop3RollEndMonitor("loop3_rollend_config.json")
    monitor.run()
