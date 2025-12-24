import psycopg2
from datetime import datetime
from uuid import uuid4
import json
import time
import threading
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('leaping_sensor.log'),
        logging.StreamHandler()
    ]
)

class LeapingSensorMonitor:
    """Monitors leaping sensor for a single machine"""

    def __init__(self, machine_key, machine_config, common_config, tp_db_config, event_db_config):
        self.machine_key = machine_key
        self.machine_config = machine_config
        self.common_config = common_config
        self.machine_id = machine_config['machine_id']
        self.logger = logging.getLogger(f"LeapingSensor-{self.machine_id}")

        # Database connections
        self.tp_conn = None
        self.tp_cursor = None
        self.event_conn = None
        self.event_cursor = None

        # State tracking
        self.last_active_timestamp = None
        self.previous_is_problem = None

        # Connect to databases
        self._connect_databases(tp_db_config, event_db_config)

        # Ensure touchpoint column exists
        self._ensure_tp_column_exists()

        # Build queries
        self._build_queries()

    def _connect_databases(self, tp_db_config, event_db_config):
        """Establish database connections"""
        try:
            # Touchpoint database connection
            self.tp_conn = psycopg2.connect(**tp_db_config)
            self.tp_cursor = self.tp_conn.cursor()
            self.logger.info(f"Connected to touchpoint database for {self.machine_id}")

            # Event database connection
            self.event_conn = psycopg2.connect(**event_db_config)
            self.event_cursor = self.event_conn.cursor()
            self.logger.info(f"Connected to event database for {self.machine_id}")
        except psycopg2.Error as e:
            self.logger.error(f"Database connection error for {self.machine_id}: {e}")
            raise

    def _build_queries(self):
        """Build SQL queries for this machine"""
        # Query to fetch MQ (material quality) status
        self.fetch_mq_query = f"""
            SELECT status
            FROM {self.machine_config['status_table']}
            ORDER BY timestamp DESC
            LIMIT 1;
        """

        # Query to fetch LS (leaping sensor) average
        self.fetch_ls_query = f"""
            SELECT
                CASE
                    WHEN COUNT(*) = 0 THEN NULL
                    WHEN AVG(
                        CASE
                            WHEN leaping_value = 'true' THEN 1
                            WHEN leaping_value = 'false' THEN 0
                            ELSE NULL
                        END
                    ) > {self.common_config['leaping_threshold']} THEN 1
                    ELSE 0
                END AS leaping_result
            FROM (
                SELECT {self.machine_config['checkpoint_column']} ->> '{self.common_config['sensor_key']}' AS leaping_value
                FROM {self.common_config['checkpoint_table']}
                WHERE {self.machine_config['checkpoint_column']} ->> '{self.common_config['sensor_key']}' IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT {self.common_config['leaping_sample_size']}
            ) subquery;
        """

        # Touchpoint queries
        tp_column = self.machine_config['tp_column']
        tp_table = self.machine_config['tp_status_table']

        self.check_tp_exists_query = f"SELECT {tp_column} FROM {tp_table} LIMIT 1;"
        self.update_tp_query = f"UPDATE {tp_table} SET {tp_column} = %s;"
        self.insert_tp_query = f"INSERT INTO {tp_table} ({tp_column}) VALUES (%s);"

        # Event insert query
        self.insert_event_query = """
            INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type)
            VALUES (%s, %s, %s, %s, %s, %s);
        """

    def _ensure_tp_column_exists(self):
        """Check if touchpoint column exists in the status table"""
        tp_column = self.machine_config['tp_column']
        tp_table = self.machine_config['tp_status_table']

        try:
            self.tp_cursor.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='{tp_table}' AND column_name='{tp_column}';
            """)
            column_exists = self.tp_cursor.fetchone()

            if not column_exists:
                self.logger.warning(f"Column '{tp_column}' does not exist in {tp_table}. Skipping {self.machine_id}.")
                print(f"⚠️  SKIPPING {self.machine_id}: Column '{tp_column}' not found in {tp_table}")
                raise ValueError(f"Column '{tp_column}' does not exist in {tp_table}")
            else:
                self.logger.info(f"Column '{tp_column}' exists in {tp_table}")
        except psycopg2.Error as e:
            self.logger.error(f"Error checking column '{tp_column}': {e}")
            self.tp_conn.rollback()
            raise

    def _format_event_data(self, is_problem):
        """Format event data for touchpoint update"""
        current_time = datetime.now()
        timestamp = current_time if is_problem else (self.last_active_timestamp or current_time)

        event_data = {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": timestamp.isoformat(),
            "color_code": 3 if is_problem else 1
        }

        if not is_problem and self.last_active_timestamp is not None:
            event_data["updated_time"] = current_time.isoformat()

        return event_data, timestamp

    def _fetch_sensor_data(self):
        """Fetch MQ and LS sensor data"""
        try:
            # Fetch MQ
            self.event_cursor.execute(self.fetch_mq_query)
            mq_row = self.event_cursor.fetchone()
            mq = int(mq_row[0]) if mq_row else None

            # Fetch LS
            self.event_cursor.execute(self.fetch_ls_query)
            ls_row = self.event_cursor.fetchone()
            ls = int(ls_row[0]) if ls_row and ls_row[0] is not None else None

            return mq, ls
        except psycopg2.Error as e:
            self.logger.error(f"Error fetching sensor data: {e}")
            raise

    def _insert_event(self, event_timestamp):
        """Insert event into event_table"""
        data = (
            event_timestamp,
            str(uuid4()),
            self.common_config['event_zone'],
            self.machine_id,
            self.common_config['event_type'],
            self.common_config['alert_type']
        )

        try:
            self.event_cursor.execute(self.insert_event_query, data)
            self.event_conn.commit()
            self.logger.info(f"Event inserted: {self.common_config['event_type']} for {self.machine_id}")
        except psycopg2.Error as e:
            self.logger.error(f"Error inserting event: {e}")
            self.event_conn.rollback()
            raise

    def _update_touchpoint(self, event_data):
        """Update or insert touchpoint status"""
        try:
            # Check if touchpoint row exists
            self.tp_cursor.execute(self.check_tp_exists_query)
            exists = self.tp_cursor.fetchone()

            json_data = json.dumps(event_data)

            if exists:
                self.tp_cursor.execute(self.update_tp_query, (json_data,))
                self.logger.info(f"Updated {self.machine_config['tp_column']} for {self.machine_id}")
            else:
                self.tp_cursor.execute(self.insert_tp_query, (json_data,))
                self.logger.info(f"Inserted {self.machine_config['tp_column']} for {self.machine_id}")

            self.tp_conn.commit()
        except psycopg2.Error as e:
            self.logger.error(f"Error updating touchpoint: {e}")
            self.tp_conn.rollback()
            raise

    def run(self):
        """Main monitoring loop"""
        self.logger.info(f"Started leaping sensor monitoring for {self.machine_id}")

        while True:
            try:
                # Fetch sensor data
                mq, ls = self._fetch_sensor_data()
                self.logger.info(f"{self.machine_id}: mq={mq}, ls={ls}")

                # Skip if no sensor data available
                if mq is None or ls is None:
                    self.logger.debug(f"{self.machine_id}: No sensor data available (mq={mq}, ls={ls}), skipping cycle")
                    time.sleep(self.common_config['poll_interval'])
                    continue

                # Determine problem state
                if mq == 1 and ls == 0:
                    is_problem = True
                elif mq == 1 and ls == 1:
                    is_problem = False
                else:
                    is_problem = False

                # Only update if state changed
                if is_problem is not None and is_problem != self.previous_is_problem:
                    # Update timestamp when problem detected
                    if is_problem:
                        self.last_active_timestamp = datetime.now()

                    # Format event data
                    event_data, event_timestamp = self._format_event_data(is_problem)

                    # Insert event if problem detected
                    if is_problem:
                        self._insert_event(event_timestamp)

                    # Update touchpoint
                    self._update_touchpoint(event_data)

                    # Update state
                    self.previous_is_problem = is_problem

                time.sleep(self.common_config['poll_interval'])

            except psycopg2.Error as db_err:
                self.logger.error(f"Database error: {db_err}")
                self.event_conn.rollback()
                self.tp_conn.rollback()
                time.sleep(self.common_config['poll_interval'])
            except Exception as ex:
                self.logger.error(f"Unexpected error: {ex}", exc_info=True)
                time.sleep(self.common_config['poll_interval'])

    def cleanup(self):
        """Close database connections"""
        if self.tp_cursor:
            self.tp_cursor.close()
        if self.tp_conn:
            self.tp_conn.close()
        if self.event_cursor:
            self.event_cursor.close()
        if self.event_conn:
            self.event_conn.close()
        self.logger.info(f"Database connections closed for {self.machine_id}")


class LeapingSensorManager:
    """Manages leaping sensor monitoring for all machines"""

    def __init__(self, config_path='leaping_config.json'):
        self.logger = logging.getLogger("LeapingSensorManager")
        self.config = self._load_config(config_path)
        self.monitors = []
        self.threads = []

    def _load_config(self, config_path):
        """Load configuration from JSON file"""
        try:
            config_file = Path(__file__).parent / config_path
            with open(config_file, 'r') as f:
                config = json.load(f)
            self.logger.info(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            raise

    def _create_monitor(self, machine_key, machine_config):
        """Create a monitor instance for a machine"""
        try:
            monitor = LeapingSensorMonitor(
                machine_key=machine_key,
                machine_config=machine_config,
                common_config=self.config['common_settings'],
                tp_db_config=self.config['tp_db'],
                event_db_config=self.config['event_db']
            )
            return monitor
        except ValueError:
            # Column doesn't exist - already logged and printed
            return None
        except Exception as e:
            self.logger.error(f"Error creating monitor for {machine_key}: {e}")
            print(f"❌ ERROR creating monitor for {machine_config.get('machine_id', machine_key)}: {e}")
            return None

    def start(self):
        """Start monitoring all enabled machines"""
        self.logger.info("Starting leaping sensor monitoring for all machines...")

        for machine_key, machine_config in self.config['machines'].items():
            if not machine_config.get('enabled', True):
                self.logger.info(f"Skipping {machine_key} (disabled in config)")
                continue

            # Create monitor
            monitor = self._create_monitor(machine_key, machine_config)
            if monitor is None:
                continue

            self.monitors.append(monitor)

            # Create and start thread
            thread = threading.Thread(
                target=monitor.run,
                name=f"LeapingSensor-{machine_config['machine_id']}",
                daemon=True
            )
            thread.start()
            self.threads.append(thread)
            self.logger.info(f"Started monitoring thread for {machine_config['machine_id']}")

        self.logger.info(f"All monitoring threads started ({len(self.threads)} machines)")

    def wait(self):
        """Wait for all threads to complete"""
        try:
            for thread in self.threads:
                thread.join()
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal")
            self.cleanup()

    def cleanup(self):
        """Cleanup all monitors"""
        self.logger.info("Cleaning up all monitors...")
        for monitor in self.monitors:
            try:
                monitor.cleanup()
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")


if __name__ == '__main__':
    try:
        manager = LeapingSensorManager()
        manager.start()
        manager.wait()
    except KeyboardInterrupt:
        print("\nScript terminated by user.")
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
