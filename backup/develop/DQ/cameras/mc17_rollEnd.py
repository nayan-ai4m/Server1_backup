import psycopg2
from datetime import datetime, timedelta
from uuid import uuid4
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RollEndSensor_TP:
    def __init__(self, config_file):
        try:
            with open(config_file) as f:
                content = f.read()
                logging.info(f"Config file content: {content}")
                self.config = json.loads(content)
        except FileNotFoundError:
            logging.error(f"Config file {config_file} not found")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in {config_file}: {e}")
            raise

        self.read_conn = None
        self.read_cursor = None
        self.write_conn = None
        self.write_cursor = None
        self.connect_databases()

        self.fetch_mq_query = "SELECT timestamp, status FROM mc17 ORDER BY timestamp DESC LIMIT 1;"
        self.fetch_rq_query = '''
            SELECT timestamp, mc17 ->> 'ROLL_END_SENSOR' AS roll_end
            FROM loop3_checkpoints
            WHERE mc17 ->> 'ROLL_END_SENSOR' IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 2;
        '''
        self.insert_event_query = """INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s);"""

        # For tracking new laminate roll condition
        self.roll_end_triggered = False
        self.rq_true_start_time = None
        self.previous_new_roll_running = None
        self.previous_rq = None

    def connect_databases(self):
        """Initialize or reconnect to both read and write databases."""
        # Close existing connections if any
        self.close_databases()

        # Connect to read database (localhost)
        try:
            self.read_conn = psycopg2.connect(**self.config["read_db"])
            self.read_cursor = self.read_conn.cursor()
            logging.info("Successfully connected to read database (localhost)")
        except psycopg2.Error as e:
            logging.error(f"Failed to connect to read database: {e}")
            raise

        # Connect to write database (remote)
        try:
            self.write_conn = psycopg2.connect(**self.config["write_db"])
            self.write_cursor = self.write_conn.cursor()
            logging.info("Successfully connected to write database (141.141.141.128)")
        except psycopg2.Error as e:
            logging.error(f"Failed to connect to write database: {e}")
            raise

    def ensure_db_connection(self, conn, cursor, db_type):
        """Ensure the database connection and cursor are valid; reconnect if necessary."""
        try:
            if conn is None or conn.closed != 0 or cursor is None or cursor.closed:
                logging.info(f"{db_type} database connection lost. Reconnecting...")
                if db_type == "read":
                    self.read_conn = psycopg2.connect(**self.config["read_db"])
                    self.read_cursor = self.read_conn.cursor()
                    logging.info("Reconnected to read database")
                else:
                    self.write_conn = psycopg2.connect(**self.config["write_db"])
                    self.write_cursor = self.write_conn.cursor()
                    logging.info("Reconnected to write database")
                return True
            # Test connection with a simple query
            cursor.execute("SELECT 1")
            return True
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            logging.error(f"{db_type} database connection error: {e}")
            if db_type == "read":
                self.read_conn = psycopg2.connect(**self.config["read_db"])
                self.read_cursor = self.read_conn.cursor()
                logging.info("Reconnected to read database")
            else:
                self.write_conn = psycopg2.connect(**self.config["write_db"])
                self.write_cursor = self.write_conn.cursor()
                logging.info("Reconnected to write database")
            return True
        except psycopg2.Error as e:
            logging.error(f"{db_type} database error during connection check: {e}")
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
            logging.info("Database connections closed")
        except psycopg2.Error as e:
            logging.error(f"Error closing database connections: {e}")

    @staticmethod
    def format_event_data(is_problem):
        timestamp = datetime.now().isoformat()
        return {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": timestamp,
            "color_code": 3 if is_problem else 1
        }

    def insert_or_update_tp(self, column, event_data):
        check_query = f"SELECT {column} FROM mc17_tp_status;"
        update_query = f"UPDATE mc17_tp_status SET {column} = %s;"
        insert_query = f"INSERT INTO mc17_tp_status ({column}) VALUES (%s);"

        try:
            if not self.ensure_db_connection(self.write_conn, self.write_cursor, "write"):
                raise psycopg2.Error("Write database connection failed")
            
            self.write_cursor.execute(check_query)
            exists = self.write_cursor.fetchone()
            if exists:
                logging.info(f"Updating {column} with event data: {event_data}")
                self.write_cursor.execute(update_query, (json.dumps(event_data),))
            else:
                logging.info(f"Inserting new {column} with event data: {event_data}")
                self.write_cursor.execute(insert_query, (json.dumps(event_data),))
            self.write_conn.commit()
        except psycopg2.Error as e:
            logging.error(f"Error updating/inserting {column} in mc17_tp_status: {e}")
            self.write_conn.rollback()

    def run(self):
        logging.info("Code Started for MC17 Roll End")
        previous_is_problem = None
        new_roll_added = False
        first_run = True

        while True:
            try:
                # Ensure database connections
                if not self.ensure_db_connection(self.read_conn, self.read_cursor, "read"):
                    logging.error("Read database connection failed, retrying in 5 seconds")
                    time.sleep(5)
                    continue
                if not self.ensure_db_connection(self.write_conn, self.write_cursor, "write"):
                    logging.error("Write database connection failed, retrying in 5 seconds")
                    time.sleep(5)
                    continue

                # Fetch machine status from read database
                self.read_cursor.execute(self.fetch_mq_query)
                mq_row = self.read_cursor.fetchone()
                mq = int(mq_row[1]) if mq_row and mq_row[1] is not None else None

                # Fetch roll end sensor data from read database
                self.read_cursor.execute(self.fetch_rq_query)
                rq_rows = self.read_cursor.fetchall()
                logging.debug(f"RQ rows fetched: {rq_rows}")
                rq = None
                if rq_rows and len(rq_rows) > 0 and rq_rows[0][1] is not None:
                    rq = 1 if rq_rows[0][1].lower() == 'true' else 0
                else:
                    self.previous_rq = None

                logging.info(f"MQ={mq}, RQ={rq}, Previous RQ={self.previous_rq}, Roll End Triggered={self.roll_end_triggered}, New Roll Added={new_roll_added}, First Run={first_run}")

                # Skip processing if mq or rq is None
                if mq is None or rq is None:
                    logging.warning("MQ or RQ is None, skipping iteration")
                    self.previous_rq = rq
                    time.sleep(1)
                    first_run = False
                    continue

                # Detect "roll end" condition
                is_problem = False
                if mq == 1 and rq == 0 and self.previous_rq == 1:
                    logging.info(f"Roll end detected: mq={mq}, rq={rq}, previous_rq={self.previous_rq}")
                    is_problem = True
                    self.roll_end_triggered = True
                    new_roll_added = False
                    logging.warning("Roll end condition detected")

                # Event for roll ending
                if is_problem and is_problem != previous_is_problem:
                    event_data = self.format_event_data(is_problem=True)
                    data = (
                        datetime.now(), str(uuid4()), "PLC",
                        "MC17", "Laminate roll about to end", "quality"
                    )
                    logging.info(f"Inserting roll-end event: {data}")
                    self.read_cursor.execute(self.insert_event_query, data)
                    self.read_conn.commit()
                    self.insert_or_update_tp("tp50", event_data)
                    # Set active=0 for tp72
                    tp72_event_data = self.format_event_data(is_problem=False)
                    self.insert_or_update_tp("tp72", tp72_event_data)
                    self.write_conn.commit()

                # Detect "new roll running" condition
                new_roll_running = new_roll_added and mq == 1 and rq == 1
                if new_roll_running and new_roll_running != self.previous_new_roll_running:
                    data = (
                        datetime.now(), str(uuid4()), "PLC",
                        "MC17", "New Roll running", "quality"
                    )
                    logging.info(f"Inserting new roll running event: {data}")
                    # Uncomment if you want to insert this event
                    # self.write_cursor.execute(self.insert_event_query, data)
                    # self.write_conn.commit()

                # Check for new laminate roll condition
                if (self.roll_end_triggered or (mq != 1 and rq != 1)) and not new_roll_added and mq != 1 and rq == 1:
                    if self.rq_true_start_time is None:
                        self.rq_true_start_time = datetime.now()
                        logging.info("Started tracking MQ!=1, RQ=1 for new laminate roll...")

                    elif datetime.now() - self.rq_true_start_time >= timedelta(minutes=3):
                        event_data = self.format_event_data(is_problem=True)
                        data = (
                            datetime.now(), str(uuid4()), "PLC",
                            "MC17", "New Laminate Roll added", "quality"
                        )
                        logging.info(f"Inserting new roll event: {data}")
                        self.read_cursor.execute(self.insert_event_query, data)
                        self.read_conn.commit()
                        self.insert_or_update_tp("tp72", event_data)
                        # Set active=0 for tp50
                        tp50_event_data = self.format_event_data(is_problem=False)
                        self.insert_or_update_tp("tp50", tp50_event_data)
                        self.write_conn.commit()

                        self.roll_end_triggered = False
                        self.rq_true_start_time = None
                        new_roll_added = True
                        logging.info("Reset roll end trigger and set new_roll_added=True")

                elif not (mq != 1 and rq == 1):
                    self.rq_true_start_time = None
                    logging.debug("Reset rq_true_start_time due to condition break")

                previous_is_problem = is_problem
                self.previous_new_roll_running = new_roll_running
                self.previous_rq = rq
                first_run = False
                time.sleep(1)

            except psycopg2.Error as e:
                logging.error(f"Database error: {e}")
                self.read_conn.rollback()
                self.write_conn.rollback()
                time.sleep(5)  # Wait longer before retrying
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                time.sleep(5)

    def __del__(self):
        self.close_databases()

if __name__ == '__main__':
    RollEndSensor_TP("mc17_rollend_config.json").run()
