import json
import time
import logging
import psycopg2
import uuid
import os
from datetime import datetime
from pycomm3 import LogixDriver

# ===================================================================
# 1. CONFIGURE LOGGING: Only ERROR+ → dated case_erector.log
# ===================================================================
BASE_LOG_DIR = "/home/ai4m/develop/ui_backend/logs"
today_str = datetime.now().strftime("%Y-%m-%d")  # e.g., 2025-11-05
DATED_DIR = os.path.join(BASE_LOG_DIR, today_str)

# Create directory if it doesn't exist
os.makedirs(DATED_DIR, exist_ok=True)
CASE_ERECTOR_LOG = os.path.join(DATED_DIR, "case_erector.log")

# Suppress pycomm3 internal logs
logging.getLogger('pycomm3').setLevel(logging.CRITICAL)

# Configure root logger: Console = WARNING+, no file yet
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Console output: WARNING and above
    ]
)

# Add dedicated handler: ONLY ERROR+ → case_erector.log
error_handler = logging.FileHandler(CASE_ERECTOR_LOG)
error_handler.setLevel(logging.ERROR)  # Only ERROR and CRITICAL
error_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
)
logging.getLogger().addHandler(error_handler)

# ===================================================================
# 2. MagazineMonitor Class
# ===================================================================
class MagazineMonitor:
    def __init__(self, config_file):
        try:
            with open(config_file) as f:
                self.config = json.load(f)
        except FileNotFoundError:
            logging.error("Error: config.json not found")
            raise
        except json.JSONDecodeError:
            logging.error("Error: Invalid JSON in config.json")
            raise

        self.loop3_tag = self.config["loop3_tag"]
        self.loop4_tag = self.config["loop4_tag"]
        self.poll_interval = self.config["poll_interval_seconds"]
        self.plc_ip = self.config["data_source"]["plc_ip"]
        self.protocol = self.config["data_source"]["protocol"]
        self.timeout = self.config["data_source"]["timeout"]
        self.db_insert_interval = 60  # Insert every 60 seconds

        if self.protocol != "ethernet_ip":
            logging.error("Unsupported protocol: %s", self.protocol)
            raise ValueError("Unsupported protocol in config")

        self.plc = LogixDriver(self.plc_ip)
        self.plc.socket_timeout = self.timeout

        # Main database connection
        try:
            self.db_conn = psycopg2.connect(**self.config["db"])
            self.db_cursor = self.db_conn.cursor()
            logging.debug("Connected to main database at %s", self.config["db"]["host"])
        except psycopg2.Error as e:
            logging.error("Failed to connect to main database: %s", e)
            raise

        # Event database connection (localhost)
        try:
            event_db_config = self.config["db"].copy()
            event_db_config["host"] = "localhost"
            self.event_db_conn = psycopg2.connect(**event_db_config)
            self.event_db_cursor = self.event_db_conn.cursor()
            logging.debug("Connected to event database at localhost")
        except psycopg2.Error as e:
            logging.error("Failed to connect to event database: %s", e)
            raise

        # Verify required tables
        for table in ["case_erector_work_instruction", "case_erector_tp_status", "case_erector_tp_status_loop4"]:
            if not self.verify_table_exists(table, self.db_cursor):
                logging.error("Table '%s' does not exist in main database", table)
                raise ValueError(f"Required table {table} not found")

        if not self.verify_table_exists("event_table", self.event_db_cursor):
            logging.error("Table 'event_table' does not exist in event database")
            raise ValueError("Required table event_table not found")

        # State tracking
        self.last_db_insert = 0
        self.loop3_last_status = None
        self.loop4_last_status = None
        self.loop3_uuid = None
        self.loop4_uuid = None
        self.loop3_timestamp = None
        self.loop4_timestamp = None

    def verify_table_exists(self, table_name, cursor):
        try:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (table_name,))
            return cursor.fetchone()[0]
        except psycopg2.Error as e:
            logging.error("Error checking table %s: %s", table_name, e)
            return False

    def read_tags(self):
        try:
            if not self.plc.connected:
                if not self.plc.open():
                    logging.error("Failed to connect to PLC at %s", self.plc_ip)
                    return None

            response = self.plc.read(self.loop3_tag, self.loop4_tag)
            if not response:
                logging.error("No data received from PLC %s", self.plc_ip)
                return None

            tag_data = {}
            for tag in response:
                if tag.error:
                    logging.error("Error reading tag %s: %s", tag.tag, tag.error)
                    tag_data[tag.tag] = None
                else:
                    tag_data[tag.tag] = 1 if tag.value is True else 0 if tag.value is False else tag.value

            loop3_status = tag_data.get(self.loop3_tag)
            loop4_status = tag_data.get(self.loop4_tag)

            if loop3_status is None:
                logging.warning("No data for tag %s", self.loop3_tag)
            if loop4_status is None:
                logging.warning("No data for tag %s", self.loop4_tag)

            return {
                "loop3_status": loop3_status,
                "loop4_status": loop4_status
            }
        except Exception as e:
            logging.error("Error reading from PLC %s: %s", self.plc_ip, str(e))
            return None

    def insert_event(self, loop, status, event_uuid, current_time):
        if status == 0:
            return
        try:
            event_type = "Case erector magazine about to end"
            camera_id = "loop3" if loop == "loop3" else "loop4"
            query = """
                INSERT INTO public.event_table(
                    timestamp, event_id, zone, camera_id, event_type, alert_type
                ) VALUES (%s, %s, %s, %s, %s, %s);
            """
            self.event_db_cursor.execute(query, (
                datetime.now(),
                event_uuid,
                "PLC",
                camera_id,
                event_type,
                "Productivity"
            ))
            self.event_db_conn.commit()
        except psycopg2.Error as e:
            logging.error("DB error inserting into event_table for %s: %s", loop, e)
            self.event_db_conn.rollback()

    def update_tp_status(self, loop, status, table_name):
        current_time = datetime.now().isoformat()
        try:
            self.db_cursor.execute(f"SELECT COUNT(*) FROM public.{table_name}")
            row_count = self.db_cursor.fetchone()[0]

            last_status = self.loop3_last_status if loop == "loop3" else self.loop4_last_status
            uuid_val = self.loop3_uuid if loop == "loop3" else self.loop4_uuid
            orig_time = self.loop3_timestamp if loop == "loop3" else self.loop4_timestamp

            if status == 1 and last_status != 1:
                new_uuid = str(uuid.uuid4())
                json_data = {
                    "uuid": new_uuid,
                    "active": 1,
                    "timestamp": current_time,
                    "color_code": 3
                }
                query = f"INSERT INTO public.{table_name}(tp41) VALUES (%s);" if row_count == 0 else f"UPDATE public.{table_name} SET tp41 = %s;"
                self.db_cursor.execute(query, (json.dumps(json_data),))
                self.db_conn.commit()

                self.insert_event(loop, status, new_uuid, current_time)

                if loop == "loop3":
                    self.loop3_uuid = new_uuid
                    self.loop3_timestamp = current_time
                    self.loop3_last_status = 1
                else:
                    self.loop4_uuid = new_uuid
                    self.loop4_timestamp = current_time
                    self.loop4_last_status = 1

            elif status == 0 and last_status == 1:
                json_data = {
                    "uuid": uuid_val,
                    "active": 0,
                    "timestamp": orig_time,
                    "color_code": 1,
                    "updated_timestamp": current_time
                }
                query = f"INSERT INTO public.{table_name}(tp41) VALUES (%s);" if row_count == 0 else f"UPDATE public.{table_name} SET tp41 = %s;"
                self.db_cursor.execute(query, (json.dumps(json_data),))
                self.db_conn.commit()

                self.insert_event(loop, status, uuid_val, current_time)

                if loop == "loop3":
                    self.loop3_last_status = 0
                else:
                    self.loop4_last_status = 0

        except psycopg2.Error as e:
            logging.error("DB error in %s for table %s: %s", loop, table_name, e)
            self.db_conn.rollback()

    def run(self):
        while True:
            tags = self.read_tags()
            if not tags or tags["loop3_status"] is None or tags["loop4_status"] is None:
                time.sleep(self.poll_interval)
                continue

            # Handle Loop 3
            if tags["loop3_status"] == 1:
                logging.warning("Loop 3 Alert: Case erector magazine about to end")
                self.update_tp_status("loop3", 1, "case_erector_tp_status")
            elif tags["loop3_status"] == 0 and self.loop3_last_status == 1:
                self.update_tp_status("loop3", 0, "case_erector_tp_status")

            # Handle Loop 4
            if tags["loop4_status"] == 1:
                logging.warning("Loop 4 Alert: Case erector magazine about to end")
                self.update_tp_status("loop4", 1, "case_erector_tp_status_loop4")
            elif tags["loop4_status"] == 0 and self.loop4_last_status == 1:
                self.update_tp_status("loop4", 0, "case_erector_tp_status_loop4")

            # Insert into work instruction table every 60 seconds
            current_time = time.time()
            if current_time - self.last_db_insert >= self.db_insert_interval:
                try:
                    insert_query = """
                        INSERT INTO public.case_erector_work_instruction(
                            "timestamp", loop3, loop4
                        ) VALUES (%s, %s, %s);
                    """
                    self.db_cursor.execute(insert_query, (
                        datetime.now(),
                        tags["loop3_status"],
                        tags["loop4_status"]
                    ))
                    self.db_conn.commit()
                    self.last_db_insert = current_time
                except psycopg2.Error as e:
                    logging.error("DB error inserting into case_erector_work_instruction: %s", e)
                    self.db_conn.rollback()

            time.sleep(self.poll_interval)

    def __del__(self):
        if hasattr(self, 'plc') and self.plc:
            self.plc.close()
        if hasattr(self, 'db_cursor'):
            self.db_cursor.close()
        if hasattr(self, 'db_conn'):
            self.db_conn.close()
        if hasattr(self, 'event_db_cursor'):
            self.event_db_cursor.close()
        if hasattr(self, 'event_db_conn'):
            self.event_db_conn.close()


# ===================================================================
# 3. Entry Point
# ===================================================================
if __name__ == "__main__":
    monitor = MagazineMonitor("case_erector.json")
    monitor.run()
