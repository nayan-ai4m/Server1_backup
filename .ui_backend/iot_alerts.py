import psycopg2
import uuid
import re
import time
import pytz
import atexit
import signal
import traceback
from datetime import datetime, timedelta
import json
import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

try:
    with open('config.json') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    print("Error: config.json not found")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"Error: Invalid JSON in config.json: {e}")
    sys.exit(1)

# ----------------------------------------------------------------------
# Dynamic Daily Logging Setup
# ----------------------------------------------------------------------
LOG_BASE_DIR = "/home/ai4m/develop/ui_backend/logs"
os.makedirs(LOG_BASE_DIR, exist_ok=True)

logger = logging.getLogger('IoT_Alerts')
logger.setLevel(logging.INFO)
logger.propagate = False  # Prevent duplicate logs if used elsewhere

# Global handler reference so we can replace it at midnight
file_handler = None

def get_current_log_path():
    today = datetime.now().strftime('%Y-%m-%d')
    daily_dir = os.path.join(LOG_BASE_DIR, today)
    os.makedirs(daily_dir, exist_ok=True)
    return os.path.join(daily_dir, "iot_alerts.log")

def setup_daily_logging():
    global file_handler
    log_path = get_current_log_path()
    
    # Remove old handler if exists
    if file_handler:
        logger.removeHandler(file_handler)
        file_handler.close()
    
    # New handler with midnight rollover
    file_handler = TimedRotatingFileHandler(
        log_path,
        when='midnight',
        interval=1,
        backupCount=30,  # keep 30 days
        encoding='utf-8'
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    
    # Also log to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# Initial setup
setup_daily_logging()

# Reconfigure logging at midnight
def check_midnight_reconfigure():
    current_date = datetime.now().strftime('%Y-%m-%d')
    last_date = getattr(check_midnight_reconfigure, 'last_date', None)
    if last_date != current_date:
        print(f"Date changed to {current_date}. Reconfiguring log path...")
        setup_daily_logging()
        check_midnight_reconfigure.last_date = current_date

# ----------------------------------------------------------------------
# Database configs
# ----------------------------------------------------------------------
SENSOR_DB_CONFIG = {
    'host': CONFIG['database']['host'],
    'database': CONFIG['database']['dbname1'],
    'user': CONFIG['database']['user'],
    'password': CONFIG['database']['password'],
    'port': CONFIG['database']['port'],
    'keepalives': 1,
    'keepalives_idle': 30,
    'keepalives_interval': 10,
    'keepalives_count': 5
}

EVENT_DB_CONFIG = {
    'host': CONFIG['database']['host'],
    'database': CONFIG['database']['dbname'],
    'user': CONFIG['database']['user'],
    'password': CONFIG['database']['password'],
    'port': CONFIG['database']['port'],
    'keepalives': 1,
    'keepalives_idle': 30,
    'keepalives_interval': 10,
    'keepalives_count': 5
}

SECOND_DB_CONFIG = {
    'host': "192.168.1.168",
    'database': "hul",
    'user': "postgres",
    'password': "ai4m2024",
    'port': 5432,
    'keepalives': 1,
    'keepalives_idle': 30,
    'keepalives_interval': 10,
    'keepalives_count': 5
}

class AnomalyHandler:
    def __init__(self):
        self.sensor_conn = None
        self.event_conn = None
        self.second_conn = None
        self.initialize_connections()
        atexit.register(self.close_connections)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def initialize_connections(self, max_retries=5, initial_delay=5):
        for attempt in range(max_retries):
            try:
                if self.sensor_conn is None or self.sensor_conn.closed:
                    self.sensor_conn = psycopg2.connect(**SENSOR_DB_CONFIG)
                    print("Sensor database connection established")
                if self.event_conn is None or self.event_conn.closed:
                    self.event_conn = psycopg2.connect(**EVENT_DB_CONFIG)
                    print("Event database connection established")
                if self.second_conn is None or self.second_conn.closed:
                    self.second_conn = psycopg2.connect(**SECOND_DB_CONFIG)
                    print("Second database connection established")
                return
            except psycopg2.Error as e:
                logger.error(f"DB connection failed (attempt {attempt + 1}/{max_retries}): {e}\n{traceback.format_exc()}")
                if attempt < max_retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error("Max retries reached. Exiting.")
                    self.close_connections()
                    sys.exit(1)

    def close_connections(self):
        print("Closing database connections...")
        for conn, name in [(self.sensor_conn, "sensor"), (self.event_conn, "event"), (self.second_conn, "second")]:
            try:
                if conn and not conn.closed:
                    conn.close()
                    print(f"{name.capitalize()} connection closed")
            except Exception as e:
                logger.error(f"Error closing {name} connection: {e}")

    def signal_handler(self, signum, frame):
        print(f"Received signal {signum}, shutting down...")
        self.close_connections()
        sys.exit(0)

    def check_connection(self, conn, name=""):
        if conn and not conn.closed:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return True
            except psycopg2.Error as e:
                logger.error(f"{name} connection lost: {e}\n{traceback.format_exc()}")
                conn.close()
                return False
        return False

    def check_and_insert_anomaly(self):
        check_midnight_reconfigure()  # <--- Important: daily log switch

        if not self.check_connection(self.sensor_conn, "Sensor"):
            logger.error("Sensor DB disconnected. Reconnecting...")
            self.initialize_connections()
            if not self.check_connection(self.sensor_conn, "Sensor"):
                logger.error("Failed to reconnect sensor DB. Skipping.")
                return

        try:
            with self.sensor_conn.cursor() as cursor:
                query = """
                WITH ranked_data AS (
                    SELECT
                        device_id, timestamp, x_rms_vel, y_rms_vel, z_rms_vel,
                        x_rms_acl, y_rms_acl, z_rms_acl, spl_db, temp_c, label,
                        ROW_NUMBER() OVER (PARTITION BY label ORDER BY timestamp DESC) AS row_num
                    FROM public.api_timeseries
                    WHERE label ~ '(17|18|19|20|21|22|25|26|27|28|29|30)'
                      AND timestamp >= NOW() - INTERVAL '5 minutes'
                )
                SELECT
                    device_id,
                    MAX(timestamp) as timestamp,
                    AVG(x_rms_vel) as avg_x_rms_vel,
                    AVG(y_rms_vel) as avg_y_rms_vel,
                    AVG(z_rms_vel) as avg_z_rms_vel,
                    AVG(x_rms_acl) as avg_x_rms_acl,
                    AVG(y_rms_acl) as avg_y_rms_acl,
                    AVG(z_rms_acl) as avg_z_rms_acl,
                    AVG(temp_c) as avg_temp_c,
                    label
                FROM ranked_data
                WHERE row_num <= 5
                GROUP BY device_id, label
                ORDER BY timestamp DESC, label DESC;
                """
                cursor.execute(query)
                data_records = cursor.fetchall()
                for new_data in data_records:
                    if new_data:
                        self.process_anomaly(*new_data)
        except psycopg2.OperationalError as e:
            logger.error(f"OperationalError in anomaly check: {e}\n{traceback.format_exc()}")
            if self.sensor_conn:
                self.sensor_conn.rollback()
                self.sensor_conn.close()
                self.sensor_conn = None
        except Exception as e:
            logger.error(f"Unexpected error in check_and_insert_anomaly: {e}\n{traceback.format_exc()}")
            self.close_connections()
            sys.exit(1)

    def process_anomaly(self, device_id, timestamp, avg_x_rms_vel, avg_y_rms_vel, avg_z_rms_vel,
                       avg_x_rms_acl, avg_y_rms_acl, avg_z_rms_acl, avg_temp_c, label):
        indian_timezone = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(indian_timezone)
        if current_time - timestamp.replace(tzinfo=indian_timezone) > timedelta(minutes=5):
            print(f"Skipping stale data for {label} (timestamp: {timestamp})")
            return

        event_types = []
        if any(val >= 9.5 for val in [avg_x_rms_vel, avg_y_rms_vel, avg_z_rms_vel]):
            event_types.append(("Velocity Critical", "critical"))
        elif all(2.5 <= val <= 9.5 for val in [avg_x_rms_vel, avg_y_rms_vel, avg_z_rms_vel]):
            event_types.append(("Velocity Warning", "warning"))

        if any(val > 17.5 for val in [avg_x_rms_acl, avg_y_rms_acl, avg_z_rms_acl]):
            event_types.append(("Acceleration Critical", "critical"))
        elif all(7.5 <= val <= 17.5 for val in [avg_x_rms_acl, avg_y_rms_acl, avg_z_rms_acl]):
            event_types.append(("Acceleration Warning", "warning"))

        if avg_temp_c >= 83:
            event_types.append(("Temperature Critical", "critical"))
        elif 63 <= avg_temp_c < 83:
            event_types.append(("Temperature Warning", "warning"))

        if not event_types:
            return

        for event_type, alert_type in event_types:
            self.insert_event(timestamp, device_id, event_type, alert_type, label)

    def insert_event(self, timestamp, device_id, event_type, alert_type, label):
        if not self.check_connection(self.event_conn, "Event"):
            logger.error("Event DB disconnected. Reconnecting...")
            self.initialize_connections()

        try:
            machine_id = self.get_machine_id(label)
            if not machine_id:
                logger.error(f"Could not parse machine ID from label: {label}")
                return

            orientation_match = re.search(r'(Horizontal|Vertical)', label)
            orientation = orientation_match.group(1) if orientation_match else "Unknown"
            event_type_with_orientation = f"{orientation} Servo {event_type}"

            indian_timezone = pytz.timezone('Asia/Kolkata')
            current_time = datetime.now(indian_timezone)

            # Insert into event_table
            with self.event_conn.cursor() as cursor:
                check_query = """
                SELECT timestamp FROM public.event_table
                WHERE camera_id = %s AND event_type = %s
                ORDER BY timestamp DESC LIMIT 1;
                """
                cursor.execute(check_query, (machine_id, event_type_with_orientation))
                last_event = cursor.fetchone()
                if last_event and (current_time - last_event[0]) < timedelta(minutes=5):
                    print(f"Skipping duplicate event: {event_type_with_orientation}")
                    return

                event_id = str(uuid.uuid4())
                insert_query = """
                INSERT INTO public.event_table (
                    timestamp, event_id, zone, camera_id, event_type, alert_type
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    current_time, event_id, 'IOT Sensor', machine_id,
                    event_type_with_orientation, 'Breakdown'
                ))
                self.event_conn.commit()
                print(f"Inserted event: {event_type_with_orientation}")

            # Update second DB if critical/warning
            if alert_type in ["critical", "warning"]:
                if not self.check_connection(self.second_conn, "Second"):
                    logger.error("Second DB disconnected. Reconnecting...")
                    self.initialize_connections()

                table_name = f"mc{machine_id.lower()[2:]}_tp_status"
                column_name = "tp16" if orientation == "Vertical" else "tp44" if orientation == "Horizontal" else None
                if not column_name:
                    return

                with self.second_conn.cursor() as cursor:
                    # Table/column existence checks
                    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema='public' AND table_name=%s)", (table_name,))
                    if not cursor.fetchone()[0]:
                        logger.error(f"Table {table_name} not found in second DB")
                        return

                    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s", (table_name,))
                    if column_name not in [row[0] for row in cursor.fetchall()]:
                        logger.error(f"Column {column_name} not in {table_name}")
                        return

                    cursor.execute(f"SELECT 1 FROM public.{table_name} LIMIT 1")
                    if not cursor.fetchone():
                        logger.error(f"No rows in {table_name}")
                        return

                    cursor.execute(f"SELECT {column_name} FROM public.{table_name} LIMIT 1")
                    current_val = cursor.fetchone()[0]

                    if alert_type == "critical":
                        json_data = {
                            "uuid": str(uuid.uuid4()),
                            "active": 1,
                            "filepath": "",
                            "timestamp": current_time.isoformat(),
                            "color_code": 3,
                            "machine_part": "sealer",
                            "updated_timestamp": current_time.isoformat()
                        }
                    else:  # warning
                        json_data = current_val or {}
                        json_data["active"] = 0
                        json_data["updated_timestamp"] = current_time.isoformat()
                        if not current_val:
                            json_data.update({
                                "uuid": str(uuid.uuid4()),
                                "filepath": "",
                                "timestamp": current_time.isoformat(),
                                "color_code": 3,
                                "machine_part": "sealer"
                            })

                    cursor.execute(f"UPDATE public.{table_name} SET {column_name} = %s", (json.dumps(json_data),))
                    self.second_conn.commit()
                    print(f"Updated {table_name}.{column_name} → active={'1' if alert_type=='critical' else '0'}")

        except Exception as e:
            logger.error(f"Error in insert_event: {e}\n{traceback.format_exc()}")
            if self.event_conn:
                self.event_conn.rollback()
            if self.second_conn:
                self.second_conn.rollback()

    def get_machine_id(self, label):
        match = re.search(r'No-(\d+)', label)
        return f"MC{match.group(1)}" if match else None


if __name__ == "__main__":
    handler = None
    try:
        handler = AnomalyHandler()
        print("IoT Anomaly Handler started")
        while True:
            handler.check_and_insert_anomaly()
            time.sleep(15)
    except KeyboardInterrupt:
        print("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error in main: {e}\n{traceback.format_exc()}")
    finally:
        if handler:
            handler.close_connections()
        print("IoT Anomaly Handler stopped")
