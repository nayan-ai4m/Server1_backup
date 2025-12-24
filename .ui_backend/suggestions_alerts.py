import psycopg2
import uuid
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import logging
import os
import traceback
import sys

# ==================== DEDICATED DATE-WISE ERROR LOGGER ====================
log_dir = "/home/ai4m/develop/ui_backend/logs"
date_str = datetime.now().strftime("%Y-%m-%d")
date_path = os.path.join(log_dir, date_str)
os.makedirs(date_path, exist_ok=True)  # Auto-create date folder

error_log_file = os.path.join(date_path, "suggestion_alert.log")

error_logger = logging.getLogger('SuggestionAlertErrorLogger')
error_logger.setLevel(logging.ERROR)
error_logger.propagate = False

error_handler = logging.FileHandler(error_log_file)
error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)

# Keep original INFO logging to console + old file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('suggestions_alerts.log'),
        logging.StreamHandler()
    ]
)

# Helper: Log error + full traceback to date-wise file
def log_error_with_trace(msg, *args, **kwargs):
    error_logger.error(msg, *args, **kwargs)
    error_logger.error(traceback.format_exc())

# =========================================================================

try:
    with open('config.json') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    log_error_with_trace("Error: config.json not found")
    raise SystemExit(1)
except json.JSONDecodeError as e:
    log_error_with_trace(f"Error: Invalid JSON in config.json: {e}")
    raise SystemExit(1)

try:
    db_params = {
        'dbname': CONFIG['database']['dbname'],
        'user': CONFIG['database']['user'],
        'password': CONFIG['database']['password'],
        'host': CONFIG['database']['host'],
        'port': CONFIG['database']['port']
    }
    suggestions_config = CONFIG['suggestions_alerts']
    machines = suggestions_config['machines']
    quality_tps = suggestions_config['quality_tps']
    breakdown_tps = suggestions_config['breakdown_tps']
    productivity_tps = suggestions_config['productivity_tps']
except KeyError as e:
    log_error_with_trace(f"Missing configuration key: {e}")
    raise SystemExit(1)

def connect_db():
    try:
        conn = psycopg2.connect(**db_params)
        logging.info("Successfully connected to the database")
        return conn
    except Exception as e:
        log_error_with_trace(f"Error connecting to database: {e}")
        raise

def get_latest_suggestions(conn):
    query = """
    SELECT DISTINCT ON (machine_number)
        timestamp, machine_number, alert_details, suggestion_details, acknowledge, ignored_time, acknowledge_time
    FROM suggestions
    WHERE machine_number = ANY(%s)
    ORDER BY machine_number, timestamp DESC
    """
    try:
        with conn.cursor() as cur:
            logging.debug(f"Executing query with machines: {machines}")
            cur.execute(query, (machines,))
            results = cur.fetchall()
            logging.info(f"Fetched {len(results)} suggestions: {[(r[1], r[0], r[4], r[5], r[6]) for r in results]}")
            return results
    except Exception as e:
        log_error_with_trace(f"Error fetching suggestions: {e}")
        return []

def get_last_event_ignored_time(conn, machine_number):
    query = """
    SELECT timestamp
    FROM event_table
    WHERE camera_id = %s AND event_type LIKE %s
    ORDER BY timestamp DESC
    LIMIT 1
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (machine_number.upper(), '%Suggestion is Ignored'))
            result = cur.fetchone()
            ignored_time = result[0] if result else None
            logging.debug(f"Last ignored_time for {machine_number.upper()}: {ignored_time}")
            return ignored_time
    except Exception as e:
        log_error_with_trace(f"Error fetching last ignored_time for {machine_number}: {e}")
        return None

def insert_event(conn, suggestion):
    timestamp, machine_number, alert_details, suggestion_details, acknowledge, ignored_time, acknowledge_time = suggestion
    logging.debug(f"Processing suggestion for {machine_number}: timestamp={timestamp}, acknowledge={acknowledge}, ignored_time={ignored_time}, acknowledge_time={acknowledge_time}")
    acknowledge_str = str(acknowledge) if acknowledge is not None else None
    if not acknowledge_str or acknowledge_str not in ['2', '3', '4']:
        logging.info(f"Skipping {machine_number}: Invalid or empty acknowledge value ({acknowledge_str})")
        return

    event_type_map = {
        '2': 'is Acknowledged',
        '3': 'is Ignored',
        '4': 'is Rejected'
    }
    kolkata_tz = ZoneInfo("Asia/Kolkata")
    event_timestamp = (
        acknowledge_time if acknowledge_str == '2' else
        ignored_time if acknowledge_str == '3' else
        datetime.now(kolkata_tz)
    )
    logging.debug(f"Event timestamp for {machine_number}: {event_timestamp}")

    try:
        suggestion_data = json.loads(suggestion_details) if isinstance(suggestion_details, str) else suggestion_details
        suggestion = suggestion_data.get('suggestion', {}) if isinstance(suggestion_data, dict) else {}
        formatted_suggestion = []
        for key, value in suggestion.items():
            key_map = {
                'HMI_Hor_Sealer_Strk_1': 'Stroke1',
                'HMI_Hor_Sealer_Strk_2': 'Stroke2',
                'HMI_Hor_Seal_Front_28': 'Rear Temperature',
                'HMI_Hor_Seal_Front_27': 'Front Temperature'
            }
            readable_key = key_map.get(key, key.replace('_', ' ').title())
            formatted_suggestion.append(f"{readable_key}: {value}")
        suggestion_text = ", ".join(formatted_suggestion)
        event_type = f"Suggestion {suggestion_text} {event_type_map[acknowledge_str]}".strip()
    except (json.JSONDecodeError, AttributeError) as e:
        log_error_with_trace(f"Error parsing suggestion_details for {machine_number}: {e}")
        event_type = f"Suggestion {str(suggestion_details)} {event_type_map[acknowledge_str]}".strip()

    try:
        alert_data = json.loads(alert_details) if isinstance(alert_details, str) else alert_details
        tp_value = alert_data.get('TP', '')
    except (json.JSONDecodeError, AttributeError) as e:
        log_error_with_trace(f"Error parsing alert_details for {machine_number}: {e}")
        tp_value = ''

    if tp_value in quality_tps:
        alert_type = 'Quality'
    elif tp_value in breakdown_tps:
        alert_type = 'Breakdown'
    elif tp_value in productivity_tps:
        alert_type = 'Productivity'
    else:
        alert_type = 'Productivity'

    logging.debug(f"Assigned alert_type for {machine_number} (TP={tp_value}): {alert_type}")

    check_duplicate_query = """
    SELECT event_id
    FROM event_table
    WHERE camera_id = %s AND event_type = %s AND timestamp = %s
    LIMIT 1
    """
    try:
        with conn.cursor() as cur:
            cur.execute(check_duplicate_query, (machine_number.upper(), event_type, event_timestamp))
            if cur.fetchone():
                logging.info(f"Skipping {machine_number}: Duplicate event found (event_type={event_type}, timestamp={event_timestamp})")
                return
    except Exception as e:
        log_error_with_trace(f"Error checking for duplicate event for {machine_number}: {e}")

    if acknowledge_str == '3':
        last_ignored_time = get_last_event_ignored_time(conn, machine_number)
        if last_ignored_time and last_ignored_time == ignored_time:
            logging.info(f"Skipping {machine_number}: Duplicate ignored_time ({ignored_time})")
            return

    event_id = str(uuid.uuid4())
    camera_id = machine_number.upper()
    zone = 'Control Panel'
    acknowledge_int = int(acknowledge_str)
    insert_query = """
    INSERT INTO event_table (
        timestamp, event_id, zone, camera_id, assigned_to, action, remark,
        resolution_time, filename, event_type, alert_type, assigned_time
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    event_data = (
        event_timestamp, event_id, zone, camera_id, None, None, None,
        None, None, event_type, alert_type, None
    )
    try:
        with conn.cursor() as cur:
            cur.execute(insert_query, event_data)
            conn.commit()
            logging.info(f"Inserted event for {machine_number}: event_id={event_id}, event_type={event_type}, alert_type={alert_type}, acknowledge={acknowledge_int}")
    except Exception as e:
        log_error_with_trace(f"Error inserting event for {machine_number}: {e}")
        conn.rollback()

def main():
    try:
        conn = connect_db()
    except Exception as e:
        log_error_with_trace(f"Failed to start: {e}")
        return

    try:
        while True:
            logging.info(f"Starting new check at {datetime.now(ZoneInfo('Asia/Kolkata'))}")
            suggestions = get_latest_suggestions(conn)
            if not suggestions:
                logging.info("No suggestions found for any machine")
            for suggestion in suggestions:
                insert_event(conn, suggestion)
            logging.info(f"Completed check: Processed {len(suggestions)} suggestions")
            time.sleep(30)
    except KeyboardInterrupt:
        logging.info("Stopping event generator...")
    except Exception as e:
        log_error_with_trace(f"Unexpected error in main loop: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()
