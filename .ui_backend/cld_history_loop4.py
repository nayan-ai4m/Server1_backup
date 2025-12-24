import psycopg2
import time
import json
import sys
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta

# ----------------------------------------------------------------------
# Logging configuration
# ----------------------------------------------------------------------
LOG_BASE_DIR = "/home/ai4m/develop/ui_backend/logs"
TODAY_STR = datetime.now().strftime("%Y-%m-%d")
LOG_DIR = os.path.join(LOG_BASE_DIR, TODAY_STR)
LOG_FILE = os.path.join(LOG_DIR, "cld_history_loop4.log")

# Create the date-folder if it does not exist
os.makedirs(LOG_DIR, exist_ok=True)

# Logger
logger = logging.getLogger("cld_history_loop")
logger.setLevel(logging.INFO)

# Rotating file handler (5 MB per file, keep 5 backups)
file_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
file_formatter = logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# Console handler (so you still see output in terminal)
console_handler = logging.StreamHandler()
console_handler.setFormatter(file_formatter)
logger.addHandler(console_handler)


def print(msg: str):
    logger.info(msg)


def log_error(msg: str):
    logger.error(msg, exc_info=True)   # includes traceback


# ----------------------------------------------------------------------
# Original helper functions (only prints replaced with logger)
# ----------------------------------------------------------------------
def load_config(config_file="config.json"):
    try:
        with open(config_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        log_error(f"{config_file} not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        log_error(f"Invalid JSON in {config_file}.")
        sys.exit(1)
    except Exception as e:
        log_error(f"Unexpected error loading {config_file}: {str(e)}")
        sys.exit(1)


def get_current_shift(shifts):
    now = datetime.now()
    current_hour = now.hour
    current_date = now.date()
    for shift_data in shifts:
        shift_name = shift_data["name"]
        start_hour = shift_data["start_hour"]
        end_hour = shift_data["end_hour"]
        cld_count_key = shift_data["cld_count_key"]
        cld_key = shift_data["cld_key"]
        if start_hour <= current_hour < end_hour or (
            start_hour > end_hour and (current_hour >= start_hour or current_hour < end_hour)
        ):
            shift_start_date = (
                current_date if current_hour >= start_hour else (current_date - timedelta(days=1))
            )
            shift_start_time = datetime(
                shift_start_date.year,
                shift_start_date.month,
                shift_start_date.day,
                start_hour,
                0,
                0,
            )
            return shift_name, cld_count_key, cld_key, shift_start_time
    return None, None, None, None


def connect_to_database(config):
    try:
        conn = psycopg2.connect(
            dbname=config["database"]["dbname"],
            user=config["database"]["user"],
            password=config["database"]["password"],
            host=config["database"]["host"],
            port=config["database"]["port"],
            connect_timeout=5,
        )
        print("Connected to PostgreSQL database successfully.")
        return conn
    except psycopg2.Error as e:
        log_error(f"Error connecting to database: {str(e)}")
        sys.exit(1)


def main():
    config = load_config()
    max_retries = 5
    retry_delay = 10  # seconds

    for attempt in range(max_retries):
        conn = None
        cur = None
        try:
            conn = connect_to_database(config)
            cur = conn.cursor()
            shifts = config["shifts"]
            machines_cld_count = config["machines"]["cld_count"]
            machines_cld = config["machines"]["cld"]

            while True:
                current_shift, cld_count_column, cld_column, shift_start_time = get_current_shift(
                    shifts
                )
                if not current_shift:
                    print("Error determining the shift.")
                    time.sleep(10)
                    continue

                for machine in machines_cld_count + machines_cld:
                    column_name = (
                        cld_count_column if machine in machines_cld_count else cld_column
                    )
                    cur.execute(
                        f"""
                        SELECT {column_name} FROM {machine}
                        WHERE DATE(timestamp) = CURRENT_DATE
                        ORDER BY timestamp DESC
                        LIMIT 1
                        """
                    )
                    result = cur.fetchone()
                    if not result:
                        continue
                    cld_value = result[0]
                    if cld_value is None:
                        continue

                    cur.execute(
                        "SELECT value FROM cld_history WHERE timestamp = %s AND machine_no = %s AND shift = %s",
                        (shift_start_time, machine, current_shift),
                    )
                    existing_entry = cur.fetchone()

                    if existing_entry:
                        existing_value = existing_entry[0]
                        if existing_value != cld_value:
                            cur.execute(
                                "UPDATE cld_history SET value = %s WHERE timestamp = %s AND machine_no = %s AND shift = %s",
                                (cld_value, shift_start_time, machine, current_shift),
                            )
                            print(f"Updated value for machine {machine}")
                    else:
                        cur.execute(
                            "INSERT INTO cld_history (timestamp, machine_no, shift, value) VALUES (%s, %s, %s, %s)",
                            (shift_start_time, machine, current_shift, cld_value),
                        )
                        print(f"Inserted new shift for machine {machine}")

                conn.commit()
                time.sleep(5)

        except psycopg2.Error as e:
            log_error(f"Database error (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                log_error("Max retries reached. Exiting.")
                sys.exit(1)

        except Exception as e:
            log_error(f"Unexpected error (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                log_error("Max retries reached. Exiting.")
                sys.exit(1)

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
                print("Database connection closed.")


if __name__ == "__main__":
    main()
