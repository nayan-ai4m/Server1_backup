import psycopg2
import datetime
import time
from zoneinfo import ZoneInfo
import schedule
import logging
import os
from logging import FileHandler

# ----------------------------------------------------------------------
# DYNAMIC DAILY LOGGING (Auto-switch at midnight)
# ----------------------------------------------------------------------
LOG_BASE_DIR = "/home/ai4m/develop/ui_backend/logs"

# Global logger & handler (will be updated daily)
logger = logging.getLogger("empty_mat_logger")
logger.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Console output
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Dynamic file handler (added/updated in get_logger())
current_log_date = None
file_handler = None

def get_or_update_logger():
    """Ensure log file is in correct date folder (auto-switch at midnight)"""
    global current_log_date, file_handler

    today_str = datetime.datetime.now(IST).strftime("%Y-%m-%d")
    log_dir = os.path.join(LOG_BASE_DIR, today_str)
    log_file = os.path.join(log_dir, "empty_mat.log")

    if current_log_date != today_str:
        # Remove old file handler
        if file_handler:
            logger.removeHandler(file_handler)
            file_handler.close()

        # Create directory + new handler
        os.makedirs(log_dir, exist_ok=True)
        file_handler = FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        current_log_date = today_str
        logger.info(f"Log switched to: {log_file}")

    return logger


# ----------------------------------------------------------------------
# Database & Config
# ----------------------------------------------------------------------
DB_PARAMS = {
    "dbname": "hul",
    "user": "postgres",
    "password": "ai4m2024",
    "host": "localhost",
    "port": "5432"
}

MACHINES = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']

SQL_QUERY_TEMPLATE = """
WITH filtered_data AS (
    SELECT
        m.timestamp AS machine_timestamp,
        COALESCE((l.{machine}::jsonb->>'I_Filling_ON_OFF_SS')::boolean, false) AS filling,
        COALESCE((l.{machine}::jsonb->>'HMI_Pulling_ON_OFF')::boolean, false) AS pulling,
        m.status
    FROM {machine} m
    CROSS JOIN LATERAL (
        SELECT l.timestamp, l.{machine}
        FROM loop3_checkpoints l
        WHERE l.{machine} IS NOT NULL
          AND l.timestamp <= m.timestamp
        ORDER BY l.timestamp DESC
        LIMIT 1
    ) l
    WHERE m.timestamp >= %s AT TIME ZONE 'Asia/Kolkata'
      AND m.timestamp <= %s AT TIME ZONE 'Asia/Kolkata'
      AND m.status = 1
      AND COALESCE((l.{machine}::jsonb->>'I_Filling_ON_OFF_SS')::boolean, false) = false
      AND COALESCE((l.{machine}::jsonb->>'HMI_Pulling_ON_OFF')::boolean, false) = true
),
with_lag AS (
    SELECT
        machine_timestamp,
        machine_timestamp - LAG(machine_timestamp) OVER (ORDER BY machine_timestamp) AS diff
    FROM filtered_data
),
grouped AS (
    SELECT
        machine_timestamp,
        SUM(CASE WHEN diff IS NULL OR diff > INTERVAL '60 seconds' THEN 1 ELSE 0 END)
            OVER (ORDER BY machine_timestamp) AS group_id
    FROM with_lag
),
periods AS (
    SELECT
        group_id,
        MIN(machine_timestamp) AS start_time,
        MAX(machine_timestamp) AS end_time
    FROM grouped
    GROUP BY group_id
)
SELECT
    %s AS shift_number,
    COALESCE(SUM(EXTRACT(EPOCH FROM (end_time - start_time))), 0) AS total_duration_seconds,
    TO_CHAR(
        (COALESCE(SUM(EXTRACT(EPOCH FROM (end_time - start_time))), 0) || ' seconds')::interval,
        'MI:SS'
    ) AS duration_formatted,
    COALESCE(SUM(EXTRACT(EPOCH FROM (end_time - start_time))) / 0.6 * 7, 0) AS waste_amount_cm
FROM periods
WHERE EXTRACT(EPOCH FROM (end_time - start_time)) > 0;
"""

INSERT_QUERY = """
INSERT INTO laminate_waste_events (timestamp, machine, duration, waste_amount_cm, shift_number)
VALUES (CURRENT_TIMESTAMP, %s, %s, %s, %s);
"""

IST = ZoneInfo("Asia/Kolkata")


# ----------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------
def test_db_connection():
    log = get_or_update_logger()
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        print("Database connection successful")

        # Check laminate_waste_events columns
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'laminate_waste_events'
        """)
        columns = [row[0] for row in cur.fetchall()]
        expected = ['timestamp', 'machine', 'duration', 'waste_amount_cm', 'shift_number']
        missing = set(expected) - set(columns)
        if missing:
            log.error(f"Missing columns in laminate_waste_events: {missing}")
            return False
        print(f"Table laminate_waste_events OK: {columns}")

        # Check machine tables
        for machine in MACHINES:
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", (machine,))
            if not cur.fetchone()[0]:
                log.error(f"Table {machine} does not exist")
                return False

            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (machine,))
            cols = [row[0] for row in cur.fetchall()]
            if not {'timestamp', 'status'}.issubset(cols):
                log.error(f"Table {machine} missing columns: {set(['timestamp','status']) - set(cols)}")
                return False

            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'loop3_checkpoints'")
            loop_cols = [row[0] for row in cur.fetchall()]
            if machine not in loop_cols:
                log.error(f"Column {machine} missing in loop3_checkpoints")
                return False

        print("All tables and columns validated")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        log.error(f"Database connection failed: {e}", exc_info=True)
        return False


def get_current_shift_and_times():
    log = get_or_update_logger()
    now = datetime.datetime.now(IST)
    current_time = now.time()
    current_date = now.date()

    if datetime.time(7, 0) <= current_time < datetime.time(15, 0):
        shift_number = 1
        shift_start = datetime.datetime.combine(current_date, datetime.time(7, 0), tzinfo=IST)
        shift_end = datetime.datetime.combine(current_date, datetime.time(15, 0), tzinfo=IST)
    elif datetime.time(15, 0) <= current_time < datetime.time(23, 0):
        shift_number = 2
        shift_start = datetime.datetime.combine(current_date, datetime.time(15, 0), tzinfo=IST)
        shift_end = datetime.datetime.combine(current_date, datetime.time(23, 0), tzinfo=IST)
    else:
        shift_number = 3
        if current_time >= datetime.time(23, 0):
            shift_start = datetime.datetime.combine(current_date, datetime.time(23, 0), tzinfo=IST)
            shift_end = datetime.datetime.combine(current_date + datetime.timedelta(days=1), datetime.time(7, 0), tzinfo=IST)
        else:
            shift_start = datetime.datetime.combine(current_date - datetime.timedelta(days=1), datetime.time(23, 0), tzinfo=IST)
            shift_end = datetime.datetime.combine(current_date, datetime.time(7, 0), tzinfo=IST)

    print(f"Current shift: {shift_number}, Start: {shift_start}, End: {shift_end}")
    return shift_number, shift_start, shift_end


def calculate_waste(start_time, end_time, shift_number, machine):
    log = get_or_update_logger()
    print(f"Calculating waste for {machine} from {start_time} to {end_time}, Shift {shift_number}")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '30s'")

        # Data check
        cur.execute(f"SELECT COUNT(*) FROM {machine} WHERE timestamp >= %s AND timestamp <= %s AND status = 1", (start_time, end_time))
        machine_count = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM loop3_checkpoints WHERE {machine} IS NOT NULL AND timestamp <= %s", (end_time,))
        loop3_count = cur.fetchone()[0]
        print(f"Data: {machine_count} rows in {machine}, {loop3_count} in loop3_checkpoints")

        sql_query = SQL_QUERY_TEMPLATE.format(machine=machine)
        log.debug(f"Executing query for {machine}")
        cur.execute(sql_query, (start_time, end_time, shift_number))

        result = cur.fetchone()
        if result and len(result) == 4:
            shift_num, duration_seconds, duration_formatted, waste_amount_cm = result
            print(f"Result: {machine} | {duration_formatted} | {waste_amount_cm:.2f} cm | Shift {shift_num}")
            cur.execute(INSERT_QUERY, (machine, duration_formatted, waste_amount_cm, shift_num))
            conn.commit()
            print(f"Inserted into laminate_waste_events")
        else:
            log.warning(f"No valid data for {machine} in interval")

    except Exception as e:
        log.error(f"Error processing {machine}: {e}", exc_info=True)
    finally:
        if cur: cur.close()
        if conn: conn.close()


def schedule_for_shift():
    log = get_or_update_logger()
    shift_number, shift_start, shift_end = get_current_shift_and_times()
    print(f"Scheduling Shift {shift_number}: {shift_start} → {shift_end}")

    schedule.clear()

    # Immediate run for last completed 30-min interval
    now = datetime.datetime.now(IST)
    minutes_since = (now - shift_start).total_seconds() // 60
    last_interval = int(minutes_since // 30) * 30
    start_time = shift_start + datetime.timedelta(minutes=last_interval - 30)
    end_time = shift_start + datetime.timedelta(minutes=last_interval)

    if start_time >= shift_start and end_time <= now and end_time <= shift_end:
        print(f"Immediate run: {start_time} → {end_time}")
        for machine in MACHINES:
            calculate_waste(start_time, end_time, shift_number, machine)

    # Schedule every 30 minutes
    next_run = shift_start + datetime.timedelta(minutes=30)
    while next_run < shift_end:
        start_time = next_run - datetime.timedelta(minutes=30)
        end_time = next_run
        if start_time >= shift_start:
            for machine in MACHINES:
                schedule.every().day.at(next_run.strftime("%H:%M")).do(
                    calculate_waste,
                    start_time=start_time,
                    end_time=end_time,
                    shift_number=shift_number,
                    machine=machine
                )
                print(f"Scheduled {machine}: {start_time.strftime('%H:%M')} → {end_time.strftime('%H:%M')}")
        next_run += datetime.timedelta(minutes=30)

    # Reschedule at shift end
    schedule.every().day.at(shift_end.strftime("%H:%M")).do(schedule_for_shift)


def main():
    log = get_or_update_logger()
    if not test_db_connection():
        log.error("Exiting due to database issues")
        return

    print("Starting empty_mat waste calculator...")
    schedule_for_shift()

    while True:
        get_or_update_logger()  # Critical: Check date every minute
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
