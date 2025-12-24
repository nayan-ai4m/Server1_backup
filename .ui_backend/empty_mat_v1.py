import psycopg2
import datetime
import time
from zoneinfo import ZoneInfo
import schedule
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection parameters
DB_PARAMS = {
    "dbname": "hul",
    "user": "postgres",
    "password": "ai4m2024",
    "host": "localhost",
    "port": "5432"
}

# List of machines to process
LOOP3_MACHINES = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']
LOOP4_MACHINES = ['mc25', 'mc26', 'mc27', 'mc30']
ALL_MACHINES = LOOP3_MACHINES + LOOP4_MACHINES

# SQL query template for calculating laminate waste - Loop 3 machines
# Uses loop3_checkpoints table with JSON fields for filling and pulling status
SQL_QUERY_TEMPLATE_LOOP3 = """
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

# SQL query template for calculating laminate waste - Loop 4 machines
# Uses mc*_fast table for status and mc*_mid table for filling_on_off
SQL_QUERY_TEMPLATE_LOOP4 = """
WITH filtered_data AS (
    SELECT
        f.timestamp AS machine_timestamp,
        f.status,
        COALESCE(m.filling_on_off, 1) AS filling_on_off
    FROM {machine}_fast f
    CROSS JOIN LATERAL (
        SELECT mid.timestamp, mid.filling_on_off
        FROM {machine}_mid mid
        WHERE mid.timestamp <= f.timestamp
        ORDER BY mid.timestamp DESC
        LIMIT 1
    ) m
    WHERE f.timestamp >= %s AT TIME ZONE 'Asia/Kolkata'
      AND f.timestamp <= %s AT TIME ZONE 'Asia/Kolkata'
      AND f.status = 1
      AND COALESCE(m.filling_on_off, 1) = 0
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

# SQL query to insert results into laminate_waste_events
INSERT_QUERY = """
INSERT INTO laminate_waste_events (timestamp, machine, duration, waste_amount_cm, shift_number)
VALUES (CURRENT_TIMESTAMP, %s, %s, %s, %s);
"""

# Timezone for Asia/Kolkata (IST)
IST = ZoneInfo("Asia/Kolkata")

def test_db_connection():
    """Test the database connection and table existence."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        logger.info("Database connection successful")

        # Verify laminate_waste_events table columns
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'laminate_waste_events'
        """)
        columns = [row[0] for row in cur.fetchall()]
        expected_columns = ['timestamp', 'machine', 'duration', 'waste_amount_cm', 'shift_number']
        if not all(col in columns for col in expected_columns):
            logger.error(f"Table laminate_waste_events missing columns: {set(expected_columns) - set(columns)}")
            return False
        logger.info(f"Table laminate_waste_events has correct columns: {columns}")

        # Verify Loop 3 machine tables and loop3_checkpoints columns
        for machine in LOOP3_MACHINES:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
            """, (machine,))
            if not cur.fetchone()[0]:
                logger.error(f"Table {machine} does not exist")
                return False
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
            """, (machine,))
            machine_columns = [row[0] for row in cur.fetchall()]
            if not {'timestamp', 'status'}.issubset(machine_columns):
                logger.error(f"Table {machine} missing required columns: {set(['timestamp', 'status']) - set(machine_columns)}")
                return False
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'loop3_checkpoints'
            """)
            loop3_columns = [row[0] for row in cur.fetchall()]
            if machine not in loop3_columns:
                logger.error(f"Column {machine} missing in loop3_checkpoints")
                return False
        logger.info("All Loop 3 machine tables and loop3_checkpoints columns validated")

        # Verify Loop 4 machine tables (_fast and _mid tables)
        for machine in LOOP4_MACHINES:
            # Check _fast table
            fast_table = f"{machine}_fast"
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
            """, (fast_table,))
            if not cur.fetchone()[0]:
                logger.error(f"Table {fast_table} does not exist")
                return False

            # Check _fast table has required columns
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
            """, (fast_table,))
            fast_columns = [row[0] for row in cur.fetchall()]
            if not {'timestamp', 'status'}.issubset(fast_columns):
                logger.error(f"Table {fast_table} missing required columns: {set(['timestamp', 'status']) - set(fast_columns)}")
                return False

            # Check _mid table
            mid_table = f"{machine}_mid"
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
            """, (mid_table,))
            if not cur.fetchone()[0]:
                logger.error(f"Table {mid_table} does not exist")
                return False

            # Check _mid table has filling_on_off column
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
            """, (mid_table,))
            mid_columns = [row[0] for row in cur.fetchall()]
            if 'filling_on_off' not in mid_columns:
                logger.error(f"Table {mid_table} missing 'filling_on_off' column")
                return False

        logger.info("All Loop 4 machine tables validated (_fast and _mid)")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def get_current_shift_and_times():
    """Determine the current shift and its start/end times based on current time."""
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

    logger.info(f"Current shift: {shift_number}, Start: {shift_start}, End: {shift_end}")
    return shift_number, shift_start, shift_end

def calculate_waste(start_time, end_time, shift_number, machine):
    """Execute SQL query to calculate laminate waste for a specific machine and store results."""
    logger.info(f"Calculating waste for {machine} from {start_time} to {end_time}, Shift {shift_number}")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        # Set statement timeout to 30 seconds to prevent hanging
        cur.execute("SET statement_timeout = '30s'")

        # Determine if this is a Loop 3 or Loop 4 machine and use appropriate query
        is_loop4 = machine in LOOP4_MACHINES

        if is_loop4:
            # Loop 4: Check data availability in _fast and _mid tables
            cur.execute(f"SELECT COUNT(*) FROM {machine}_fast WHERE timestamp >= %s AND timestamp <= %s AND status = 1", (start_time, end_time))
            fast_count = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {machine}_mid WHERE timestamp <= %s", (end_time,))
            mid_count = cur.fetchone()[0]
            logger.info(f"Data check for {machine}: {fast_count} rows in {machine}_fast, {mid_count} rows in {machine}_mid")

            # Use Loop 4 query template
            sql_query = SQL_QUERY_TEMPLATE_LOOP4.format(machine=machine)
        else:
            # Loop 3: Check data availability in machine table and loop3_checkpoints
            cur.execute(f"SELECT COUNT(*) FROM {machine} WHERE timestamp >= %s AND timestamp <= %s AND status = 1", (start_time, end_time))
            machine_count = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM loop3_checkpoints WHERE {machine} IS NOT NULL AND timestamp <= %s", (end_time,))
            loop3_count = cur.fetchone()[0]
            logger.info(f"Data check for {machine}: {machine_count} rows in {machine}, {loop3_count} rows in loop3_checkpoints")

            # Use Loop 3 query template
            sql_query = SQL_QUERY_TEMPLATE_LOOP3.format(machine=machine)

        # Log the formatted SQL query for debugging
        logger.debug(f"Formatted query for {machine}: {sql_query % (start_time, end_time, shift_number)}")

        # Execute the SQL query
        logger.debug(f"Executing query for {machine}")
        cur.execute(sql_query, (start_time, end_time, shift_number))
        logger.debug(f"Query executed for {machine}")

        result = cur.fetchone()
        if result and len(result) == 4:
            shift_num, duration_seconds, duration_formatted, waste_amount_cm = result
            logger.info(f"Query result for {machine}: shift={shift_num}, duration_seconds={duration_seconds}, duration_formatted={duration_formatted}, waste_amount_cm={waste_amount_cm}")

            # Only insert if there's actual waste (duration > 0)
            if duration_seconds is not None and duration_seconds > 0:
                cur.execute(INSERT_QUERY, (
                    machine,
                    duration_formatted,
                    waste_amount_cm,
                    shift_num
                ))
                conn.commit()
                logger.info(f"Inserted data for {machine}: duration={duration_formatted}, waste_amount_cm={waste_amount_cm}, shift={shift_num}")
            else:
                logger.info(f"No waste detected for {machine} in this interval (duration=0), skipping insert")
        else:
            logger.warning(f"No data returned for {machine} from {start_time} to {end_time} or invalid result format: {result}")

    except Exception as e:
        logger.error(f"Error processing {machine} from {start_time} to {end_time}: {e}")
        # Do not raise; continue with other machines
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def schedule_for_shift():
    """Schedule calculations every 30 minutes for the current shift for all machines (Loop 3 + Loop 4)."""
    shift_number, shift_start, shift_end = get_current_shift_and_times()
    logger.info(f"Starting scheduling for Shift {shift_number} from {shift_start} to {shift_end}")
    logger.info(f"Processing machines: Loop 3 = {LOOP3_MACHINES}, Loop 4 = {LOOP4_MACHINES}")

    # Clear any existing schedules
    schedule.clear()

    # Run immediate calculation for the most recent completed 30-minute interval
    current_time = datetime.datetime.now(IST)
    minutes_since_shift = (current_time - shift_start).total_seconds() // 60
    last_interval = int(minutes_since_shift // 30) * 30  # Round down to previous 30-min mark
    start_time = shift_start + datetime.timedelta(minutes=last_interval - 30)
    end_time = shift_start + datetime.timedelta(minutes=last_interval)
    if start_time >= shift_start and end_time <= shift_end and end_time <= current_time:
        logger.info(f"Running immediate calculation for {start_time} to {end_time}")
        for machine in ALL_MACHINES:
            calculate_waste(start_time, end_time, shift_number, machine)

    # Schedule future calculations every 30 minutes
    first_run = shift_start + datetime.timedelta(minutes=30)
    while first_run < shift_end:
        start_time = first_run - datetime.timedelta(minutes=30)
        end_time = first_run
        if start_time >= shift_start:
            for machine in ALL_MACHINES:
                schedule.every().day.at(first_run.strftime("%H:%M")).do(
                    calculate_waste, start_time=start_time, end_time=end_time, shift_number=shift_number, machine=machine
                )
                logger.info(f"Scheduled calculation for {machine} from {start_time} to {end_time}")
        first_run += datetime.timedelta(minutes=30)

    # Schedule a check for the next shift
    schedule.every().day.at(shift_end.strftime("%H:%M")).do(schedule_for_shift)

def main():
    """Main function to start the scheduler."""
    if not test_db_connection():
        logger.error("Exiting due to database connection failure")
        return

    # Recommended indexes to create in PostgreSQL for performance (run these in your database):
    #
    # Loop 3 machines (mc17-mc22):
    # CREATE INDEX idx_mc17_timestamp_status ON mc17 (timestamp, status);
    # CREATE INDEX idx_loop3_checkpoints_timestamp_mc17 ON loop3_checkpoints (timestamp) WHERE mc17 IS NOT NULL;
    # Repeat for other Loop 3 machines (mc18, mc19, mc20, mc21, mc22) as needed.
    #
    # Loop 4 machines (mc25, mc26, mc27, mc30):
    # CREATE INDEX idx_mc25_fast_timestamp_status ON mc25_fast (timestamp, status);
    # CREATE INDEX idx_mc25_mid_timestamp ON mc25_mid (timestamp);
    # Repeat for other Loop 4 machines (mc26, mc27, mc30) as needed.

    schedule_for_shift()
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()
