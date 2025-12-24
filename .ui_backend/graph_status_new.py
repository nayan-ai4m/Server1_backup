import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime, timedelta
import time
import sys
import os
import logging
import traceback

# ----------------------------------------------------------------------
# Logging setup – date-wise folder + graph_status.log
# ----------------------------------------------------------------------
LOG_BASE_DIR = "/home/ai4m/develop/ui_backend/logs"
CURRENT_DATE_STR = datetime.now().strftime("%Y-%m-%d")
LOG_DIR = os.path.join(LOG_BASE_DIR, CURRENT_DATE_STR)

os.makedirs(LOG_DIR, exist_ok=True)  # create folder if missing

LOG_FILE = os.path.join(LOG_DIR, "graph_status.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),  # still see output in console
    ],
)

log = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Config loading (with logging)
# ----------------------------------------------------------------------
try:
    with open("graph_config_new.json") as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    log.error("config.json not found")
    os._exit(1)
except json.JSONDecodeError as e:
    log.error(f"Invalid JSON in config.json: {e}")
    os._exit(1)

DB_CONFIG_SOURCE = {
    "dbname": CONFIG["database"]["dbname"],
    "user": CONFIG["database"]["user"],
    "password": CONFIG["database"]["password"],
    "host": CONFIG["database"]["host"],
    "port": CONFIG["database"]["port"],
    "connect_timeout": CONFIG["database"].get("connect_timeout", 5),
}

try:
    DB_CONFIG_TARGET = {
        "dbname": CONFIG["target_database"]["dbname"],
        "user": CONFIG["target_database"]["user"],
        "password": CONFIG["target_database"]["password"],
        "host": CONFIG["target_database"]["host"],
        "port": CONFIG["target_database"]["port"],
        "connect_timeout": CONFIG["target_database"].get("connect_timeout", 5),
    }
except KeyError as e:
    log.error(f"'target_database' section not found in config.json: missing {e}")
    os._exit(1)


class GraphStatusProcessor:
    def __init__(self, source_db_config, target_db_config):
        self.source_db_config = source_db_config
        self.target_db_config = target_db_config
        self.source_conn = None
        self.target_conn = None
        self.shifts = CONFIG["shifts"]

    def ensure_source_connection(self):
        if self.source_conn is None or self.source_conn.closed != 0:
            try:
                self.source_conn = psycopg2.connect(**self.source_db_config)
            except Exception as e:
                log.error(f"Failed to connect to source DB: {e}")
                raise

    def ensure_target_connection(self):
        if self.target_conn is None or self.target_conn.closed != 0:
            try:
                self.target_conn = psycopg2.connect(**self.target_db_config)
            except Exception as e:
                log.error(f"Failed to connect to target DB: {e}")
                raise

    def connect_source_db(self):
        self.ensure_source_connection()
        return self.source_conn

    def connect_target_db(self):
        self.ensure_target_connection()
        return self.target_conn

    def close_db(self):
        if self.source_conn is not None and self.source_conn.closed == 0:
            self.source_conn.close()
            self.source_conn = None
        if self.target_conn is not None and self.target_conn.closed == 0:
            self.target_conn.close()
            self.target_conn = None

    def __enter__(self):
        self.ensure_source_connection()
        self.ensure_target_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_db()

    def get_machine_data(self, machine_id):
        """
        Route to appropriate data fetching method based on machine loop.
        Loop 3 (mc17-mc22): Query loop3_checkpoints JSONB table
        Loop 4 (mc25, mc26, mc27, mc30): Query individual mc##_fast and mc##_mid tables
        Note: MC28 and MC29 are skipped - no data/requirements
        """
        self.ensure_source_connection()

        # Determine machine number
        machine_num = int(machine_id.replace('mc', '').replace('MC', ''))

        # Skip MC28 and MC29 - no data/requirements
        if machine_num in [28, 29]:
            log.info(f"Skipping {machine_id} - no data requirements")
            return None

        # Route to Loop 4 method for mc25, mc26, mc27, mc30
        if machine_num in [25, 26, 27, 30]:
            return self._get_loop4_machine_data(machine_id)

        # Continue with Loop 3 logic for mc17-mc22
        if machine_id == "mc18":
            query_template = """
                WITH query1 AS (
                    SELECT
                        time_bucket('5 seconds', timestamp) AS timebucket,
                        MAX(({machine_id}->>'HMI_Hopper_1_Low_Level')::float) AS hopper_low,
                        MAX(({machine_id}->>'HMI_Hopper_1_High_Level')::float) AS hopper_high,
                        MAX(({machine_id}->>'Hopper_1_Level_Percentage')::float) AS hopper_percentage,
                        MAX(({machine_id}->'HMI_Hor_Seal_Rear_35'->>'SetValue')::float) AS horizontal_rear,
                        MAX(({machine_id}->'HMI_Hor_Seal_Rear_36'->>'SetValue')::float) AS horizontal_front,
                        MAX(({machine_id}->>'HMI_Hopper_2_Low_Level')::float) AS hopper_low_2,
                        MAX(({machine_id}->>'HMI_Hopper_2_High_Level')::float) AS hopper_high_2,
                        MAX(({machine_id}->>'Hopper_2_Level_Percentage')::float) AS hopper_percentage_2,
                        MAX(({machine_id}->'HMI_Hor_Seal_Rear_35'->>'TempDisplay')::float) AS hor_sealer_rear_1_temp,
                        MAX(({machine_id}->'HMI_Hor_Seal_Rear_36'->>'TempDisplay')::float) AS hor_sealer_front_1_temp
                    FROM loop3_checkpoints
                    WHERE {machine_id} != 'null'
                    GROUP BY timebucket
                    ORDER BY timebucket DESC
                    LIMIT 100
                )
                SELECT json_build_object(
                    'hopper_level', json_agg(json_build_object(
                        'timestamp', query1.timebucket,
                        'actualValue', query1.hopper_percentage,
                        'upperLimit', query1.hopper_high,
                        'lowerLimit', query1.hopper_low
                    )),
                    'hopper_level_2', json_agg(json_build_object(
                        'timestamp', query1.timebucket,
                        'actualValue', query1.hopper_percentage_2,
                        'upperLimit', query1.hopper_high_2,
                        'lowerLimit', query1.hopper_low_2
                    )),
                    'hor_sealer_rear_1', json_agg(json_build_object(
                        'timestamp', query1.timebucket,
                        'setPoint', query1.horizontal_rear,
                        'actualValue', ROUND(query1.hor_sealer_rear_1_temp::numeric, 2),
                        'upperLimit', query1.horizontal_rear + 5,
                        'lowerLimit', query1.horizontal_rear - 5
                    )),
                    'hor_sealer_front_1', json_agg(json_build_object(
                        'timestamp', query1.timebucket,
                        'setPoint', query1.horizontal_front,
                        'actualValue', ROUND(query1.hor_sealer_front_1_temp::numeric, 2),
                        'upperLimit', query1.horizontal_front + 5,
                        'lowerLimit', query1.horizontal_front - 5
                    ))
                ) AS result
                FROM query1
                LIMIT 100
            """
        elif machine_id == "mc17":
            query_template = """
                WITH query1 AS (
                    SELECT
                        time_bucket('5 seconds', timestamp) AS timebucket,
                        MAX(({machine_id}->>'HMI_Hopper_Low_Level')::float) AS hopper_low,
                        MAX(({machine_id}->>'HMI_Hopper_High_Level')::float) AS hopper_high,
                        MAX(({machine_id}->>'Hopper_Level_Percentage')::float) AS hopper_percentage,
                        MAX(({machine_id}->'HMI_Hor_Seal_Rear_28'->>'SetValue')::float) AS horizontal_rear,
                        MAX(({machine_id}->'HMI_Hor_Seal_Front_27'->>'SetValue')::float) AS horizontal_front,
                        MAX(({machine_id}->'HMI_Hor_Seal_Rear_28'->>'TempDisplay')::float) AS hor_sealer_rear_1_temp,
                        MAX(({machine_id}->'HMI_Hor_Seal_Front_27'->>'TempDisplay')::float) AS hor_sealer_front_1_temp
                    FROM loop3_checkpoints
                    WHERE {machine_id} != 'null'
                    GROUP BY timebucket
                    ORDER BY timebucket DESC
                    LIMIT 100
                )
                SELECT json_build_object(
                    'hopper_level', json_agg(json_build_object(
                        'timestamp', query1.timebucket,
                        'actualValue', query1.hopper_percentage,
                        'upperLimit', query1.hopper_high,
                        'lowerLimit', query1.hopper_low
                    )),
                    'hor_sealer_rear_1', json_agg(json_build_object(
                        'timestamp', query1.timebucket,
                        'setPoint', query1.horizontal_rear,
                        'actualValue', ROUND(query1.hor_sealer_rear_1_temp::numeric, 2),
                        'upperLimit', query1.horizontal_rear + 5,
                        'lowerLimit', query1.horizontal_rear - 5
                    )),
                    'hor_sealer_front_1', json_agg(json_build_object(
                        'timestamp', query1.timebucket,
                        'setPoint', query1.horizontal_front,
                        'actualValue', ROUND(query1.hor_sealer_front_1_temp::numeric, 2),
                        'upperLimit', query1.horizontal_front + 5,
                        'lowerLimit', query1.horizontal_front - 5
                    ))
                ) AS result
                FROM query1
                LIMIT 100
            """
        else:
            query_template = """
                WITH query1 AS (
                    SELECT
                        time_bucket('5 seconds', timestamp) AS timebucket,
                        MAX(({machine_id}->>'HMI_Hopper_Low_Level')::float) AS hopper_low,
                        MAX(({machine_id}->>'HMI_Hopper_High_Level')::float) AS hopper_high,
                        MAX(({machine_id}->>'Hopper_Level_Percentage')::float) AS hopper_percentage,
                        MAX(({machine_id}->'HMI_Ver_Seal_Rear_27'->>'SetValue')::float) AS horizontal_rear,
                        MAX(({machine_id}->'HMI_Ver_Seal_Rear_28'->>'SetValue')::float) AS horizontal_front,
                        MAX(({machine_id}->'HMI_Ver_Seal_Rear_27'->>'TempDisplay')::float) AS hor_sealer_rear_1_temp,
                        MAX(({machine_id}->'HMI_Ver_Seal_Rear_28'->>'TempDisplay')::float) AS hor_sealer_front_1_temp
                    FROM loop3_checkpoints
                    WHERE {machine_id} != 'null'
                    GROUP BY timebucket
                    ORDER BY timebucket DESC
                    LIMIT 100
                )
                SELECT json_build_object(
                    'hopper_level', json_agg(json_build_object(
                        'timestamp', query1.timebucket,
                        'actualValue', query1.hopper_percentage,
                        'upperLimit', query1.hopper_high,
                        'lowerLimit', query1.hopper_low
                    )),
                    'hor_sealer_rear_1', json_agg(json_build_object(
                        'timestamp', query1.timebucket,
                        'setPoint', query1.horizontal_rear,
                        'actualValue', ROUND(query1.hor_sealer_rear_1_temp::numeric, 2),
                        'upperLimit', query1.horizontal_rear + 5,
                        'lowerLimit', query1.horizontal_rear - 5
                    )),
                    'hor_sealer_front_1', json_agg(json_build_object(
                        'timestamp', query1.timebucket,
                        'setPoint', query1.horizontal_front,
                        'actualValue', ROUND(query1.hor_sealer_front_1_temp::numeric, 2),
                        'upperLimit', query1.horizontal_front + 5,
                        'lowerLimit', query1.horizontal_front - 5
                    ))
                ) AS result
                FROM query1
                LIMIT 100
            """

        query = sql.SQL(query_template).format(machine_id=sql.Identifier(machine_id))
        with self.source_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                return result["result"] if result else None
            except Exception as e:
                log.error(
                    f"Error executing query for {machine_id}: {e}\n{traceback.format_exc()}"
                )
                return None

    def _get_loop4_machine_data(self, machine_id):
        """
        Fetch hopper and horizontal sealer data for Loop 4 machines (mc25-mc30).
        Queries individual mc##_fast and mc##_mid tables.
        Returns data in same JSON format as Loop 3 machines.
        """
        self.ensure_source_connection()

        machine_num = int(machine_id.replace('mc', '').replace('MC', ''))
        table_name_fast = f"{machine_id}_fast"
        table_name_mid = f"{machine_id}_mid"

        try:
            # Determine if dual-hopper or single-hopper machine
            if machine_num in [25, 26]:
                # Dual-hopper machines (mc25, mc26)
                hopper_query = f"""
                    WITH bucketed_fast AS (
                        SELECT
                            time_bucket('5 seconds', timestamp) AS bucket_timestamp,
                            AVG(hopper_left_level) AS avg_hopper_left,
                            AVG(hopper_right_level) AS avg_hopper_right
                        FROM {table_name_fast}
                        WHERE hopper_left_level IS NOT NULL
                           OR hopper_right_level IS NOT NULL
                        GROUP BY bucket_timestamp
                    ),
                    mid_with_buckets AS (
                        SELECT
                            time_bucket('5 seconds', timestamp) AS bucket_timestamp,
                            hopper_left_extreme_low_level,
                            hopper_left_low_level,
                            hopper_left_high_level,
                            hopper_right_extreme_low_level,
                            hopper_right_low_level,
                            hopper_right_high_level,
                            ROW_NUMBER() OVER (PARTITION BY time_bucket('5 seconds', timestamp) ORDER BY timestamp DESC) as rn
                        FROM {table_name_mid}
                    )
                    SELECT
                        bf.bucket_timestamp,
                        bf.avg_hopper_left,
                        bf.avg_hopper_right,
                        COALESCE(mb.hopper_left_low_level, 70.0) as left_low,
                        COALESCE(mb.hopper_left_high_level, 75.0) as left_high,
                        COALESCE(mb.hopper_right_low_level, 70.0) as right_low,
                        COALESCE(mb.hopper_right_high_level, 75.0) as right_high
                    FROM bucketed_fast bf
                    LEFT JOIN mid_with_buckets mb ON bf.bucket_timestamp = mb.bucket_timestamp AND mb.rn = 1
                    ORDER BY bf.bucket_timestamp DESC
                    LIMIT 100
                """
            else:
                # Single-hopper machines (mc27, mc28, mc29, mc30)
                hopper_query = f"""
                    WITH bucketed_fast AS (
                        SELECT
                            time_bucket('5 seconds', timestamp) AS bucket_timestamp,
                            AVG(hopper_level) AS avg_hopper_level
                        FROM {table_name_fast}
                        WHERE hopper_level IS NOT NULL
                        GROUP BY bucket_timestamp
                    ),
                    mid_with_buckets AS (
                        SELECT
                            time_bucket('5 seconds', timestamp) AS bucket_timestamp,
                            hopper_low_level,
                            hopper_high_level,
                            ROW_NUMBER() OVER (PARTITION BY time_bucket('5 seconds', timestamp) ORDER BY timestamp DESC) as rn
                        FROM {table_name_mid}
                    )
                    SELECT
                        bf.bucket_timestamp,
                        bf.avg_hopper_level,
                        COALESCE(mb.hopper_low_level, 70.0) as hopper_low,
                        COALESCE(mb.hopper_high_level, 75.0) as hopper_high
                    FROM bucketed_fast bf
                    LEFT JOIN mid_with_buckets mb ON bf.bucket_timestamp = mb.bucket_timestamp AND mb.rn = 1
                    ORDER BY bf.bucket_timestamp DESC
                    LIMIT 100
                """

            # Fetch horizontal sealer temperature data
            sealer_query = f"""
                SELECT
                    time_bucket('5 seconds', timestamp) AS bucket_timestamp,
                    AVG(hor_temp_27_pv_front) AS avg_front_temp_pv,
                    AVG(hor_temp_27_sv_front) AS avg_front_temp_sv,
                    AVG(hor_temp_28_pv_rear) AS avg_rear_temp_pv,
                    AVG(hor_temp_28_sv_rear) AS avg_rear_temp_sv
                FROM {table_name_mid}
                WHERE hor_temp_27_pv_front IS NOT NULL
                   OR hor_temp_27_sv_front IS NOT NULL
                   OR hor_temp_28_pv_rear IS NOT NULL
                   OR hor_temp_28_sv_rear IS NOT NULL
                GROUP BY bucket_timestamp
                ORDER BY bucket_timestamp DESC
                LIMIT 100
            """

            with self.source_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Execute hopper query
                cursor.execute(hopper_query)
                hopper_results = cursor.fetchall()

                # Execute sealer query
                cursor.execute(sealer_query)
                sealer_results = cursor.fetchall()

            # Transform hopper data to match Loop 3 format
            hopper_level = []
            hopper_level_2 = []

            if machine_num in [25, 26]:
                # Dual-hopper format
                for row in hopper_results:
                    if row['avg_hopper_left'] is not None:
                        hopper_level.append({
                            "timestamp": row['bucket_timestamp'].isoformat(),
                            "actualValue": float(row['avg_hopper_left']),
                            "lowerLimit": float(row['left_low']),
                            "upperLimit": float(row['left_high'])
                        })
                    if row['avg_hopper_right'] is not None:
                        hopper_level_2.append({
                            "timestamp": row['bucket_timestamp'].isoformat(),
                            "actualValue": float(row['avg_hopper_right']),
                            "lowerLimit": float(row['right_low']),
                            "upperLimit": float(row['right_high'])
                        })
            else:
                # Single-hopper format
                for row in hopper_results:
                    if row['avg_hopper_level'] is not None:
                        hopper_level.append({
                            "timestamp": row['bucket_timestamp'].isoformat(),
                            "actualValue": float(row['avg_hopper_level']),
                            "lowerLimit": float(row['hopper_low']),
                            "upperLimit": float(row['hopper_high'])
                        })

            # Transform sealer data
            hor_sealer_front_1 = []
            hor_sealer_rear_1 = []

            for row in sealer_results:
                if row['avg_front_temp_pv'] is not None and row['avg_front_temp_sv'] is not None:
                    hor_sealer_front_1.append({
                        "timestamp": row['bucket_timestamp'].isoformat(),
                        "actualValue": round(float(row['avg_front_temp_pv']), 2),
                        "setPoint": float(row['avg_front_temp_sv']),
                        "upperLimit": float(row['avg_front_temp_sv']) + 5,
                        "lowerLimit": float(row['avg_front_temp_sv']) - 5
                    })

                if row['avg_rear_temp_pv'] is not None and row['avg_rear_temp_sv'] is not None:
                    hor_sealer_rear_1.append({
                        "timestamp": row['bucket_timestamp'].isoformat(),
                        "actualValue": round(float(row['avg_rear_temp_pv']), 2),
                        "setPoint": float(row['avg_rear_temp_sv']),
                        "upperLimit": float(row['avg_rear_temp_sv']) + 5,
                        "lowerLimit": float(row['avg_rear_temp_sv']) - 5
                    })

            # Build result in same format as Loop 3
            result = {
                "hopper_level": hopper_level,
                "hor_sealer_rear_1": hor_sealer_rear_1,
                "hor_sealer_front_1": hor_sealer_front_1
            }

            # Add hopper_level_2 for dual-hopper machines
            if machine_num in [25, 26]:
                result["hopper_level_2"] = hopper_level_2

            return result

        except Exception as e:
            log.error(
                f"Error executing Loop 4 query for {machine_id}: {e}\n{traceback.format_exc()}"
            )
            # Rollback transaction to prevent cascade failures
            if self.source_conn and self.source_conn.closed == 0:
                self.source_conn.rollback()
            return None

    # ------------------------------------------------------------------
    # The rest of your methods (get_checkweigher_data, get_sku_data,
    # get_current_shift_times, get_cld_shift_data, etc.) remain the same
    # but every `except` block now uses `log.error` with full traceback.
    # ------------------------------------------------------------------
    def get_checkweigher_data(self, table_name):
        self.ensure_source_connection()
        query_template = """
            WITH query1 AS (
                SELECT
                    timestamp AS timebucket,
                    cld_weight,
                    (target_weight + upper_limit) AS upperlimit,
                    (target_weight - upper_limit) AS lowerlimit
                FROM {table_name}
                WHERE
                    cld_weight IS NOT NULL AND
                    target_weight IS NOT NULL AND
                    upper_limit IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 100
            )
            SELECT json_build_object(
                'data',
                json_agg(
                    json_build_object(
                        'timestamp', timebucket,
                        'actualValue', cld_weight,
                        'upperLimit', upperlimit,
                        'lowerLimit', lowerlimit
                    )
                )
            ) AS result
            FROM query1
        """
        query = sql.SQL(query_template).format(table_name=sql.Identifier(table_name))
        with self.source_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                return result["result"] if result else None
            except Exception as e:
                log.error(
                    f"Error executing checkweigher query for {table_name}: {e}\n{traceback.format_exc()}"
                )
                return None

    def get_sku_data(self, table_name):
        self.ensure_source_connection()
        if table_name == "loop3_sku":
            query = """
                WITH query1 AS (
                    SELECT
                        timestamp,
                        primary_tank/10 AS primarytank,
                        secondary_tank/10 AS secondarytank
                    FROM loop3_sku
                    ORDER BY timestamp DESC
                    LIMIT 100
                )
                SELECT json_build_object(
                    'data',
                    json_agg(
                        json_build_object(
                            'timestamp', timestamp,
                            'primarytank', round(primarytank::numeric, 2),
                            'secondarytank', round(secondarytank::numeric, 2)
                        )
                    )
                ) AS result
                FROM query1
            """
        elif table_name == "loop4_sku":
            query = """
                WITH query1 AS (
                    SELECT
                        timestamp,
                        (primary_tank/100) * 8 AS primarytank,
                        (secondary_tank/100) * 8 AS secondarytank
                    FROM loop4_sku
                    ORDER BY timestamp DESC
                    LIMIT 100
                )
                SELECT json_build_object(
                    'data',
                    json_agg(
                        json_build_object(
                            'timestamp', timestamp,
                            'primarytank', round(primarytank::numeric, 2),
                            'secondarytank', round(secondarytank::numeric, 2)
                        )
                    )
                ) AS result
                FROM query1
            """
        else:
            log.error(f"Invalid SKU table name: {table_name}")
            return None

        with self.source_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                return result["result"] if result else None
            except Exception as e:
                log.error(
                    f"Error executing SKU query for {table_name}: {e}\n{traceback.format_exc()}"
                )
                return None

    def get_current_shift_times(self):
        now = datetime.now().astimezone()
        current_hour = now.hour
        current_date = now.date()
        for shift in self.shifts:
            start_hour = shift["start_hour"]
            end_hour = shift["end_hour"]
            if start_hour > end_hour:
                if current_hour >= start_hour or current_hour < end_hour:
                    if current_hour >= start_hour:
                        start_time = datetime.combine(
                            current_date, datetime.min.time().replace(hour=start_hour)
                        ).astimezone()
                        end_time = datetime.combine(
                            current_date + timedelta(days=1),
                            datetime.min.time().replace(hour=end_hour),
                        ).astimezone()
                    else:
                        start_time = datetime.combine(
                            current_date - timedelta(days=1),
                            datetime.min.time().replace(hour=start_hour),
                        ).astimezone()
                        end_time = datetime.combine(
                            current_date, datetime.min.time().replace(hour=end_hour)
                        ).astimezone()
                    return start_time, end_time, shift["name"]
            else:
                if start_hour <= current_hour < end_hour:
                    start_time = datetime.combine(
                        current_date, datetime.min.time().replace(hour=start_hour)
                    ).astimezone()
                    end_time = datetime.combine(
                        current_date, datetime.min.time().replace(hour=end_hour)
                    ).astimezone()
                    return start_time, end_time, shift["name"]
        log.warning("No matching shift found, using default shift")
        start_time = datetime.combine(
            current_date, datetime.min.time().replace(hour=0)
        ).astimezone()
        end_time = datetime.combine(
            current_date + timedelta(days=1), datetime.min.time().replace(hour=0)
        ).astimezone()
        return start_time, end_time, "Unknown"

    def get_cld_shift_data(self, table_name):
        self.ensure_source_connection()
        start_time, end_time, shift = self.get_current_shift_times()
        query_template = """
            SELECT json_build_object(
                'timestamp_range', json_build_object(
                    'start', %s::timestamptz,
                    'end', %s::timestamptz
                ),
                'cld_count', COUNT(*),
                'Properweight', COUNT(CASE WHEN status = 'ok' THEN 1 END),
                'Underweight', COUNT(CASE WHEN status = 'underweight' THEN 1 END),
                'Overweight', COUNT(CASE WHEN status = 'overweight' THEN 1 END),
                'Null', COUNT(CASE WHEN status = 'null' THEN 1 END)
            ) AS result
            FROM (
                SELECT
                    CASE
                        WHEN cld_weight < (target_weight + upper_limit) AND cld_weight > (target_weight - lower_limit) THEN 'ok'
                        WHEN cld_weight > (target_weight + upper_limit) THEN 'overweight'
                        WHEN cld_weight < (target_weight - lower_limit) THEN 'underweight'
                        ELSE 'null'
                    END AS status
                FROM {table_name}
                WHERE timestamp BETWEEN %s AND %s
            ) AS classified_data
        """
        query = sql.SQL(query_template).format(table_name=sql.Identifier(table_name))
        try:
            with self.source_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (start_time, end_time, start_time, end_time))
                result = cursor.fetchone()
                if result and result["result"]:
                    return result["result"]
                else:
                    return {
                        "timestamp_range": {
                            "start": start_time.isoformat(),
                            "end": end_time.isoformat(),
                        },
                        "cld_count": 0,
                        "Properweight": 0,
                        "Underweight": 0,
                        "Overweight": 0,
                        "Null": 0,
                    }
        except Exception as e:
            log.error(
                f"Error getting CLD shift data for {table_name}: {e}\n{traceback.format_exc()}"
            )
            return {
                "timestamp_range": {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat(),
                },
                "cld_count": 0,
                "Properweight": 0,
                "Underweight": 0,
                "Overweight": 0,
                "Null": 0,
            }

    def check_if_table_exists(self):
        self.ensure_target_connection()
        with self.target_conn.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'graph_status'
                    )
                """
                )
                table_exists = cursor.fetchone()[0]
                if not table_exists:
                    cursor.execute(
                        """
                        CREATE TABLE graph_status (
                            timestamp TIMESTAMPTZ PRIMARY KEY,
                            mc17 JSONB,
                            mc18 JSONB,
                            mc19 JSONB,
                            mc20 JSONB,
                            mc21 JSONB,
                            mc22 JSONB,
                            loop3_checkweigher JSONB,
                            loop4_checkweigher JSONB,
                            loop3_sku JSONB,
                            loop4_sku JSONB,
                            loop3_cld JSONB,
                            loop4_cld JSONB
                        )
                    """
                    )
                    self.target_conn.commit()
                    print("Created graph_status table in target database")
                else:
                    # column checks & adds (unchanged)
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT FROM information_schema.columns 
                            WHERE table_name = 'graph_status' AND column_name = 'loop3_cld'
                        )
                    """
                    )
                    loop3_cld_exists = cursor.fetchone()[0]
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT FROM information_schema.columns 
                            WHERE table_name = 'graph_status' AND column_name = 'loop4_cld'
                        )
                    """
                    )
                    loop4_cld_exists = cursor.fetchone()[0]
                    if not loop3_cld_exists:
                        cursor.execute(
                            "ALTER TABLE graph_status ADD COLUMN loop3_cld JSONB"
                        )
                        print("Added loop3_cld column to graph_status table")
                    if not loop4_cld_exists:
                        cursor.execute(
                            "ALTER TABLE graph_status ADD COLUMN loop4_cld JSONB"
                        )
                        print("Added loop4_cld column to graph_status table")
                    if not loop3_cld_exists or not loop4_cld_exists:
                        self.target_conn.commit()

                cursor.execute("SELECT COUNT(*) FROM graph_status")
                count = cursor.fetchone()[0]
                if count == 0:
                    timestamp = datetime.now().astimezone().replace(microsecond=0)
                    cursor.execute(
                        """
                        INSERT INTO graph_status (timestamp)
                        VALUES (%s)
                    """,
                        (timestamp,),
                    )
                    self.target_conn.commit()
                    print(f"Created initial record with timestamp {timestamp}")
                    return timestamp
                else:
                    cursor.execute("SELECT timestamp FROM graph_status LIMIT 1")
                    timestamp = cursor.fetchone()[0]
                    print(f"Using existing record with timestamp {timestamp}")
                    return timestamp
            except Exception as e:
                log.error(
                    f"Error in check_if_table_exists: {e}\n{traceback.format_exc()}"
                )
                raise

    def process_all_machines(self):
        machine_ids = CONFIG["setpoints"]["machines"]
        checkweigher_tables = {
            "loop3_checkweigher": "loop3_checkweigher",
            "loop4_checkweigher": "loop4_checkweigher",
        }
        sku_tables = ["loop3_sku", "loop4_sku"]
        cld_tables = {
            "loop3_checkweigher": "loop3_cld",
            "loop4_checkweigher": "loop4_cld",
        }

        record_timestamp = self.check_if_table_exists()
        new_timestamp = datetime.now().astimezone().replace(microsecond=0)
        update_data = {}

        for machine_id in machine_ids:
            try:
                print(f"Processing data for {machine_id}...")
                data = self.get_machine_data(machine_id)
                if data:
                    update_data[machine_id] = json.dumps(data)
                    print(f"Collected data for {machine_id}")
                else:
                    log.warning(f"No data returned for {machine_id}")
            except Exception as e:
                log.error(
                    f"Error processing {machine_id}: {e}\n{traceback.format_exc()}"
                )
                self.source_conn.rollback()

        # similar blocks with logging...
        for table_name, column_name in checkweigher_tables.items():
            try:
                print(f"Processing checkweigher {table_name}...")
                data = self.get_checkweigher_data(table_name)
                if data:
                    update_data[column_name] = json.dumps(data)
                    print(f"Collected data for {table_name}")
                else:
                    log.warning(f"No data for {table_name}")
            except Exception as e:
                log.error(
                    f"Error processing checkweigher {table_name}: {e}\n{traceback.format_exc()}"
                )
                self.source_conn.rollback()

        for table_name in sku_tables:
            try:
                print(f"Processing SKU {table_name}...")
                data = self.get_sku_data(table_name)
                if data:
                    update_data[table_name] = json.dumps(data)
                    print(f"Collected data for {table_name}")
                else:
                    log.warning(f"No data for {table_name}")
            except Exception as e:
                log.error(
                    f"Error processing SKU {table_name}: {e}\n{traceback.format_exc()}"
                )
                self.source_conn.rollback()

        for source_table, dest_column in cld_tables.items():
            try:
                print(f"Processing CLD shift data for {source_table}...")
                data = self.get_cld_shift_data(source_table)
                if data:
                    update_data[dest_column] = json.dumps(data)
                    print(f"Collected CLD data for {source_table}")
                else:
                    log.warning(f"No CLD data for {source_table}")
            except Exception as e:
                log.error(
                    f"Error processing CLD {source_table}: {e}\n{traceback.format_exc()}"
                )
                self.source_conn.rollback()

        if update_data:
            self.update_all_machine_data(update_data, record_timestamp, new_timestamp)

    def update_all_machine_data(self, update_data, old_timestamp, new_timestamp):
        self.ensure_target_connection()
        try:
            with self.target_conn.cursor() as cursor:
                set_parts = []
                params = []
                for column_name, data in update_data.items():
                    set_parts.append(f"{column_name} = %s")
                    params.append(data)
                params.append(new_timestamp)
                params.append(old_timestamp)
                query = f"""
                    UPDATE graph_status
                    SET {', '.join(set_parts)}, timestamp = %s
                    WHERE timestamp = %s
                """
                cursor.execute(query, params)
                self.target_conn.commit()
                print(
                    f"Successfully updated graph_status with {len(update_data)} columns"
                )
            return new_timestamp
        except Exception as e:
            self.target_conn.rollback()
            log.error(
                f"Error updating graph_status in target database: {e}\n{traceback.format_exc()}"
            )
            raise


if __name__ == "__main__":
    try:
        with GraphStatusProcessor(DB_CONFIG_SOURCE, DB_CONFIG_TARGET) as processor:
            print("Starting continuous processing every 5 seconds...")
            while True:
                start_time = time.time()
                processor.process_all_machines()
                elapsed_time = time.time() - start_time
                sleep_time = max(0, 5 - elapsed_time)
                print(
                    f"Finished processing cycle. Sleeping for {sleep_time:.2f} seconds..."
                )
                time.sleep(sleep_time)
    except KeyboardInterrupt:
        print("Process stopped by user")
    except Exception as e:
        log.error(f"Fatal error in main loop: {e}\n{traceback.format_exc()}")
        os._exit(1)
