import psycopg2
from psycopg2.extras import Json, RealDictCursor
import time
from datetime import datetime, timedelta, date
from datetime import timezone
import pytz
import traceback
import json
import os
import sys
import logging
import threading
import os.path
from pathlib import Path

# Removed global today_date - now calculated fresh in each method to avoid stale data


def setup_logging():
    log_base_dir = "logs"
    os.makedirs(log_base_dir, exist_ok=True)
    # Get fresh date for logging directory to avoid stale data after midnight
    today_date = date.today()
    log_dir = os.path.join(log_base_dir, today_date.strftime("%Y-%m-%d"))
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "loop3_monitor_new.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )


def load_config():
    try:
        with open("loop3_config.json") as config_file:
            config = json.load(config_file)
            return config
    except Exception as e:
        logging.error(f"Error loading config file: {e}")
        raise


class DatabaseConnection:
    def __init__(self, config, db_type="read"):
        self.conn_params = config["database"][db_type]
        self.conn = None
        self.cursor = None
        self.connect_db()

    def connect_db(self):
        while True:
            try:
                self.close_db_connection()
                self.conn = psycopg2.connect(**self.conn_params)
                self.cursor = self.conn.cursor()
                print("Successfully connected to PostgreSQL database")
                return True
            except Exception as e:
                logging.error(f"Database connection failed: {e}")
                os._exit(1)

    def close_db_connection(self):
        try:
            if hasattr(self, "cursor") and self.cursor:
                self.cursor.close()
        except Exception as e:
            print(f"Error closing cursor: {e}")
            os._exit(1)
        try:
            if hasattr(self, "conn") and self.conn:
                self.conn.close()
        except Exception as e:
            print(f"Error closing connection: {e}")
            os._exit(1)
        self.cursor = None
        self.conn = None

    def ensure_db_connection(self):
        while True:
            if (
                self.conn is None
                or self.conn.closed != 0
                or self.cursor is None
                or self.cursor.closed
            ):
                print("Database connection lost. Reconnecting...")
                return self.connect_db()
            return True

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()


class Loop3Monitor:
    def __init__(self, db, write_db, config):
        self.db = db
        self.write_db = write_db
        self.cursor = db.cursor
        self.india_timezone = pytz.timezone("Asia/Kolkata")
        self.config = config
        self.bboee = [81.0, 83.2, 84.0, 82.3, 89.6, 82.2]
        self.machines_config = config.get("machine_data", {}).get("machines", {})

        self.last_valid_mc17_data = None
        self.last_valid_mc18_data = None
        self.last_valid_mc19_data = None
        self.last_valid_mc20_data = None
        self.last_valid_mc21_data = None
        self.last_valid_mc22_data = None

        self.UPDATE_LOOP3_QUERY = """
            UPDATE loop3_overview_new
            SET
                last_update = %s,
                primary_tank_level = %s,
                secondary_tank_level = %s,
                bulk_settling_time = %s,
                cld_production = %s,
                ega = %s,
                taping_machine = %s,
                case_erector = %s,
                machines = %s,
                plantair = %s,
                planttemperature = %s,
                batch_sku = %s,
                cobot_status = 'Offline',
                checkweigher = %s
            WHERE shift = %s
        """

        self.INSERT_LOOP3_QUERY = """
            INSERT INTO loop3_overview_new (
                shift,
                last_update,
                primary_tank_level,
                secondary_tank_level,
                bulk_settling_time,
                cld_production,
                ega,
                taping_machine,
                case_erector,
                machines,
                plantair,
                planttemperature,
                batch_sku,
                cobot_status,
                checkweigher
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Offline', %s)
        """

        self.EGA_QUERY = """
            WITH query1 AS (
                SELECT timestamp, cld_weight, target_weight, lower_limit, upper_limit,
                    CASE
                        WHEN cld_weight > (target_weight - lower_limit) AND cld_weight < (target_weight + upper_limit) THEN 'ok'
                        ELSE 'notok'
                    END AS status
                FROM loop3_checkweigher
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
                ORDER BY timestamp DESC
            )
            SELECT
                CASE
                    WHEN COUNT(*) > 0 THEN
                        LEAST(50, GREATEST(-50, ((AVG(cld_weight) - AVG(target_weight)) / AVG(target_weight)) * 100))
                    ELSE 0
                END AS EGA
            FROM query1
            WHERE status = 'ok'
        """

        self.BULK_SETTLING_TIME_QUERY = """
            WITH query1 AS (
                SELECT timestamp, cld_weight,
                    CASE
                        WHEN cld_weight > (target_weight - lower_limit)
                        AND cld_weight < (target_weight + upper_limit) THEN 'ok'
                        ELSE 'notok'
                    END AS status
                FROM loop3_checkweigher
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
            ),
            sum_ok_weight AS (
                SELECT SUM(cld_weight) AS total_weight, COUNT(*) AS count_all
                FROM query1
                WHERE status = 'ok'
            ),
            cld_zero_check AS (
                SELECT COUNT(*) = 0 AS is_zero
                FROM query1
            )
            SELECT
                CASE
                    WHEN primary_tank = 0 AND secondary_tank = 0 THEN NULL
                    WHEN is_zero THEN NULL
                    ELSE (
                        ((primary_tank / 10.0) + (secondary_tank / 10.0)) /
                        GREATEST(s.total_weight, 1)
                    ) * 1000
                END AS result
            FROM loop3_sku
            CROSS JOIN sum_ok_weight s
            CROSS JOIN cld_zero_check c
            WHERE DATE(timestamp) = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.CLD_PRODUCTION_QUERY_TEMPLATE = """
            SELECT mc{mc}, timestamp
            FROM loop3_checkpoints
            WHERE mc{mc} != 'null'
            AND timestamp >= NOW() - INTERVAL '15 minutes'
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.QUERY_PRIMARY_SECONDARY_TANK = """
            SELECT
                CASE WHEN primary_tank = 0 THEN 0 ELSE primary_tank/10 END,
                CASE WHEN secondary_tank = 0 THEN 0 ELSE secondary_tank/10 END
            FROM loop3_sku
            WHERE DATE(timestamp) = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.QUERY_TAPING_MACHINE = """
            SELECT loop3_status
            FROM taping
            WHERE DATE(timestamp) = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.QUERY_CASE_ERECTOR = """
            SELECT loop3_status
            FROM case_erector
            WHERE DATE(timestamp) = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.QUERY_EXTRACT_LATEST_PLANT_DATA = """
            SELECT plant_params, timestamp
            FROM dark_cascade_overview
            WHERE DATE(timestamp) >= CURRENT_DATE
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.hopper_level_query = """
            SELECT hopper_1_level, hopper_2_level
            FROM mc{mc}
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
            LIMIT 1
        """

        self.GET_MACHINE_STATUS_QUERY = """
            SELECT status, timestamp
            FROM mc{mc}
            WHERE timestamp >= NOW() - INTERVAL '5 minutes'
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.GET_checkweigher_STATUS_QUERY = """
            SELECT loop3
            FROM checkweigher_status
        """

        self.GET_CHECKPOINT_QUERY = """
            SELECT mc{mc}, timestamp
            FROM loop3_checkpoints
            WHERE mc{mc} != 'null'
            AND timestamp >= NOW() - INTERVAL '15 minutes'
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.GET_MACHINE_UPTIME = """
            SELECT COALESCE(ch.machine_uptime, INTERVAL '00:00:00')
            FROM (SELECT INTERVAL '00:00:00' AS default_uptime) fallback
            LEFT JOIN cld_history ch
                ON DATE(ch.timestamp) = CURRENT_DATE
                AND ch.shift = %s
                AND ch.machine_no = %s
        """

        self.BATCH_SKU_SHIFT_QUERY = """
            SELECT batch_sku, shift
            FROM loop3_sku
            WHERE timestamp >= NOW() - INTERVAL '5 minutes'
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.GET_STOPPAGE_TIME = """
            SELECT
                mc{mc} ->> 'stoppage_time' AS stoppage_time,
                mc{mc} ->> 'stoppage_count' AS stoppage_count,
                mc{mc} ->> 'longest_duration' AS longest_duration,
                mc{mc} ->> 'highest_count' AS highest_count
            FROM machine_stoppage_status
            WHERE shift = %s
            ORDER BY timestamp DESC
            LIMIT 2
        """

        self.GET_PREV_STOPPAGE_TIME = """
            SELECT
                mc{mc} ->> 'stoppage_time' AS stoppage_time,
                mc{mc} ->> 'stoppage_count' AS stoppage_count,
                mc{mc} ->> 'longest_duration' AS longest_duration,
                mc{mc} ->> 'highest_count' AS highest_count
            FROM machine_stoppage_status
            ORDER BY timestamp DESC
            LIMIT 2
        """

        self.GET_MACHINE_DATA_QUERY = """
            SELECT
                time_bucket('5 minutes', timestamp) AS time_bucket,
                {ver_pressure_select},
                {hor_pressure_select},
                {hopper_1_level_select},
                {hopper_2_level_select},
                {status_column} AS status,
                timestamp,
                EXTRACT(EPOCH FROM timestamp) AS epoch_timestamp
            FROM {table}
            WHERE timestamp > NOW() - '5 minutes'::interval
            GROUP BY time_bucket, {status_column}, timestamp
            ORDER BY time_bucket DESC
            LIMIT 1
        """

        self.GET_MACHINE_MID_DATA_QUERY = """
            SELECT
                time_bucket('5 minutes', timestamp) AS time_bucket,
                AVG(({vertical_sealer_rear_sum}) / {vertical_sealer_rear_count}) AS avg_vertical_rear_temp,
                AVG(({vertical_sealer_front_sum}) / {vertical_sealer_front_count}) AS avg_vertical_front_temp,
                AVG({horizontal_sealer_rear_column}) AS avg_horizontal_rear_temp,
                AVG({horizontal_sealer_front_column}) AS avg_horizontal_front_temp,
                timestamp
            FROM {mid_table}
            WHERE timestamp > NOW() - '5 minutes'::interval
            GROUP BY time_bucket, timestamp
            ORDER BY time_bucket DESC
            LIMIT 1
        """

        self.LAMINATE_LENGTH_QUERY = """
            SELECT COALESCE(SUM(waste_amount_cm), 0) AS total_waste_amount_cm,
                COALESCE(TO_CHAR(SUM(TO_TIMESTAMP(duration, 'MI:SS') - TO_TIMESTAMP('00:00', 'MI:SS')), 'MI:SS'), '00:00') AS total_duration
            FROM laminate_waste_events
            WHERE machine = %s
            AND timestamp >= %s AND timestamp < %s
            AND waste_amount_cm > 0
        """

    def construct_machine_data_query(self, machine, shift):
        if machine not in self.machines_config:
            logging.error(f"No configuration found for machine {machine}")
            return None, None

        config = self.machines_config[machine]

        ver_pressure_select = (
            f"AVG({config['ver_pressure_column']}) AS avg_ver_pressure"
            if config["ver_pressure_column"]
            else "COALESCE(NULL, 0) AS avg_ver_pressure"
        )
        hor_pressure_select = (
            f"AVG({config['hor_pressure_column']}) AS avg_hor_pressure"
            if config["hor_pressure_column"]
            else "COALESCE(NULL, 0) AS avg_hor_pressure"
        )
        hopper_1_level_select = (
            f"AVG({config['hopper_1_level_column']}) AS avg_hopper_1_level"
            if config["hopper_1_level_column"]
            else "COALESCE(NULL, 0) AS avg_hopper_1_level"
        )
        hopper_2_level_select = (
            f"AVG({config['hopper_2_level_column']}) AS avg_hopper_2_level"
            if config["hopper_2_level_column"]
            else "COALESCE(NULL, 0) AS avg_hopper_2_level"
        )

        main_query = self.GET_MACHINE_DATA_QUERY.format(
            table=config["table"],
            status_column=config["status_column"],
            ver_pressure_select=ver_pressure_select,
            hor_pressure_select=hor_pressure_select,
            hopper_1_level_select=hopper_1_level_select,
            hopper_2_level_select=hopper_2_level_select,
        )

        mid_query = None
        if "mid_table" in config:
            vertical_sealer_rear_sum = " + ".join(
                config["vertical_sealer_rear_columns"]
            )
            vertical_sealer_front_sum = " + ".join(
                config["vertical_sealer_front_columns"]
            )
            vertical_sealer_rear_count = len(config["vertical_sealer_rear_columns"])
            vertical_sealer_front_count = len(config["vertical_sealer_front_columns"])

            mid_query = self.GET_MACHINE_MID_DATA_QUERY.format(
                mid_table=config["mid_table"],
                vertical_sealer_rear_sum=vertical_sealer_rear_sum,
                vertical_sealer_front_sum=vertical_sealer_front_sum,
                vertical_sealer_rear_count=vertical_sealer_rear_count,
                vertical_sealer_front_count=vertical_sealer_front_count,
                horizontal_sealer_rear_column=config["horizontal_sealer_rear_column"],
                horizontal_sealer_front_column=config["horizontal_sealer_front_column"],
            )

        return main_query, mid_query

    def get_shift_and_time_range(self, current_time):
        config = load_config()
        shift_config = config["shifts"]

        current_hour = current_time.hour

        if (
            current_hour >= shift_config["A"]["start_hour"]
            and current_hour < shift_config["A"]["end_hour"]
        ):
            shift = "A"
            json_key = shift_config["A"]["loop3_name"]
            start_time = current_time.replace(
                hour=shift_config["A"]["start_hour"], minute=0, second=0, microsecond=0
            )
            end_time = current_time.replace(
                hour=shift_config["A"]["end_hour"], minute=0, second=0, microsecond=0
            )
        elif (
            current_hour >= shift_config["B"]["start_hour"]
            and current_hour < shift_config["B"]["end_hour"]
        ):
            shift = "B"
            json_key = shift_config["B"]["loop3_name"]
            start_time = current_time.replace(
                hour=shift_config["B"]["start_hour"], minute=0, second=0, microsecond=0
            )
            end_time = current_time.replace(
                hour=shift_config["B"]["end_hour"], minute=0, second=0, microsecond=0
            )
        else:
            shift = "C"
            json_key = shift_config["C"]["loop3_name"]
            if current_hour < shift_config["C"]["end_hour"]:
                start_time = (current_time - timedelta(days=1)).replace(
                    hour=shift_config["C"]["start_hour"],
                    minute=0,
                    second=0,
                    microsecond=0,
                )
                end_time = current_time.replace(
                    hour=shift_config["C"]["end_hour"],
                    minute=0,
                    second=0,
                    microsecond=0,
                )
            else:
                start_time = current_time.replace(
                    hour=shift_config["C"]["start_hour"],
                    minute=0,
                    second=0,
                    microsecond=0,
                )
                end_time = current_time.replace(
                    hour=shift_config["C"]["end_hour"],
                    minute=0,
                    second=0,
                    microsecond=0,
                ) + timedelta(days=1)

        return shift, json_key, start_time, end_time

    def get_machine_data(self, mc, json_key, start_time, end_time, shift):
        print("\n---------------------------------------------------")
        print(f"\n\n machine number : {mc}")
        print("\n---------------------------------------------------")

        try:
            shift_value = json_key.split("_Data")[0]
            machine_no = f"mc{mc}"
            print("shift ", shift)

            self.db.cursor.execute(self.GET_MACHINE_UPTIME, (shift_value, machine_no))
            machine_uptime = self.db.cursor.fetchone()
            machine_uptime = machine_uptime[0] if machine_uptime else timedelta()

            self.db.cursor.execute(self.GET_CHECKPOINT_QUERY.format(mc=mc))
            checkpoint_data = self.db.cursor.fetchone()

            speed = 100
            cld_production = 0
            HMI_VER_OPEN_START = HMI_VER_CLOSE_END = HMI_HOZ_OPEN_START = (
                HMI_HOZ_CLOSE_END
            ) = 0.0

            if checkpoint_data and isinstance(checkpoint_data[0], dict):
                speed = checkpoint_data[0].get("Machine_Speed_PPM", 100)
                cld_production = checkpoint_data[0].get(json_key, 0) or 0
                print(f"\nCld count : {cld_production}")
                HMI_VER_OPEN_START = checkpoint_data[0].get("HMI_VER_OPEN_START", 0.0)
                HMI_VER_CLOSE_END = checkpoint_data[0].get("HMI_VER_CLOSE_END", 0.0)
                HMI_HOZ_OPEN_START = checkpoint_data[0].get("HMI_HOZ_OPEN_START", 0.0)
                HMI_HOZ_CLOSE_END = checkpoint_data[0].get("HMI_HOZ_CLOSE_END", 0.0)

            sealing_time_vertical = (
                (HMI_VER_OPEN_START - HMI_VER_CLOSE_END) * (500 / (3 * speed))
                if speed != 0
                else 0.0
            )
            sealing_time_horizontal = (
                (HMI_HOZ_OPEN_START - HMI_HOZ_CLOSE_END) * (500 / (3 * speed))
                if speed != 0
                else 0.0
            )

            self.cursor.execute(
                self.LAMINATE_LENGTH_QUERY, (machine_no, start_time, end_time)
            )
            result = self.cursor.fetchall()

            if result:
                total_waste_amount_cm, empty_duration = result[0]
                laminate_length = total_waste_amount_cm / 100
            else:
                laminate_length = 0
                empty_duration = "00:00"

            print("laminate_length = ", laminate_length)
            print("empty duration ", empty_duration)

            self.db.cursor.execute(self.GET_STOPPAGE_TIME.format(mc=mc), (shift,))
            stoppage_minutes, stoppage_data, longest_duration, highest_count = (
                self.db.cursor.fetchone()
            )

            self.db.cursor.execute(self.GET_PREV_STOPPAGE_TIME.format(mc=mc))
            results = self.db.cursor.fetchall()

            prev_stoppage_minutes = prev_stoppage_data = prev_longest_duration = (
                prev_highest_count
            ) = None
            if len(results) >= 2:
                result = results[1]
                prev_stoppage_minutes = result[0]
                prev_stoppage_data = result[1]
                prev_longest_duration = result[2]
                prev_highest_count = result[3]

            if longest_duration or prev_longest_duration:
                try:
                    longest_duration = json.loads(longest_duration)
                    prev_longest_duration = (
                        json.loads(prev_longest_duration)
                        if prev_longest_duration
                        else {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                    )
                except json.JSONDecodeError:
                    longest_duration = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                    prev_longest_duration = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}

            if highest_count or prev_highest_count:
                try:
                    highest_count = json.loads(highest_count)
                    prev_highest_count = (
                        json.loads(prev_highest_count)
                        if prev_highest_count
                        else {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                    )
                except json.JSONDecodeError:
                    highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                    prev_highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}

            print("longest stoppage time ", longest_duration)
            print("prev longest stoppage time ", prev_longest_duration)

            current_time = datetime.now(self.india_timezone)
            time_difference_hours = (current_time - start_time).total_seconds() / 3600
            print("time diff = ", time_difference_hours)
            print("cld count = ", cld_production)
            BBOEE = (
                ((cld_production * 100) / (600 * time_difference_hours)) * 8
                if time_difference_hours > 0
                else 0
            )
            print("\n BBOEE = ", BBOEE)
            BBOEE = max(0, min(BBOEE, 100))

            self.db.cursor.execute(self.GET_MACHINE_STATUS_QUERY.format(mc=mc))
            result = self.db.cursor.fetchone()

            if result:
                status_value, timestamp_value = result
            else:
                status_value = 2
                timestamp_value = None

            if timestamp_value:
                time_difference = current_time - timestamp_value
                print(f"Current Time : {current_time}")
                print(f"DB Timestamp : {timestamp_value}")
                print(f"Time Difference : {time_difference.total_seconds()} seconds")
                if time_difference.total_seconds() > 220:
                    status_message = "Offline"
                else:
                    status_message = "Running" if status_value == 1 else "Stopped"
            else:
                print("No recent data found in the last 5 minutes.")
                status_message = "Offline"

            print("Machine Status = ", status_message)

            main_query, mid_query = self.construct_machine_data_query(f"mc{mc}", shift)
            if not main_query:
                raise ValueError(f"Failed to construct main query for {mc}")

            self.db.cursor.execute(main_query)
            main_data = self.db.cursor.fetchone()

            mid_data = None
            if mid_query:
                self.db.cursor.execute(mid_query)
                mid_data = self.db.cursor.fetchone()

            self.db.cursor.execute(self.hopper_level_query.format(mc=mc))
            hopper_data = self.db.cursor.fetchone()

            mc_data = {
                "speed": max(speed if speed is not None else 100, 100),
                "laminate_length": round(laminate_length, 2),
                "laminate_duration": str(empty_duration),
                "status": status_message,
                "jamming": True,
                "pulling": True,
                "sealing": True,
                "sealing_time_vertical": (
                    166.67
                    if sealing_time_vertical is None
                    else round(sealing_time_vertical, 2)
                ),
                "sealing_time_horizontal": (
                    259.26
                    if sealing_time_horizontal is None
                    else round(sealing_time_horizontal, 2)
                ),
                "stoppage_time": (
                    stoppage_minutes if stoppage_minutes is not None else 0
                ),
                "longest_duration": longest_duration,
                "cld_production": round(cld_production, 2),
                "stoppage_count": (
                    int(stoppage_data) if stoppage_data is not None else 0
                ),
                "highest_count": highest_count,
                "prev_highest_count": prev_highest_count,
                "outgoing_quality": True,
                "vertical_pressure": (
                    round(main_data[1], 2)
                    if main_data and main_data[1] is not None
                    else 0
                ),
                "horizontal_pressure": (
                    round(main_data[2], 2)
                    if main_data and main_data[2] is not None
                    else 0
                ),
                "hopper_level_primary": (
                    round(main_data[3], 2)
                    if main_data and main_data[3] is not None
                    else 0
                ),
                "hopper_level_secondary": (
                    round(main_data[4], 2)
                    if main_data and main_data[4] is not None
                    else 0
                ),
                "vertical_rear_temp": (
                    round(mid_data[1], 2) if mid_data and mid_data[1] is not None else 0
                ),
                "vertical_front_temp": (
                    round(mid_data[2], 2) if mid_data and mid_data[2] is not None else 0
                ),
                "horizontal_rear_temp": (
                    round(mid_data[3], 2)
                    if mid_data and mid_data[3] is not None
                    else (
                        round(main_data[6], 2)
                        if main_data and main_data[6] is not None and not mid_query
                        else 0
                    )
                ),
                "horizontal_front_temp": (
                    round(mid_data[4], 2)
                    if mid_data and mid_data[4] is not None
                    else (
                        round(main_data[7], 2)
                        if main_data and main_data[7] is not None and not mid_query
                        else 0
                    )
                ),
                "breakdown_prediction": True,
                "productivity_prediction": True,
                "machine_uptime": str(machine_uptime),
                "bboee": round(BBOEE, 2) if BBOEE is not None else 0,
                "prev_stoppage_time": (
                    prev_stoppage_minutes if prev_stoppage_minutes is not None else 0
                ),
                "prev_stoppage_count": (
                    int(prev_stoppage_data) if prev_stoppage_data is not None else 0
                ),
                "prev_longest_duration": prev_longest_duration,
            }

            if main_data or mid_data:
                setattr(self, f"last_valid_mc{mc}_data", mc_data)
            else:
                print(f"No data found for {mc}")
                last_valid_data = getattr(self, f"last_valid_mc{mc}_data", None)
                mc_data = (
                    last_valid_data
                    if last_valid_data
                    else {
                        "speed": 0,
                        "laminate_length": round(laminate_length, 2),
                        "laminate_duration": str(empty_duration),
                        "status": "Offline",
                        "jamming": True,
                        "pulling": True,
                        "sealing": True,
                        "sealing_time_vertical": (
                            166.67
                            if sealing_time_vertical is None
                            else round(sealing_time_vertical, 2)
                        ),
                        "sealing_time_horizontal": (
                            259.26
                            if sealing_time_horizontal is None
                            else round(sealing_time_horizontal, 2)
                        ),
                        "stoppage_time": (
                            stoppage_minutes if stoppage_minutes is not None else 0
                        ),
                        "longest_duration": longest_duration,
                        "cld_production": round(cld_production, 2),
                        "stoppage_count": (
                            int(stoppage_data) if stoppage_data is not None else 0
                        ),
                        "highest_count": highest_count,
                        "prev_highest_count": prev_highest_count,
                        "outgoing_quality": True,
                        "vertical_pressure": 0.0,
                        "horizontal_pressure": 0.0,
                        "hopper_level_primary": 0.0,
                        "hopper_level_secondary": 0.0,
                        "vertical_rear_temp": 0.0,
                        "vertical_front_temp": 0.0,
                        "horizontal_rear_temp": 0.0,
                        "horizontal_front_temp": 0.0,
                        "breakdown_prediction": True,
                        "productivity_prediction": True,
                        "machine_uptime": str(machine_uptime),
                        "bboee": round(BBOEE, 2) if BBOEE is not None else 0,
                        "prev_stoppage_time": (
                            prev_stoppage_minutes
                            if prev_stoppage_minutes is not None
                            else 0
                        ),
                        "prev_stoppage_count": (
                            int(prev_stoppage_data)
                            if prev_stoppage_data is not None
                            else 0
                        ),
                        "prev_longest_duration": prev_longest_duration,
                    }
                )

            return mc_data

        except Exception as e:
            print(f"Error occurred: {e}")
            logging.error(f"Error processing {mc}: {e}")
            mc_data = {
                "speed": 0,
                "laminate_length": round(laminate_length, 2),
                "laminate_duration": str(empty_duration),
                "status": "Offline",
                "jamming": True,
                "pulling": True,
                "sealing": True,
                "sealing_time_vertical": (
                    166.67
                    if sealing_time_vertical is None
                    else round(sealing_time_vertical, 2)
                ),
                "sealing_time_horizontal": (
                    259.26
                    if sealing_time_horizontal is None
                    else round(sealing_time_horizontal, 2)
                ),
                "stoppage_time": (
                    stoppage_minutes if stoppage_minutes is not None else 0
                ),
                "longest_duration": longest_duration,
                "cld_production": round(cld_production, 2),
                "stoppage_count": (
                    int(stoppage_data) if stoppage_data is not None else 0
                ),
                "highest_count": highest_count,
                "prev_highest_count": prev_highest_count,
                "outgoing_quality": True,
                "vertical_pressure": 0,
                "horizontal_pressure": 0,
                "hopper_level_primary": 0,
                "hopper_level_secondary": 0,
                "vertical_rear_temp": 0,
                "vertical_front_temp": 0,
                "horizontal_rear_temp": 0,
                "horizontal_front_temp": 0,
                "breakdown_prediction": True,
                "productivity_prediction": True,
                "machine_uptime": str(machine_uptime),
                "bboee": round(BBOEE, 2) if BBOEE is not None else 0,
                "prev_stoppage_time": (
                    prev_stoppage_minutes if prev_stoppage_minutes is not None else 0
                ),
                "prev_stoppage_count": (
                    int(prev_stoppage_data) if prev_stoppage_data is not None else 0
                ),
                "prev_longest_duration": prev_longest_duration,
            }
            os._exit(1)

    def get_machine_17(self, json_key, start_time, end_time, shift):
        return self.get_machine_data("17", json_key, start_time, end_time, shift)

    def get_machine_18(self, json_key, start_time, end_time, shift):
        return self.get_machine_data("18", json_key, start_time, end_time, shift)

    def get_machine_19(self, json_key, start_time, end_time, shift):
        return self.get_machine_data("19", json_key, start_time, end_time, shift)

    def get_machine_20(self, json_key, start_time, end_time, shift):
        return self.get_machine_data("20", json_key, start_time, end_time, shift)

    def get_machine_21(self, json_key, start_time, end_time, shift):
        return self.get_machine_data("21", json_key, start_time, end_time, shift)

    def get_machine_22(self, json_key, start_time, end_time, shift):
        return self.get_machine_data("22", json_key, start_time, end_time, shift)

    def extract_latest_plant_data(self):
        try:
            self.db.cursor.execute(self.QUERY_EXTRACT_LATEST_PLANT_DATA)
            result = self.db.cursor.fetchone()

            if result:
                plant_params = result[0]
                timestamp = result[1]
                if isinstance(plant_params, dict):
                    plantair = plant_params.get("PLANTAIR", 0.0)
                    planttemperature = plant_params.get("PLANT_TEMPERATURE", 0.0)
                    plantair = plantair if plantair is not None else 0.0
                    planttemperature = (
                        planttemperature if planttemperature is not None else 0.0
                    )
                    return plantair, planttemperature
            return 0.0, 0.0
        except Exception as e:
            print("error", e)
            logging.error(f"Error extracting plant data: {e}")
            self.db.rollback()
            os._exit(1)

    def fetch_latest_values(self, start_time, end_time, shift, json_key):
        try:
            # Get fresh today_date to avoid stale data after midnight
            today_date = datetime.now(self.india_timezone).date()

            self.db.cursor.execute(self.QUERY_PRIMARY_SECONDARY_TANK, (today_date,))
            result = self.db.cursor.fetchone()
            primary_tank, secondary_tank = (result[0], result[1]) if result else (0, 0)

            print("primary tank ", primary_tank)
            print("secondary tank ", secondary_tank)

            self.db.cursor.execute(self.QUERY_TAPING_MACHINE, (today_date,))
            taping_machine = self.db.cursor.fetchone()
            taping_machine = (
                int(taping_machine[0])
                if taping_machine and taping_machine[0] is not None
                else 2
            )
            print("taping machine = ", taping_machine)

            self.db.cursor.execute(self.QUERY_CASE_ERECTOR, (today_date,))
            case_erector = self.db.cursor.fetchone()
            case_erector = (
                int(case_erector[0])
                if case_erector and case_erector[0] is not None
                else 2
            )
            print("case erector = ", case_erector)

            self.db.cursor.execute(self.GET_checkweigher_STATUS_QUERY)
            row = self.db.cursor.fetchone()
            checkweigher = row[0] if row and row[0] is not None else "Offline"
            if checkweigher not in ["Running", "Stopped", "Offline"]:
                logging.warning(
                    f"Invalid checkweigher status {checkweigher}, defaulting to 'Offline'"
                )
                checkweigher = "Offline"
            print(f"Checkweigher Status: {checkweigher}")

            machines_data = {
                "mc17": self.get_machine_17(json_key, start_time, end_time, shift),
                "mc18": self.get_machine_18(json_key, start_time, end_time, shift),
                "mc19": self.get_machine_19(json_key, start_time, end_time, shift),
                "mc20": self.get_machine_20(json_key, start_time, end_time, shift),
                "mc21": self.get_machine_21(json_key, start_time, end_time, shift),
                "mc22": self.get_machine_22(json_key, start_time, end_time, shift),
            }

            return (
                primary_tank,
                secondary_tank,
                taping_machine,
                case_erector,
                machines_data,
                checkweigher,
            )

        except Exception as e:
            logging.error(f"Error fetching latest values: {e}")
            self.db.rollback()
            os._exit(1)

    def fetch_batch_sku_and_shift(self):
        try:
            self.db.cursor.execute(self.BATCH_SKU_SHIFT_QUERY)
            result = self.db.cursor.fetchone()
            if result:
                batch_sku, shift = result
                if shift not in ["A", "B", "C"]:
                    shift = "A"
                return batch_sku, shift
            return " ", "A"
        except Exception as e:
            logging.error(f"Error fetching batch_sku and shift: {e}")
            self.db.rollback()
            os._exit(1)

    def get_previous_shift(self, current_shift):
        shift_sequence = {"A": "C", "B": "A", "C": "B"}
        return shift_sequence.get(current_shift, "A")

    def calculate_ega(self):
        try:
            self.db.cursor.execute(self.EGA_QUERY)
            result = self.db.cursor.fetchone()
            return result[0] if result and result[0] is not None else 0.0
        except Exception as e:
            logging.error(f"Error calculating EGA: {e}")
            self.db.rollback()
            os._exit(1)

    def calculate_bulk_settling_time(self):
        try:
            today_date = datetime.now(self.india_timezone).date()
            self.db.cursor.execute(self.BULK_SETTLING_TIME_QUERY, (today_date,))
            result = self.db.cursor.fetchone()
            bulk_settling_time = (
                0 if result is None or result[0] is None else int(result[0])
            )
            bulk_settling_time = max(0, min(bulk_settling_time, 99))
            print("bulk settling time ", bulk_settling_time)
            return bulk_settling_time
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logging.error(
                f"Error in calculate_bulk_settling_time: {exc_type} in {fname} at line {exc_tb.tb_lineno}: {e}"
            )
            os._exit(1)

    def calculate_cld_production(self, json_key):
        try:
            cld_production = 0
            for mc in range(17, 23):
                query = self.CLD_PRODUCTION_QUERY_TEMPLATE.format(mc=mc)
                self.db.cursor.execute(query)
                result = self.db.cursor.fetchone()
                if result and isinstance(result[0], dict):
                    cld_production += result[0].get(json_key, 0)
            print("\nTotal CLD Production:", cld_production)
            return cld_production
        except Exception as e:
            logging.error(f"Error calculating CLD production: {e}")
            self.db.rollback()
            os._exit(1)

    def update_loop3(self):
        try:
            self.db.ensure_db_connection()
            self.write_db.ensure_db_connection()
            current_time = datetime.now(self.india_timezone)
            shift, json_key, start_time, end_time = self.get_shift_and_time_range(
                current_time
            )

            bulk_settling_time = self.calculate_bulk_settling_time()
            cld_production = self.calculate_cld_production(json_key)
            ega = self.calculate_ega()

            (
                primary_tank,
                secondary_tank,
                taping_machine,
                case_erector,
                machines_data,
                checkweigher,
            ) = self.fetch_latest_values(start_time, end_time, shift, json_key)
            plantair, planttemperature = self.extract_latest_plant_data()

            batch_sku, _ = self.fetch_batch_sku_and_shift()  # Ignore database shift, use calculated shift

            self.write_db.cursor.execute(
                self.UPDATE_LOOP3_QUERY,
                (
                    current_time,
                    round(primary_tank, 2),
                    round(secondary_tank, 2),
                    int(bulk_settling_time),
                    round(cld_production, 2),
                    round(ega, 2),
                    taping_machine,
                    case_erector,
                    Json(machines_data),
                    round(plantair, 2),
                    round(planttemperature, 2),
                    batch_sku,
                    checkweigher,
                    shift,  # shift parameter moved to end for WHERE clause
                ),
            )

            if self.write_db.cursor.rowcount == 0:
                logging.info("\n\nNo existing row updated, inserting new row.")
                self.insert_into_loop3(
                    current_time,
                    primary_tank,
                    secondary_tank,
                    bulk_settling_time,
                    cld_production,
                    ega,
                    taping_machine,
                    case_erector,
                    machines_data,
                    plantair,
                    planttemperature,
                    batch_sku,
                    shift,
                    checkweigher,
                )

            self.write_db.commit()
            logging.info(f"\n\nSuccessfully updated loop3_overview at {current_time}")

        except Exception as e:
            error_message = str(e)
            self.db.rollback()  # Rollback read database
            self.write_db.rollback()

            if "violates check constraint" in error_message or "chunk" in error_message:
                logging.warning(
                    f"\n\nHypertable chunk constraint violated, inserting instead. Error: {error_message}"
                )

                try:
                    self.insert_into_loop3(
                        current_time,
                        primary_tank,
                        secondary_tank,
                        bulk_settling_time,
                        cld_production,
                        ega,
                        taping_machine,
                        case_erector,
                        machines_data,
                        plantair,
                        planttemperature,
                        batch_sku,
                        shift,
                        checkweigher,
                    )
                except Exception as insert_error:
                    logging.error(
                        f"\n\nError inserting loop3_overview data after hypertable error: {insert_error}"
                    )
                    self.db.rollback()
                    self.write_db.rollback()
                    os._exit(1)
            else:
                logging.error(f"\n\nError in update_loop3_overview : {e}")
                # self.write_db.rollback()
                os._exit(1)

    def insert_into_loop3(
        self,
        current_time,
        primary_tank,
        secondary_tank,
        bulk_settling_time,
        cld_production,
        ega,
        taping_machine,
        case_erector,
        machines_data,
        plantair,
        planttemperature,
        batch_sku,
        shift,
        checkweigher,
    ):
        try:
            self.write_db.ensure_db_connection()
            self.write_db.cursor.execute(
                self.INSERT_LOOP3_QUERY,
                (
                    shift,  # shift parameter moved to first position
                    current_time,
                    round(primary_tank, 2),
                    round(secondary_tank, 2),
                    int(bulk_settling_time),
                    round(cld_production, 2),
                    round(ega, 2),
                    taping_machine,
                    case_erector,
                    Json(machines_data),
                    round(plantair, 2),
                    round(planttemperature, 2),
                    batch_sku,
                    checkweigher,
                ),
            )
            self.write_db.commit()
            print(
                f"Inserted data at {current_time} with bulk settling time {bulk_settling_time}"
            )
        except Exception as e:
            logging.error(f"\n\n Error inserting into loop3_overview : {e}")
            self.write_db.rollback()
            os._exit(1)


def main():
    try:
        setup_logging()
        config = load_config()
        db = DatabaseConnection(config, "read")
        write_db = DatabaseConnection(config, "write")
        db.connect_db()
        write_db.connect_db()
        monitor = Loop3Monitor(db, write_db, config)

        data_flow_thread = threading.Thread(target=db.ensure_db_connection, daemon=True)
        data_flow_thread.start()

        while True:
            try:
                monitor.update_loop3()
            except Exception as e:
                logging.error(f"\nError in main loop : {e}")
                db.rollback()
                write_db.rollback()
                os._exit(1)
            finally:
                logging.info("\nSleeping for 5 seconds before the next iteration...")
                time.sleep(5)
    except KeyboardInterrupt:
        logging.info("Shutting down Loop3 monitor...")
        os._exit(1)
    finally:
        db.close_db_connection()
        write_db.close_db_connection()


if __name__ == "__main__":
    main()
