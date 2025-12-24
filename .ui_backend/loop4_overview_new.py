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
import json

# Removed global today_date - now calculated fresh in each method to avoid stale data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('loop4_monitor_new.log'),
        logging.StreamHandler()
    ]
)

def load_config():
    try:
        with open('loop_config.json') as config_file:
            config = json.load(config_file)
            return config
    except Exception as e:
        logging.error(f"Error loading config file: {e}")
        raise

class DatabaseConnection:
    def __init__(self, config , db_type = "read"):
        self.conn_params =  config['database'][db_type]
        self.conn = None
        self.cursor = None
        self.connect_db()

    def connect_db(self):
        while True:
            try:
                self.close_db_connection()
                self.conn = psycopg2.connect(**self.conn_params)
                self.cursor = self.conn.cursor()
                logging.info("Database connection established successfully")
                print("Successfully connected to PostgreSQL database")
                return True
            except Exception as e:
                logging.error(f"Database connection failed: {e}")
                os._exit(1)

    def close_db_connection(self):
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
        except Exception as e:
            print(f"Error closing cursor: {e}")
            os._exit(1)
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
        except Exception as e:
            print(f"Error closing connection: {e}")
            os._exit(1)
        self.cursor = None
        self.conn = None

    def ensure_db_connection(self):
        """Lightweight check to ensure we have a valid database connection."""
        while True:

            if self.conn is None or self.conn.closed != 0 or self.cursor is None or self.cursor.closed:
                print("Database connection lost. Reconnecting...")
                return self.connect_db()
            return True

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()

class Loop4Monitor:
    def __init__(self, db,write_db,config):
        self.db = db
        self.write_db = write_db
        self.cursor = db.cursor
        self.india_timezone = pytz.timezone("Asia/Kolkata")

        self.last_valid_mc25_data = None
        self.last_valid_mc26_data = None
        self.last_valid_mc27_data = None
        self.last_valid_mc28_data = None
        self.last_valid_mc29_data = None
        self.last_valid_mc30_data = None

        self.UPDATE_LOOP4_QUERY = """
            UPDATE loop4_overview_new
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
                batch_sku = %s,
                cobot_status = %s,
                checkweigher = %s
            WHERE shift = %s
        """

        self.INSERT_LOOP4_QUERY = """
            INSERT INTO loop4_overview_new (
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
                batch_sku,
                cobot_status,
                checkweigher
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
        """

        self.EGA_QUERY = """
            with query1 as (select timestamp,cld_weight,target_weight,lower_limit,upper_limit,
                CASE
                when cld_weight > (target_weight - lower_limit) and cld_weight < (target_weight + upper_limit) then 'ok'
                else 'notok'
                end as status
                FROM loop4_checkweigher
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
                order by timestamp desc )
                SELECT
                    CASE
                WHEN COUNT(*) > 0 THEN
                    LEAST(50, GREATEST(-50,
                    ((AVG(cld_weight) - AVG(target_weight)) / AVG(target_weight)) * 100
                 ))
                ELSE 0
                END AS EGA
                FROM query1
                where status = 'ok'
        """

        self.BULK_SETTLING_TIME_QUERY = """
            WITH query1 AS (
                SELECT timestamp, cld_weight,
                    CASE
                        WHEN cld_weight > (target_weight - lower_limit)
                        AND cld_weight < (target_weight + upper_limit) THEN 'ok'
                        ELSE 'notok'
                    END AS status
                FROM loop4_checkweigher
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
            ),
            sum_ok_weight AS (
                SELECT SUM(cld_weight) AS total_weight,COUNT(*) AS count_all
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
                    WHEN is_zero THEN NULL -- Changed from 12.0 to NULL
                    ELSE
                        (
                            ((primary_tank * 0.08) + (secondary_tank * 0.08)) /
                            GREATEST(s.total_weight, 1)
                        ) * 1000
                END AS result
            FROM loop4_sku
            CROSS JOIN sum_ok_weight s
            CROSS JOIN cld_zero_check c
            WHERE DATE(timestamp) = %s
            ORDER BY timestamp DESC
            LIMIT 1;
        """


        self.CLD_PRODUCTION_QUERY_TEMPLATE = """
            SELECT timestamp, {column}
            FROM {mc_table}
            WHERE timestamp >= NOW() - INTERVAL '5 minutes'
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.QUERY_PRIMARY_SECONDARY_TANK = """
            SELECT
                CASE WHEN primary_tank = 0 THEN 0 ELSE (primary_tank)*0.08 END,
                CASE WHEN secondary_tank = 0 THEN 0 ELSE (secondary_tank)*0.08 END
            FROM loop4_sku
            WHERE DATE(timestamp) = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.QUERY_TAPING_MACHINE = """
            SELECT loop4_status
            FROM taping
            WHERE DATE(timestamp) = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.QUERY_CASE_ERECTOR = """
            SELECT loop4_status
            FROM case_erector
            WHERE DATE(timestamp) = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.GET_MACHINE_STATUS_QUERY = """
            SELECT {cld_column}, timestamp ,status FROM {mc}
            WHERE timestamp >= NOW() - INTERVAL '5 minutes'
            ORDER BY timestamp DESC
            LIMIT 1
        """

        self.GET_checkweigher_STATUS_QUERY = """
            SELECT loop4
            FROM checkweigher_status
        """

        self.GET_MACHINE_STATUS_QUERY_28_29 = """
            SELECT {cld_column}, timestamp ,state FROM {mc}
            WHERE timestamp >= NOW() - INTERVAL '5 minutes'
            ORDER BY timestamp DESC
            LIMIT 1 """

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
                FROM loop4_sku
                WHERE timestamp >= now() - interval '5 minutes'
                ORDER BY timestamp DESC
                LIMIT 1
        """

        self.STOPAGE_COUNT_QUERY = """
            SELECT COUNT(*)
            FROM machine_stoppages
            WHERE machine = %s
            AND stop_at BETWEEN %s AND %s
        """

        self.get_hopper_data_25_26 = """
            select hopper_1_level,hopper_2_level from {mc} where timestamp > now() - interval '5 minutes'
            limit 1;
        """

        self.get_hopper_data_27_30 = """
            select hopper_level_left,hopper_level_right from {mc} where timestamp > now() - interval '5 minutes'
            limit 1;
        """

        self.GET_MACHINE_DATA_QUERY_25_26 = """
            SELECT time_bucket('5 minutes', timestamp) AS time_bucket, AVG(speed) / 12 AS avg_speed,
                        AVG(ver_pressure) AS avg_ver_pressure, AVG(hor_pressure) AS avg_hor_pressure,
                        AVG(hopper_1_level) AS avg_hopper_1_level, AVG(hopper_2_level) AS avg_hopper_2_level,
                        AVG((verticalsealerrear_1 + verticalsealerrear_2 + verticalsealerrear_3 +
                             verticalsealerrear_4 + verticalsealerrear_5 + verticalsealerrear_6 +
                             verticalsealerrear_7 + verticalsealerrear_8 + verticalsealerrear_9 +
                             verticalsealerrear_10 + verticalsealerrear_11 + verticalsealerrear_12 +
                             verticalsealerrear_13) / 13)/10 AS avg_vertical_rear_temp,
                        AVG((verticalsealerfront_1 + verticalsealerfront_2 + verticalsealerfront_3 +
                             verticalsealerfront_4 + verticalsealerfront_5 + verticalsealerfront_6 +
                             verticalsealerfront_7 + verticalsealerfront_8 + verticalsealerfront_9 +
                             verticalsealerfront_10 + verticalsealerfront_11 + verticalsealerfront_12 +
                             verticalsealerfront_13) / 13)/10 AS avg_vertical_front_temp,
                        AVG(hor_sealer_rear)/10 AS avg_horizontal_rear_temp,
                        AVG(hor_sealer_front) /10 AS avg_horizontal_front_temp,
                        timestamp,EXTRACT(EPOCH FROM timestamp) AS epoch_timestamp
                        FROM {mc}
                        WHERE timestamp > now() - '5 minutes'::interval
                        GROUP BY time_bucket,timestamp
                        ORDER BY time_bucket DESC
                        LIMIT 1
        """

        self.GET_MACHINE_DATA_QUERY_27 = """
            SELECT time_bucket('5 minutes', timestamp) AS time_bucket, AVG(speed) / 12 AS avg_speed,
                        AVG(ver_pressure) AS avg_ver_pressure, AVG(horizontal_pressure) AS avg_hor_pressure,
                        AVG(hopper_level_left) AS avg_hopper_1_level, AVG(hopper_level_right) AS avg_hopper_2_level,
                        AVG((verticalsealerrear_1 + verticalsealerrear_2 + verticalsealerrear_3 +
                             verticalsealerrear_4 + verticalsealerrear_5 + verticalsealerrear_6 +
                             verticalsealerrear_7 + verticalsealerrear_8 + verticalsealerrear_9 +
                             verticalsealerrear_10 + verticalsealerrear_11 + verticalsealerrear_12 +
                             verticalsealerrear_13) / 13)/10 AS avg_vertical_rear_temp,
                        AVG((verticalsealerfront_1 + verticalsealerfront_2 + verticalsealerfront_3 +
                             verticalsealerfront_4 + verticalsealerfront_5 + verticalsealerfront_6 +
                             verticalsealerfront_7 + verticalsealerfront_8 + verticalsealerfront_9 +
                             verticalsealerfront_10 + verticalsealerfront_11 + verticalsealerfront_12 +
                             verticalsealerfront_13) / 13)/10 AS avg_vertical_front_temp,
                        AVG(hor_sealer_rear)/10 AS avg_horizontal_rear_temp,
                        AVG(hor_sealer_front)/10 AS avg_horizontal_front_temp,
                        timestamp,EXTRACT(EPOCH FROM timestamp) AS epoch_timestamp
                        FROM {mc}
                        WHERE timestamp > now() - '5 minutes'::interval
                        GROUP BY time_bucket,timestamp
                        ORDER BY time_bucket DESC
                        LIMIT 1
        """

        self.GET_MACHINE_DATA_QUERY_28_29 = """
            select time_bucket('5 minutes',timestamp) as time_bucket,AVG(speed) /12 AS avg_speed,
                        AVG((vertical_sealer_rear_1_temp + vertical_sealer_rear_2_temp + vertical_sealer_rear_3_temp +
                            vertical_sealer_rear_4_temp + vertical_sealer_rear_5_temp + vertical_sealer_rear_6_temp +
                            vertical_sealer_rear_7_temp + vertical_sealer_rear_8_temp + vertical_sealer_rear_9_temp +
                            vertical_sealer_rear_10_temp + vertical_sealer_rear_11_temp + vertical_sealer_rear_12_temp +
                            vertical_sealer_rear_13_temp) / 13)/10 AS avg_vertical_rear_temp,
                        AVG((vertical_sealer_front_1_temp + vertical_sealer_front_2_temp + vertical_sealer_front_3_temp +
                            vertical_sealer_front_4_temp + vertical_sealer_front_5_temp + vertical_sealer_front_6_temp +
                            vertical_sealer_front_7_temp + vertical_sealer_front_8_temp + vertical_sealer_front_9_temp +
                            vertical_sealer_front_10_temp + vertical_sealer_front_11_temp + vertical_sealer_front_12_temp +
                            vertical_sealer_front_13_temp) / 13)/10 AS avg_vertical_front_temp,
                        AVG(horizontal_sealer_rear_1_temp)/10 AS avg_horizontal_rear_temp,
                        AVG(horizontal_sealer_front_1_temp)/10 AS avg_horizontal_front_temp,
                        state,{cld_column},timestamp,EXTRACT(EPOCH FROM timestamp) AS epoch_timestamp
                        from {mc}
                        where timestamp > now() - '5 minutes'::interval
                        group by time_bucket,state,{cld_column},timestamp
                        order by time_bucket DESC
                        LIMIT 1
        """

        self.GET_MACHINE_DATA_QUERY_30 = """
            SELECT time_bucket('5 minutes', timestamp) AS time_bucket, AVG(speed) / 12 AS avg_speed,
                        AVG(vertical_pressure) AS avg_ver_pressure, AVG(horizontal_pressure) AS avg_hor_pressure,
                        AVG(hopper_level_left) AS avg_hopper_1_level, AVG(hopper_level_right) AS avg_hopper_2_level,
                        AVG((verticalsealerrear_1 + verticalsealerrear_2 + verticalsealerrear_3 +
                             verticalsealerrear_4 + verticalsealerrear_5 + verticalsealerrear_6 +
                             verticalsealerrear_7 + verticalsealerrear_8 + verticalsealerrear_9 +
                             verticalsealerrear_10 + verticalsealerrear_11 + verticalsealerrear_12 +
                             verticalsealerrear_13) / 13)/10 AS avg_vertical_rear_temp,
                        AVG((verticalsealerfront_1 + verticalsealerfront_2 + verticalsealerfront_3 +
                             verticalsealerfront_4 + verticalsealerfront_5 + verticalsealerfront_6 +
                             verticalsealerfront_7 + verticalsealerfront_8 + verticalsealerfront_9 +
                             verticalsealerfront_10 + verticalsealerfront_11 + verticalsealerfront_12 +
                             verticalsealerfront_13) / 13)/10 AS avg_vertical_front_temp,
                        AVG(hor_sealer_rear)/10 AS avg_horizontal_rear_temp,
                        AVG(hor_sealer_front)/10 AS avg_horizontal_front_temp,
                        timestamp,EXTRACT(EPOCH FROM timestamp) AS epoch_timestamp
                        FROM {mc}
                        WHERE timestamp > now() - '5 minutes'::interval
                        GROUP BY time_bucket,timestamp
                        ORDER BY time_bucket DESC
                        LIMIT 1"""

        self.GET_STOPPAGE_TIME = """
            SELECT
                {mc} ->> 'stoppage_time' AS stoppage_time,
                {mc} ->> 'stoppage_count' AS stoppage_count,
                {mc} ->> 'longest_duration' AS longest_duration,
                {mc} ->> 'highest_count' AS highest_count
            FROM machine_stoppage_status
            WHERE shift = %s
            ORDER BY timestamp DESC
            LIMIT 1;
        """
        self.GET_PREV_STOPPAGE_TIME = """
            SELECT
                mc{mc} ->> 'stoppage_time' AS stoppage_time,
                mc{mc} ->> 'stoppage_count' AS stoppage_count,
                mc{mc} ->> 'longest_duration' AS longest_duration,
                mc{mc} ->> 'highest_count' AS highest_count
            FROM machine_stoppage_status
            ORDER BY timestamp DESC
            LIMIT 2;
        """

    def get_shift_and_time_range(self, current_time):
        config = load_config()
        shift_config = config['shifts']

        current_hour = current_time.hour

        # Check for Shift A (7am-3pm)
        if current_hour >= shift_config["A"]["start_hour"] and current_hour < shift_config["A"]["end_hour"]:
            shift = "A"
            json_key = shift_config["A"]["name"]
            start_time = current_time.replace(
                hour=shift_config["A"]["start_hour"],
                minute=0, second=0, microsecond=0
            )
            end_time = current_time.replace(
                hour=shift_config["A"]["end_hour"],
                minute=0, second=0, microsecond=0
            )

        # Check for Shift B (3pm-11pm)
        elif current_hour >= shift_config["B"]["start_hour"] and current_hour < shift_config["B"]["end_hour"]:
            shift = "B"
            json_key = shift_config["B"]["name"]
            start_time = current_time.replace(
                hour=shift_config["B"]["start_hour"],
                minute=0, second=0, microsecond=0
            )
            end_time = current_time.replace(
                hour=shift_config["B"]["end_hour"],
                minute=0, second=0, microsecond=0
            )

        # Otherwise it's Shift C (11pm-7am)
        else:
            shift = "C"
            json_key = shift_config["C"]["name"]

            if current_hour < shift_config["C"]["end_hour"]:  # Before 7am
                start_time = (current_time - timedelta(days=1)).replace(
                    hour=shift_config["C"]["start_hour"],
                    minute=0, second=0, microsecond=0
                )
                end_time = current_time.replace(
                    hour=shift_config["C"]["end_hour"],
                    minute=0, second=0, microsecond=0
                )
            else:  # After 11pm
                start_time = current_time.replace(
                    hour=shift_config["C"]["start_hour"],
                    minute=0, second=0, microsecond=0
                )
                end_time = current_time.replace(
                    hour=shift_config["C"]["end_hour"],
                    minute=0, second=0, microsecond=0
                ) + timedelta(days=1)

        return shift, json_key, start_time, end_time

    def fetch_machine_uptime(self,shift_value,machine_no):
        self.db.cursor.execute(self.GET_MACHINE_UPTIME,(shift_value,machine_no))
        machine_uptime = self.db.cursor.fetchone()
        machine_uptime = machine_uptime[0] if machine_uptime else timedelta()

        return machine_uptime


    def get_machine_25(self,start_time,shift):
        print("\n---------------------------------------------------")
        print("\n\n machine number : mc25")
        print("\n---------------------------------------------------")

        self.write_db.cursor.execute(self.GET_STOPPAGE_TIME.format(mc='mc25'),(shift,))
        stoppage_time, stoppage_count,longest_duration , highest_count = self.write_db.cursor.fetchone()

        self.write_db.cursor.execute(self.GET_PREV_STOPPAGE_TIME.format(mc='25'))
        results = self.write_db.cursor.fetchall()

        if len(results) >= 2:
            result = results[1]
            prev_stoppage_time = result[0]
            prev_stoppage_data = result[1]
            prev_longest_duration = result[2]
            prev_highest_count = result[3]

        if longest_duration or prev_longest_duration:
            try:
                longest_duration = json.loads(longest_duration)
                prev_longest_duration = json.loads(prev_longest_duration)
            except json.JSONDecodeError:
                longest_duration = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                prev_longest_duration = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}

        if highest_count or prev_highest_count:
                try :
                    highest_count = json.loads(highest_count)
                    prev_highest_count = json.loads(prev_highest_count)
                except json.JSONDecodeError:
                    highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                    prev_highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}

        print("longest stoppage time ",longest_duration)
        print("prev longest stoppage time ",prev_longest_duration)

        if stoppage_time and stoppage_time[0] is not None or (prev_stoppage_time and prev_stoppage_time[0] is not None):
            stoppage_time_minute = float(stoppage_time)
            prev_stoppage_minutes = float(prev_stoppage_time)
        else:
            stoppage_time_minute = 0
            prev_stoppage_minutes = 0

        current_time = datetime.now(self.india_timezone)
        # Use passed-in shift parameter to get json_key for machine_uptime lookup
        config = load_config()
        shift_config = config['shifts']
        json_key = shift_config[shift]["name"]
        machine_uptime = self.fetch_machine_uptime(json_key,'mc25')

        # For *_mid tables: use shift column for CLD count, get status from base table
        cld_column_mid = f"shift_{shift.lower()}"

        # Get CLD production from *_mid table (within last 10 minutes)
        query_cld = f"SELECT {cld_column_mid}, timestamp FROM mc25_mid WHERE timestamp >= NOW() - INTERVAL '10 minutes' ORDER BY timestamp DESC LIMIT 1"
        self.db.cursor.execute(query_cld)
        result_cld = self.db.cursor.fetchone()

        if result_cld:
            cld_production = result_cld[0] if result_cld[0] is not None else 0
            timestamp_value = result_cld[1]
        else:
            cld_production = 0
            timestamp_value = None

        # Get status from base table
        query_status = "SELECT status FROM mc25 WHERE timestamp >= NOW() - INTERVAL '5 minutes' ORDER BY timestamp DESC LIMIT 1"
        self.db.cursor.execute(query_status)
        result_status = self.db.cursor.fetchone()
        status_value = result_status[0] if result_status else 2

        print(f"\nCld count : {cld_production}")

        time_difference_hours = (current_time - start_time).total_seconds() / 3600
        BBOEE = ((cld_production * 100) / (720 * time_difference_hours)) * 8 if time_difference_hours > 0 else 0
        BBOEE = max(0, min(BBOEE, 100))

        if timestamp_value:
            time_difference = current_time - timestamp_value
            print(f"Current Time : {current_time}")
            print(f"DB Timestamp : {timestamp_value}")
            print(f"Time Difference : {time_difference.total_seconds()} seconds")

            if time_difference.total_seconds() > 300:
                status_message = "Offline"
            else:
                status_message = "Running" if status_value == 1 else "Stopped"
        else:
            print("No recent data found in the last 5 minutes.")
            status_message = "Offline"  # Default to "Offline" if no data

        print("Machine Status =", status_message)


        try:
            self.db.cursor.execute(self.get_hopper_data_25_26.format(mc='mc25'))
            hopper_data = self.db.cursor.fetchone()

            self.db.cursor.execute(self.GET_MACHINE_DATA_QUERY_25_26.format(mc='mc25'))
            avg_data_result = self.db.cursor.fetchall()

            if avg_data_result:
                avg_data = avg_data_result[0]

                mc_data = {'speed': 120 ,'status':status_message,'jamming':True,'pulling':True,'sealing':True,'sealing_time_vertical':192,'sealing_time_horizontal':195,'stoppage_time':round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production':cld_production,'stoppage_count':int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality':True,'vertical_pressure':round(avg_data[2], 2) if avg_data[2] is not None else 0,'vertical_rear_temp':round(avg_data[6],2) if avg_data[6] is not None else 0,
                   'horizontal_pressure':round(avg_data[3], 2) if avg_data[3] is not None else 0,'vertical_front_temp':round(avg_data[7],2) if avg_data[7] is not None else 0,'breakdown_prediction': True,'hopper_level_primary': round(hopper_data[0], 2) if hopper_data[0] is not None else 0,'horizontal_rear_temp':round(avg_data[8],2) if avg_data[8] is not None else 0,'horizontal_front_temp':round(avg_data[9],2) if avg_data[9] is not None else 0,'hopper_level_secondary': round(hopper_data[1], 2) if hopper_data[1] is not None else 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2)
                   ,"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}

                self.last_valid_mc25_data = mc_data

            else:
                print("No data found for mc25")
                mc_data = self.last_valid_mc25_data if self.last_valid_mc25_data is not None else {
                'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical':0,'sealing_time_horizontal': 0,'stoppage_time':round(stoppage_time_minute) ,"longest_duration" : longest_duration,'cld_production':cld_production,'stoppage_count': int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality': True,'vertical_pressure': 0,'vertical_rear_temp': 0,
                    'horizontal_pressure': 0,'vertical_front_temp':0,'breakdown_prediction': True,'hopper_level_primary': 0,'horizontal_rear_temp':0,'horizontal_front_temp': 0,'hopper_level_secondary': 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}

        except Exception as e:
                print(f"Error occurred: {e}")
                mc_data = {'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 0,'sealing_time_horizontal': 0,'stoppage_time': round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production': cld_production,'stoppage_count': 0,'outgoing_quality': True,'vertical_pressure':0,'vertical_rear_temp': 0,
                    'horizontal_pressure': 0,'vertical_front_temp': 0,'breakdown_prediction': True,'hopper_level_primary': 0,'horizontal_rear_temp':0,'horizontal_front_temp':0,'hopper_level_secondary': 0,'productivity_prediction': True, 'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}
                os._exit(1)

        return mc_data

    def get_machine_26(self, start_time,shift):
        print("\n---------------------------------------------------")
        print("\n\n machine number : mc26")
        print("\n---------------------------------------------------")

        self.write_db.cursor.execute(self.GET_STOPPAGE_TIME.format(mc='mc26'),(shift,))
        stoppage_time, stoppage_count,longest_duration, highest_count= self.write_db.cursor.fetchone()

        self.write_db.cursor.execute(self.GET_PREV_STOPPAGE_TIME.format(mc='26'))
        results = self.write_db.cursor.fetchall()

        if len(results) >= 2:
            result = results[1]
            prev_stoppage_time = result[0]
            prev_stoppage_data = result[1]
            prev_longest_duration = result[2]
            prev_highest_count = result[3]

        if longest_duration or prev_longest_duration:
            try:
                longest_duration = json.loads(longest_duration)
                prev_longest_duration = json.loads(prev_longest_duration)
            except json.JSONDecodeError:
                longest_duration = {}
                prev_longest_duration = {}

        if highest_count or prev_highest_count:
                try :
                    highest_count = json.loads(highest_count)
                    prev_highest_count = json.loads(prev_highest_count)
                except json.JSONDecodeError:
                    highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                    prev_highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}

        print("longest stoppage time ",longest_duration)
        print("prev longest stoppage time ",prev_longest_duration)

        if stoppage_time and stoppage_time[0] is not None or (prev_stoppage_time and prev_stoppage_time[0] is not None):
            stoppage_time_minute = float(stoppage_time)
            prev_stoppage_minutes = float(prev_stoppage_time)
        else:
            stoppage_time_minute = 0
            prev_stoppage_minutes = 0

        current_time = datetime.now(self.india_timezone)
        # Use passed-in shift parameter to get json_key for machine_uptime lookup
        config = load_config()
        shift_config = config['shifts']
        json_key = shift_config[shift]["name"]

        machine_uptime = self.fetch_machine_uptime(json_key,'mc26')

        # For *_mid tables: use shift column for CLD count, get status from base table
        cld_column_mid = f"shift_{shift.lower()}"

        # Get CLD production from *_mid table (within last 10 minutes)
        query_cld = f"SELECT {cld_column_mid}, timestamp FROM mc26_mid WHERE timestamp >= NOW() - INTERVAL '10 minutes' ORDER BY timestamp DESC LIMIT 1"
        self.db.cursor.execute(query_cld)
        result_cld = self.db.cursor.fetchone()

        if result_cld:
            cld_production = result_cld[0] if result_cld[0] is not None else 0
            timestamp_value = result_cld[1]
        else:
            cld_production = 0
            timestamp_value = None

        # Get status from base table
        query_status = "SELECT status FROM mc26 WHERE timestamp >= NOW() - INTERVAL '5 minutes' ORDER BY timestamp DESC LIMIT 1"
        self.db.cursor.execute(query_status)
        result_status = self.db.cursor.fetchone()
        status_value = result_status[0] if result_status else 2

        print(f"\nCld count : {cld_production}")

        time_difference_hours = (current_time - start_time).total_seconds() / 3600
        BBOEE = ((cld_production * 100) / (720 * time_difference_hours)) * 8 if time_difference_hours > 0 else 0
        BBOEE = max(0, min(BBOEE, 100))

        if timestamp_value:
            time_difference = current_time - timestamp_value
            print(f"Current Time : {current_time}")
            print(f"DB Timestamp : {timestamp_value}")
            print(f"Time Difference : {time_difference.total_seconds()} seconds")

            if time_difference.total_seconds() > 300:
                status_message = "Offline"
            else:
                status_message = "Running" if status_value == 1 else "Stopped"
        else:
            print("No recent data found in the last 5 minutes.")
            status_message = "Offline"  # Default to "Offline" if no data

        print("Machine Status =", status_message)


        try:
            self.db.cursor.execute(self.get_hopper_data_25_26.format(mc='mc26'))
            hopper_data = self.db.cursor.fetchone()

            self.db.cursor.execute(self.GET_MACHINE_DATA_QUERY_25_26.format(mc='mc26'))
            avg_data_result = self.db.cursor.fetchall()

            if avg_data_result:
                avg_data = avg_data_result[0]

                mc_data = {'speed': 120,'status':status_message,'jamming':True,'pulling':True,'sealing':True,'sealing_time_vertical':192,'sealing_time_horizontal':195,'stoppage_time':round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production':cld_production,'stoppage_count':int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality':True,'vertical_pressure':round(avg_data[2], 2) if avg_data[2] is not None else 0,'vertical_rear_temp':round(avg_data[6],2) if avg_data[6] is not None else 0,
                   'horizontal_pressure': round(avg_data[3], 2) if avg_data[3] is not None else 0,'vertical_front_temp':round(avg_data[7],2) if avg_data[7] is not None else 0,'breakdown_prediction': True,'hopper_level_primary': round(hopper_data[0], 2) if hopper_data[0] is not None else 0,'horizontal_rear_temp':round(avg_data[8],2) if avg_data[8] is not None else 0,'horizontal_front_temp':round(avg_data[9],2) if avg_data[9] is not None else 0,'hopper_level_secondary': round(hopper_data[1], 2) if hopper_data[1] is not None else 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2)
                   ,"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}
                self.last_valid_mc26_data = mc_data
            else:
                print("No data found for mc26")
                mc_data = self.last_valid_mc26_data if self.last_valid_mc26_data is not None else {
                'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 0,'sealing_time_horizontal': 0,'stoppage_time': round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production': cld_production,'stoppage_count': int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality': True,'vertical_pressure': 0,'vertical_rear_temp': 0,
                    'horizontal_pressure': 0,'vertical_front_temp': 0,'breakdown_prediction': True,'hopper_level_primary': 0,'horizontal_rear_temp': 0,'horizontal_front_temp': 0,'hopper_level_secondary': 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}

        except Exception as e:
            print(f"Error occurred: {e}")
            mc_data = {'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 0,'sealing_time_horizontal': 0,'stoppage_time': round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production': cld_production,'stoppage_count': int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality': True,'vertical_pressure': 0,'vertical_rear_temp': 0,
                    'horizontal_pressure': 0,'vertical_front_temp': 0,'breakdown_prediction': True,'hopper_level_primary': 0,'horizontal_rear_temp': 0,'horizontal_front_temp': 0,'hopper_level_secondary': 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}
            os._exit(1)
        return mc_data

    def get_machine_27(self, start_time,shift):
        print("\n---------------------------------------------------")
        print("\n\n machine number : mc27")
        print("\n---------------------------------------------------")

        self.write_db.cursor.execute(self.GET_STOPPAGE_TIME.format(mc='mc27'),(shift,))
        stoppage_time, stoppage_count,longest_duration, highest_count = self.write_db.cursor.fetchone()

        self.write_db.cursor.execute(self.GET_PREV_STOPPAGE_TIME.format(mc='27'))
        results = self.write_db.cursor.fetchall()

        if len(results) >= 2:
            result = results[1]
            prev_stoppage_time = result[0]
            prev_stoppage_data = result[1]
            prev_longest_duration = result[2]
            prev_highest_count = result[3]

        if longest_duration or prev_longest_duration:
            try:
                longest_duration = json.loads(longest_duration)
                prev_longest_duration = json.loads(prev_longest_duration)
            except json.JSONDecodeError:
                longest_duration = {}
                prev_longest_duration = {}

        if highest_count or prev_highest_count:
                try :
                    highest_count = json.loads(highest_count)
                    prev_highest_count = json.loads(prev_highest_count)
                except json.JSONDecodeError:
                    highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                    prev_highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}

        print("longest stoppage time ",longest_duration)
        print("prev longest stoppage time ",prev_longest_duration)

        if stoppage_time and stoppage_time[0] is not None or (prev_stoppage_time and prev_stoppage_time[0] is not None):
            stoppage_time_minute = float(stoppage_time)
            prev_stoppage_minutes = float(prev_stoppage_time)
        else:
            stoppage_time_minute = 0
            prev_stoppage_minutes = 0

        current_time = datetime.now(self.india_timezone)
        # Use passed-in shift parameter to get json_key for machine_uptime lookup
        config = load_config()
        shift_config = config['shifts']
        json_key = shift_config[shift]["name"]
        machine_uptime = self.fetch_machine_uptime(json_key,'mc27')

        # For *_mid tables: use shift column for CLD count, get status from base table
        cld_column_mid = f"shift_{shift.lower()}"

        # Get CLD production from *_mid table (within last 10 minutes)
        query_cld = f"SELECT {cld_column_mid}, timestamp FROM mc27_mid WHERE timestamp >= NOW() - INTERVAL '10 minutes' ORDER BY timestamp DESC LIMIT 1"
        self.db.cursor.execute(query_cld)
        result_cld = self.db.cursor.fetchone()

        if result_cld:
            cld_production = result_cld[0] if result_cld[0] is not None else 0
            timestamp_value = result_cld[1]
        else:
            cld_production = 0
            timestamp_value = None

        # Get status from base table
        query_status = "SELECT status FROM mc27 WHERE timestamp >= NOW() - INTERVAL '5 minutes' ORDER BY timestamp DESC LIMIT 1"
        self.db.cursor.execute(query_status)
        result_status = self.db.cursor.fetchone()
        status_value = result_status[0] if result_status else 2

        print(f"\nCld count : {cld_production}")

        time_difference_hours = (current_time - start_time).total_seconds() / 3600
        BBOEE = ((cld_production * 100) / (600 * time_difference_hours)) * 8 if time_difference_hours > 0 else 0
        BBOEE = max(0, min(BBOEE, 100))

        if timestamp_value:
            time_difference = current_time - timestamp_value
            print(f"Current Time : {current_time}")
            print(f"DB Timestamp : {timestamp_value}")
            print(f"Time Difference : {time_difference.total_seconds()} seconds")

            if time_difference.total_seconds() > 300:
                status_message = "Offline"
            else:
                status_message = "Running" if status_value == 1 else "Stopped"
        else:
            print("No recent data found in the last 5 minutes.")
            status_message = "Offline"

        print("Machine Status =", status_message)


        try:
            self.db.cursor.execute(self.get_hopper_data_27_30.format(mc='mc27'))
            hopper_data = self.db.cursor.fetchone()

            self.db.cursor.execute(self.GET_MACHINE_DATA_QUERY_27.format(mc='mc27'))
            avg_data_result = self.db.cursor.fetchall()

            if avg_data_result:
                avg_data = avg_data_result[0]

                mc_data = {'speed': 100, 'status': status_message, 'jamming': True, 'pulling': True, 'sealing': True, 'sealing_time_vertical': 222, 'sealing_time_horizontal': 240, 'stoppage_time': round(stoppage_time_minute), "longest_duration" : longest_duration,'cld_production': cld_production, 'stoppage_count': int(stoppage_count) if stoppage_count is not None else 0, 'outgoing_quality': True, 'vertical_pressure': round(avg_data[2], 2) if avg_data[2] is not None else 0,
                'vertical_rear_temp': round(avg_data[6], 2) if avg_data[6] is not None else 0, 'horizontal_pressure': round(avg_data[3], 2) if avg_data[3] is not None else 0, 'vertical_front_temp': round(avg_data[7], 2) if avg_data[7] is not None else 0, 'breakdown_prediction': True, 'hopper_level_primary': round(hopper_data[0], 2) if hopper_data[0] is not None else 0, 'horizontal_rear_temp': round(avg_data[8], 2) if avg_data[8] is not None else 0, 'horizontal_front_temp': round(avg_data[9], 2) if avg_data[9] is not None else 0,
                'hopper_level_secondary': round(hopper_data[1], 2) if hopper_data[1] is not None else 0, 'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}

                self.last_valid_mc27_data = mc_data
            else:
                print("No data found for mc27 query")
                mc_data = self.last_valid_mc27_data if self.last_valid_mc27_data is not None else {
                'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 0,'sealing_time_horizontal': 0,'stoppage_time':round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production':cld_production,'stoppage_count':int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality': True,'vertical_pressure': 0,'vertical_rear_temp': 0,
                    'horizontal_pressure': 0,'vertical_front_temp':0,'breakdown_prediction': True,'hopper_level_primary': 0,'horizontal_rear_temp': 0,'horizontal_front_temp': 0,'hopper_level_secondary': 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}
        except Exception as e:
            print(f"Error occurred: {e}")
            mc_data = {'speed':0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 0,'sealing_time_horizontal': 0,'stoppage_time': round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production':cld_production,'stoppage_count': int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality': True,'vertical_pressure': 0,'vertical_rear_temp': 0,
                    'horizontal_pressure': 0,'vertical_front_temp': 0,'breakdown_prediction': True,'hopper_level_primary': 0,'horizontal_rear_temp': 0,'horizontal_front_temp': 0,'hopper_level_secondary': 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}
            os._exit(1)
        return mc_data

    def get_machine_28(self, start_time, shift):
        print("\n---------------------------------------------------")
        print("\n\n machine number : mc28")
        print("\n---------------------------------------------------")

        self.write_db.cursor.execute(self.GET_STOPPAGE_TIME.format(mc='mc28'),(shift,))
        stoppage_time, stoppage_count,longest_duration, highest_count= self.write_db.cursor.fetchone()

        self.write_db.cursor.execute(self.GET_PREV_STOPPAGE_TIME.format(mc='28'))
        results = self.write_db.cursor.fetchall()

        if len(results) >= 2:
            result = results[1]
            prev_stoppage_time = result[0]
            prev_stoppage_data = result[1]
            prev_longest_duration = result[2]
            prev_highest_count = result[3]

        if longest_duration or prev_longest_duration:
            try:
                longest_duration = json.loads(longest_duration)
                prev_longest_duration = json.loads(prev_longest_duration)
            except json.JSONDecodeError:
                longest_duration = {}
                prev_longest_duration = {}

        if highest_count or prev_highest_count :
                try :
                    highest_count = json.loads(highest_count)
                    prev_highest_count = json.loads(prev_highest_count)
                except json.JSONDecodeError:
                    highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                    prev_highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}

        print("longest stoppage time ",longest_duration)
        print("prev longest stoppage time ",prev_longest_duration)

        if stoppage_time and stoppage_time[0] is not None or (prev_stoppage_time and prev_stoppage_time[0] is not None):
            stoppage_time_minute = float(stoppage_time)
            prev_stoppage_minutes = float(prev_stoppage_time)
        else:
            stoppage_time_minute = 0
            prev_stoppage_minutes = 0

        current_time = datetime.now(self.india_timezone)
        # Use passed-in shift parameter to get json_key for machine_uptime lookup
        config = load_config()
        shift_config = config['shifts']
        json_key = shift_config[shift]["name"]
        machine_uptime = self.fetch_machine_uptime(json_key,'mc28')
        cld_column = f"cld_{shift.lower()}"

        timestamp_value = None
        status_value = 2

        query = self.GET_MACHINE_STATUS_QUERY_28_29.format(cld_column=cld_column,mc='mc28')
        self.db.cursor.execute(query)
        result = self.db.cursor.fetchone()

        if result:
            cld_value, timestamp_value,status_value = result
            cld_production = cld_value if cld_value is not None else 0
        else:
            cld_production = 0
            status_value = 2
            timestamp_value = None

        print(f"Cld count : {cld_production}")

        time_difference_hours = (current_time - start_time).total_seconds() / 3600
        BBOEE = ((cld_production * 100) / (600 * time_difference_hours)) * 8 if time_difference_hours > 0 else 0
        BBOEE = max(0, min(BBOEE, 100))

        if timestamp_value:
            time_difference = current_time - timestamp_value
            print(f"Current Time : {current_time}")
            print(f"DB Timestamp : {timestamp_value}")
            print(f"Time Difference : {time_difference.total_seconds()} seconds")

            if time_difference.total_seconds() > 300:
                status_message = "Offline"
            else:
                status_message = "Running" if status_value == 1 else "Stopped"
        else:
            print("No recent data found in the last 5 minutes.")
            status_message = "Offline"  # Default to "Offline" if no data

        #status_message = "Offline"

        print("Machine Status =", status_message)

        try:
            self.db.cursor.execute(self.GET_MACHINE_DATA_QUERY_28_29.format(cld_column=cld_column,mc='mc28'))
            avg_data_result = self.db.cursor.fetchall()

            if avg_data_result:
                avg_data = avg_data_result[0]

                mc_data = {'speed':avg_data[1] if avg_data[1] is not None else 100,'status':status_message,'jamming':True,'pulling':True,'sealing':True,'sealing_time_vertical':231,'sealing_time_horizontal':231,'stoppage_time':round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production':cld_production,'stoppage_count': int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality':True,'vertical_rear_temp':round(avg_data[2],2) if avg_data[2] is not None else 0,
                   'vertical_front_temp':round(avg_data[3],2) if avg_data[3] is not None else 0,'breakdown_prediction': True,'horizontal_rear_temp': round(avg_data[4], 2) if avg_data[4] is not None else 0
                    ,'horizontal_front_temp':round(avg_data[5],2) if avg_data[5] is not None else 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2)
                   ,"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}
                self.last_valid_mc28_data = mc_data
            else:
                print("No data found for mc28")
                mc_data = self.last_valid_mc28_data if self.last_valid_mc28_data is not None else {
                'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 0,'sealing_time_horizontal': 0,'stoppage_time': round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production':cld_production,'stoppage_count': int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality': True,'vertical_rear_temp': 0,
                    'vertical_front_temp': 0,'breakdown_prediction': True,'horizontal_rear_temp': 0,'horizontal_front_temp': 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}

        except Exception as e:
            print(f"Error occurred: {e}")
            mc_data = {'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 0,'sealing_time_horizontal': 0,'stoppage_time': round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production':cld_production,'stoppage_count': int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality': True,'vertical_rear_temp': 0,
                    'vertical_front_temp': 0,'breakdown_prediction': True,'horizontal_rear_temp': 0,'horizontal_front_temp': 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}
            os._exit(1)
        return mc_data

    def get_machine_29(self, start_time,shift):
        print("\n---------------------------------------------------")
        print("\n\n machine number : mc29")
        print("\n---------------------------------------------------")

        self.write_db.cursor.execute(self.GET_STOPPAGE_TIME.format(mc='mc29'),(shift,))
        stoppage_time, stoppage_count,longest_duration, highest_count= self.write_db.cursor.fetchone()

        self.write_db.cursor.execute(self.GET_PREV_STOPPAGE_TIME.format(mc='29'))
        results = self.write_db.cursor.fetchall()

        if len(results) >= 2:
            result = results[1]
            prev_stoppage_time = result[0]
            prev_stoppage_data = result[1]
            prev_longest_duration = result[2]
            prev_highest_count = result[3]

        if longest_duration or prev_longest_duration:
            try:
                longest_duration = json.loads(longest_duration)
                prev_longest_duration = json.loads(prev_longest_duration)
            except json.JSONDecodeError:
                longest_duration = {}
                prev_longest_duration = {}

        if highest_count or prev_highest_count :
                try :
                    highest_count = json.loads(highest_count)
                    prev_highest_count = json.loads(prev_highest_count)
                except json.JSONDecodeError:
                    highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                    prev_highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}

        print("longest stoppage time ",longest_duration)
        print("prev longest stoppage time ",prev_longest_duration)

        if stoppage_time and stoppage_time[0] is not None or (prev_stoppage_time and prev_stoppage_time[0] is not None):
            stoppage_time_minute = float(stoppage_time)
            prev_stoppage_minutes = float(prev_stoppage_time)
        else:
            stoppage_time_minute = 0
            prev_stoppage_minutes = 0

        current_time = datetime.now(self.india_timezone)
        # Use passed-in shift parameter to get json_key for machine_uptime lookup
        config = load_config()
        shift_config = config['shifts']
        json_key = shift_config[shift]["name"]
        machine_uptime = self.fetch_machine_uptime(json_key,'mc29')
        cld_column = f"cld_{shift.lower()}"

        timestamp_value = None
        status_value = 2

        query = self.GET_MACHINE_STATUS_QUERY_28_29.format(cld_column=cld_column,mc='mc29')
        self.db.cursor.execute(query)
        result = self.db.cursor.fetchone()

        if result:
            cld_value, timestamp_value,status_value = result
            cld_production = cld_value if cld_value is not None else 0
        else:
            cld_production = 0
            status_value = 2
            timestamp_value = None

        print(f"Cld count : {cld_production}")

        time_difference_hours = (current_time - start_time).total_seconds() / 3600
        BBOEE = ((cld_production * 100) / (600 * time_difference_hours)) * 8 if time_difference_hours > 0 else 0
        BBOEE = max(0, min(BBOEE, 100))

        if timestamp_value:
            time_difference = current_time - timestamp_value
            print(f"Current Time : {current_time}")
            print(f"DB Timestamp : {timestamp_value}")
            print(f"Time Difference : {time_difference.total_seconds()} seconds")

            if time_difference.total_seconds() > 300:
                status_message = "Offline"
            else:
                status_message = "Running" if status_value == 1 else "Stopped"
        else:
            print("No recent data found in the last 5 minutes.")
            status_message = "Offline"

        #status_message = "Offline"

        print("Machine Status =", status_message)


        try:
            self.db.cursor.execute(self.GET_MACHINE_DATA_QUERY_28_29.format(cld_column=cld_column,mc='mc29'))
            avg_data_result = self.db.cursor.fetchall()

            if avg_data_result:
                avg_data = avg_data_result[0]

                mc_data = {'speed':avg_data[1] if avg_data[1] is not None else 100,'status':status_message,'jamming':True,'pulling':True,'sealing':True,'sealing_time_vertical':230,'sealing_time_horizontal':240,'stoppage_time':round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production':cld_production,'stoppage_count':int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality':True,'vertical_rear_temp':round(avg_data[2],2) if avg_data[2] is not None else 0,
                   'vertical_front_temp':round(avg_data[3],2) if avg_data[3] is not None else 0,'breakdown_prediction': True,'horizontal_rear_temp': round(avg_data[4], 2) if avg_data[4] is not None else 0,'horizontal_front_temp':round(avg_data[5],2) if avg_data[5] is not None else 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2)
                   ,"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}
                self.last_valid_mc29_data = mc_data
            else:
                print("No data found for mc29")
                mc_data = self.last_valid_mc29_data if self.last_valid_mc29_data is not None else {
                'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical':0,'sealing_time_horizontal':0,'stoppage_time': round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production': cld_production,'stoppage_count':int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality': True,'vertical_rear_temp': 0,
                    'vertical_front_temp': 0,'breakdown_prediction': True,'horizontal_rear_temp': 0,'horizontal_front_temp': 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}

        except Exception as e:
            print(f"Error occurred: {e}")
            mc_data = {'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 0,'sealing_time_horizontal': 0,'stoppage_time': round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production': cld_production,'stoppage_count': int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality': True,'vertical_rear_temp': 0,
                    'vertical_front_temp': 0,'breakdown_prediction': True,'horizontal_rear_temp': 0,'horizontal_front_temp': 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}
            os._exit(1)
        return mc_data

    def get_machine_30(self, start_time,shift):
        print("\n---------------------------------------------------")
        print("\n\n machine number : mc30")
        print("\n---------------------------------------------------")

        self.write_db.cursor.execute(self.GET_STOPPAGE_TIME.format(mc='mc30'),(shift,))
        stoppage_time, stoppage_count,longest_duration, highest_count= self.write_db.cursor.fetchone()

        self.write_db.cursor.execute(self.GET_PREV_STOPPAGE_TIME.format(mc='30'))
        results = self.write_db.cursor.fetchall()

        if len(results) >= 2:
            result = results[1]
            prev_stoppage_time = result[0]
            prev_stoppage_data = result[1]
            prev_longest_duration = result[2]
            prev_highest_count = result[3]

        if longest_duration or prev_longest_duration:
            try:
                longest_duration = json.loads(longest_duration)
                prev_longest_duration = json.loads(prev_longest_duration)
            except json.JSONDecodeError:
                longest_duration = {}
                prev_longest_duration = {}

        if highest_count or prev_highest_count :
                try :
                    highest_count = json.loads(highest_count)
                    prev_highest_count = json.loads(prev_highest_count)
                except json.JSONDecodeError:
                    highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                    prev_highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}

        print("longest stoppage time ",longest_duration)
        print("prev longest stoppage time ",prev_longest_duration)

        if stoppage_time and stoppage_time[0] is not None or (prev_stoppage_time and prev_stoppage_time[0] is not None):
            stoppage_time_minute = float(stoppage_time)
            prev_stoppage_minutes = float(prev_stoppage_time)
        else:
            stoppage_time_minute = 0
            prev_stoppage_minutes = 0

        current_time = datetime.now(self.india_timezone)
        # Use passed-in shift parameter to get json_key for machine_uptime lookup
        config = load_config()
        shift_config = config['shifts']
        json_key = shift_config[shift]["name"]
        machine_uptime = self.fetch_machine_uptime(json_key,'mc30')

        # For *_mid tables: use shift column for CLD count, get status from base table
        cld_column_mid = f"shift_{shift.lower()}"

        # Get CLD production from *_mid table (within last 10 minutes)
        query_cld = f"SELECT {cld_column_mid}, timestamp FROM mc30_mid WHERE timestamp >= NOW() - INTERVAL '10 minutes' ORDER BY timestamp DESC LIMIT 1"
        self.db.cursor.execute(query_cld)
        result_cld = self.db.cursor.fetchone()

        if result_cld:
            cld_production = result_cld[0] if result_cld[0] is not None else 0
            timestamp_value = result_cld[1]
        else:
            cld_production = 0
            timestamp_value = None

        # Get status from base table
        query_status = "SELECT status FROM mc30 WHERE timestamp >= NOW() - INTERVAL '5 minutes' ORDER BY timestamp DESC LIMIT 1"
        self.db.cursor.execute(query_status)
        result_status = self.db.cursor.fetchone()
        status_value = result_status[0] if result_status else 2

        print(f"Cld count : {cld_production}")

        time_difference_hours = (current_time - start_time).total_seconds() / 3600
        BBOEE = ((cld_production * 100) / (630 * time_difference_hours)) * 8 if time_difference_hours > 0 else 0
        BBOEE = max(0, min(BBOEE, 100))

        if timestamp_value:
            time_difference = current_time - timestamp_value
            print(f"Current Time : {current_time}")
            print(f"DB Timestamp : {timestamp_value}")
            print(f"Time Difference : {time_difference.total_seconds()} seconds")

            if time_difference.total_seconds() > 300:
                status_message = "Offline"
            else:
                status_message = "Running" if status_value == 1 else "Stopped"
        else:
            print("No recent data found in the last 5 minutes.")
            status_message = "Offline"  # Default to "Offline" if no data

        print("Machine Status =", status_message)

        try:
            self.db.cursor.execute(self.get_hopper_data_27_30.format(mc='mc30'))
            hopper_data = self.db.cursor.fetchone()

            self.db.cursor.execute(self.GET_MACHINE_DATA_QUERY_30.format(mc='mc30'))
            avg_data_result = self.db.cursor.fetchall()

            if avg_data_result:
                avg_data = avg_data_result[0]

                mc_data = {'speed': 105, 'status': status_message, 'jamming': True, 'pulling': True, 'sealing': True, 'sealing_time_vertical': 222, 'sealing_time_horizontal': 240, 'stoppage_time': round(stoppage_time_minute), "longest_duration" : longest_duration,'cld_production': cld_production, 'stoppage_count': int(stoppage_count) if stoppage_count is not None else 0, 'outgoing_quality': True, 'vertical_pressure': round(avg_data[2], 2) if avg_data[2] is not None else 0,
                'vertical_rear_temp': round(avg_data[6], 2) if avg_data[6] is not None else 0, 'horizontal_pressure': round(avg_data[3], 2) if avg_data[3] is not None else 0, 'vertical_front_temp': round(avg_data[7], 2) if avg_data[7] is not None else 0, 'breakdown_prediction': True, 'hopper_level_primary': round(hopper_data[0], 2) if hopper_data[0] is not None else 0, 'horizontal_rear_temp': round(avg_data[8], 2) if avg_data[8] is not None else 0,
                'horizontal_front_temp': round(avg_data[9], 2) if avg_data[9] is not None else 0, 'hopper_level_secondary': round(hopper_data[1], 2) if hopper_data[1] is not None else 0, 'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}

                self.last_valid_mc30_data = mc_data
            else:
                print("No data found for mc30")
                mc_data = self.last_valid_mc30_data if self.last_valid_mc30_data is not None else {
                'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 0,'sealing_time_horizontal': 0,'stoppage_time': round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production': cld_production,'stoppage_count': int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality': True,'vertical_pressure': 0,'vertical_rear_temp': 0,
                    'horizontal_pressure': 0,'vertical_front_temp': 0,'breakdown_prediction': True,'hopper_level_primary': 0,'horizontal_rear_temp': 0,'horizontal_front_temp': 0,'hopper_level_secondary': 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}

        except Exception as e:
            print(f"Error occurred: {e}")
            mc_data = {'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 0,'sealing_time_horizontal': 0,'stoppage_time': round(stoppage_time_minute),"longest_duration" : longest_duration,'cld_production': cld_production,'stoppage_count': int(stoppage_count) if stoppage_count is not None else 0,'outgoing_quality': True,'vertical_pressure': 0,'vertical_rear_temp': 0,
                    'horizontal_pressure': 0,'vertical_front_temp': 0,'breakdown_prediction': True,'hopper_level_primary': 0,'horizontal_rear_temp': 0,'horizontal_front_temp': 0,'hopper_level_secondary': 0,'productivity_prediction': True,'machine_uptime' : str(machine_uptime),'bboee':round(BBOEE,2),"prev_stoppage_time" : round(prev_stoppage_minutes),"prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,"prev_longest_duration" : prev_longest_duration,"highest_count" : highest_count,"prev_highest_count" : prev_highest_count}
            os._exit(1)
        return mc_data


    def fetch_latest_values(self, start_time, end_time,shift):
        try:
            # Get fresh today_date to avoid stale data after midnight
            today_date = datetime.now(self.india_timezone).date()

            self.db.cursor.execute(self.QUERY_PRIMARY_SECONDARY_TANK, (today_date,))
            result = self.db.cursor.fetchone()

            if result is None:
                primary_tank, secondary_tank = 0, 0
            else:
                primary_tank, secondary_tank = result[0],result[1]

            print("primary tank ", primary_tank)
            print("secondary tank ", secondary_tank)


            self.db.cursor.execute(self.QUERY_TAPING_MACHINE, (today_date,))
            taping_machine = self.db.cursor.fetchone()
            taping_machine = int(taping_machine[0]) if taping_machine and taping_machine[0] is not None else 2
            print("taping machine = ", taping_machine)


            self.db.cursor.execute(self.QUERY_CASE_ERECTOR, (today_date,))
            case_erector = self.db.cursor.fetchone()
            case_erector = int(case_erector[0]) if case_erector and case_erector[0] is not None else 2
            print("case erector = ", case_erector)

            self.db.cursor.execute(self.GET_checkweigher_STATUS_QUERY)
            row = self.db.cursor.fetchone()

            if row and row[0] is not None:
                checkweigher = row[0]
            else:
                checkweigher = 'Offline'

            print(f"Checkweigher Status: {checkweigher}")

            machines_data = {
                "mc25": self.get_machine_25(start_time,shift),
                "mc26": self.get_machine_26(start_time,shift),
                "mc27": self.get_machine_27(start_time,shift),
                "mc28": self.get_machine_28(start_time,shift),
                "mc29": self.get_machine_29(start_time,shift),
                "mc30": self.get_machine_30(start_time,shift)
            }

            return primary_tank, secondary_tank, taping_machine, case_erector, machines_data,checkweigher

        except Exception as e:
            logging.error(f"Error fetching latest values: {e}")
            self.db.rollback()
            #return 0, 0, 2, 2, {}
            os._exit(1)

    def fetch_batch_sku_and_shift(self):
        try :
            self.db.cursor.execute(self.BATCH_SKU_SHIFT_QUERY)
            result = self.db.cursor.fetchone()

            if result:
                batch_sku, shift = result
                print(f" batch_sku: {batch_sku}, shift: {shift}")
                return batch_sku, shift
            else:
                logging.info("No batch_sku and shift found, defaulting to '--'")
                return " ", " "

        except Exception as e:
            logging.error(f"Error fetching batch_sku and shift: {e}")
            self.db.rollback()
            #return " ", " "
            os._exit(1)

    def calculate_ega(self):
        try:

            self.db.cursor.execute(self.EGA_QUERY)
            result = self.db.cursor.fetchone()

            if result and result[0] is not None:
                logging.info(f"Calculated EGA: {result[0]}")
                return result[0]
            else:
                logging.info("EGA calculation returned None, defaulting to 0.0")
                return 1.1
        except Exception as e:
            logging.error(f"Error calculating EGA: {e}")
            self.db.rollback()
            #return 0.0
            os._exit(1)


    def calculate_bulk_settling_time(self):
        try:
            today_date = datetime.now(self.india_timezone).date()

            self.db.cursor.execute(self.BULK_SETTLING_TIME_QUERY, (today_date,))
            result = self.db.cursor.fetchone()

            #bulk_settling_time = 0.0 if result is None or result[0] is None  else result[0]
            bulk_settling_time = 0 if result is None or result[0] is None else int(result[0])
            bulk_settling_time = max(0, min(bulk_settling_time, 99))
            print("bulk settling time ",bulk_settling_time)
            return bulk_settling_time

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logging.error(f"Error in calculate_bulk_settling_time: {exc_type} in {fname} at line {exc_tb.tb_lineno}: {e}")
            #return 0.0
            os._exit(1)


    def calculate_cld_production(self,shift):
        try:
            today_date = datetime.now(self.india_timezone).date()

            # For mc25, mc26, mc27, mc30 - use _mid tables with shift_a/b/c columns
            cld_column_mid = f"shift_{shift.lower()}"

            # For mc28, mc29 - keep existing cld_a/b/c columns
            cld_column_mc28_29 = f"cld_{shift.lower()}"

            cld_production = 0

            machines = {
                "mc25_mid": cld_column_mid,
                "mc26_mid": cld_column_mid,
                "mc27_mid": cld_column_mid,
                "mc30_mid": cld_column_mid,
                "mc28": cld_column_mc28_29,
                "mc29": cld_column_mc28_29
            }

            for mc_table, column in machines.items():
                query = self.CLD_PRODUCTION_QUERY_TEMPLATE.format(column=column,mc_table=mc_table)
                self.db.cursor.execute(query)
                result = self.db.cursor.fetchone()

                if result:
                    latest_timestamp, latest_count = result

                    if latest_timestamp.date() == today_date:
                        cld_production += latest_count if latest_count is not None else 0
                    else:
                        cld_production += 0
                    print(f"\nMachine: {mc_table}, Latest Count: {latest_count}")
                else:
                    cld_production += 0

            print("\nTotal CLD Production:", cld_production)

            return cld_production
        except Exception as e :
            logging.error(f"Error calculating CLD production: {e}")
            self.db.rollback()
            os._exit(1)


    def update_loop4(self):
        try:
            self.db.ensure_db_connection()
            current_time = datetime.now(self.india_timezone)
            shift, json_key,start_time, end_time = self.get_shift_and_time_range(current_time)

            # Keep start_time and end_time in IST timezone - don't convert to UTC
            # The machine methods need IST times for correct BBOEE calculation with IST current_time

            bulk_settling_time = self.calculate_bulk_settling_time()

            cld_production = self.calculate_cld_production(shift)

            ega = self.calculate_ega()

            primary_tank, secondary_tank, taping_machine, case_erector, machines_data , checkweigher = self.fetch_latest_values(start_time, end_time,shift)

            batch_sku, _ = self.fetch_batch_sku_and_shift()  # Ignore database shift, use calculated shift

            self.write_db.cursor.execute(self.UPDATE_LOOP4_QUERY, (
                current_time,
                round(primary_tank, 2),
                round(secondary_tank, 2),
                round(bulk_settling_time,2),
                round(cld_production, 2),
                round(ega ,2),
                taping_machine,
                case_erector,
                Json(machines_data),
                batch_sku,
                'Offline',
                checkweigher,
                shift  # shift parameter moved to end for WHERE clause
            ))

            if self.write_db.cursor.rowcount == 0:
                logging.info("\n\nNo existing row updated, inserting new row.")
                self.insert_into_loop4(current_time, primary_tank, secondary_tank, bulk_settling_time, cld_production, ega, taping_machine, case_erector, machines_data,batch_sku,shift,checkweigher)

            self.write_db.commit()
            logging.info(f"\n\nSuccessfully updated loop4_overview at {current_time}")

        except Exception as e:
            error_message = str(e)

            if "violates check constraint" in error_message or "chunk" in error_message:
                logging.warning(f"\n\nHypertable chunk constraint violated, inserting instead. Error: {error_message}")


                self.db.rollback()
                try:
                    self.insert_into_loop4(current_time, primary_tank, secondary_tank, bulk_settling_time, cld_production, ega, taping_machine, case_erector, machines_data, batch_sku,shift,checkweigher)
                except Exception as insert_error:
                    logging.error(f"\n\nError inserting loop4_overview data after hypertable error: {insert_error}")
                    self.write_db.rollback()
                    os._exit(1)

            else:
                logging.error(f"\n\nError in update_loop4_overview : {e}")
                self.write_db.rollback()
            os._exit(1)

    def insert_into_loop4(self, current_time, primary_tank, secondary_tank, bulk_settling_time, cld_production, ega, taping_machine, case_erector, machines_data,batch_sku,shift,checkweigher):
        try:
            self.db.ensure_db_connection()
            self.db.cursor.execute(self.INSERT_LOOP4_QUERY, (
                    shift,  # shift parameter moved to first position
                    current_time,
                    round(primary_tank, 2),
                    round(secondary_tank, 2),
                    round(bulk_settling_time,2),
                    round(cld_production, 2),
                    round(ega,2) ,
                    taping_machine,
                    case_erector,
                    Json(machines_data),
                    batch_sku,
                    'Offline',
                    checkweigher
                ))

            self.db.commit()
            print(f"Updated data at {current_time} with bulk settling time {bulk_settling_time}")

        except Exception as e:
            logging.error(f"\n\n Error inserting into loop4_overview : {e}")
            self.db.rollback()
            os._exit(1)

def main():

    try :
        config = load_config()
        db = DatabaseConnection(config, "read")
        write_db = DatabaseConnection(config, "write")

        db.connect_db()
        write_db.connect_db()
        monitor = Loop4Monitor(db,write_db, config)

        data_flow_thread = threading.Thread(target=db.ensure_db_connection, daemon=True)
        data_flow_thread.start()

        while True:
            try :
                monitor.update_loop4()
            except Exception as e:
                logging.error(f"\nError in main loop : {e}")
                db.rollback()
                write_db.rollback
                os._exit(1)
            finally:
                logging.info("\nSleeping for 5 seconds before the next iteration...")
                time.sleep(5)

    except KeyboardInterrupt:
        logging.info("Shutting down Loop4 monitor...")
        os._exit(1)
    finally:
        db.close_db_connection()
        write_db.close_db_connection()


if __name__ == "__main__":
    main()

