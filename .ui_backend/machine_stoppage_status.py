import json
import time
import schedule
import psycopg2
from datetime import datetime, timedelta
import logging
import sys
from zoneinfo import ZoneInfo
import os
import glob
import traceback

class MachineStoppageMonitorFromTable:
    """
    Machine stoppage monitor using pre-computed machine_stoppages table.
    Inserts new row ONLY at shift start (or if missing), updates every 5 minutes.
    """
    def __init__(self, config_file="config.json"):
        # === MAIN LOGGER (INFO + DEBUG) ===
        self.logger = logging.getLogger('MachineStoppageMonitorFromTable')
        self.logger.setLevel(logging.INFO)

        # Remove old general log files
        for log_file in glob.glob("machine_stoppage_from_table.log*"):
            try:
                os.remove(log_file)
            except:
                pass

        handler = logging.FileHandler('machine_stoppage_from_table.log', mode='w')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(console_handler)

        # === DEDICATED ERROR LOGGER (date-wise folder) ===
        self.error_logger = logging.getLogger('MachineStoppageErrorLogger')
        self.error_logger.setLevel(logging.ERROR)
        self.error_logger.propagate = False  # prevent double logging

        log_dir = "/home/ai4m/develop/ui_backend/logs"
        date_str = datetime.now().strftime("%Y-%m-%d")
        full_path = os.path.join(log_dir, date_str)
        os.makedirs(full_path, exist_ok=True)  # create date folder if not exists

        error_file = os.path.join(full_path, "machine_stoppage_status.log")
        error_handler = logging.FileHandler(error_file)
        error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
        error_handler.setFormatter(error_formatter)
        self.error_logger.addHandler(error_handler)

        # === Load config ===
        self.config = self.load_config(config_file)
        self.db_config = {
            'dbname': self.config['database']['dbname'],
            'user': self.config['database']['user'],
            'password': self.config['database']['password'],
            'host': self.config['database']['host'],
            'port': self.config['database']['port']
        }
        self.read_conn = None
        self.read_cursor = None
        self.write_conn = None
        self.write_cursor = None

        # SQL Queries (unchanged)
        self.GET_STOPPAGE_COUNT_FROM_TABLE = """
            SELECT COUNT(*)
            FROM machine_stoppages
            WHERE machine = %s
              AND stop_at >= %s AND stop_at < %s
              AND stop_till IS NOT NULL
        """
        self.GET_TOP_3_DURATIONS_FROM_TABLE = """
            SELECT
                SUM(EXTRACT(EPOCH FROM duration)/60) AS total_minutes,
                reason
            FROM machine_stoppages
            WHERE machine = %s
              AND stop_at >= %s AND stop_at < %s
              AND stop_till IS NOT NULL
              AND duration IS NOT NULL
            GROUP BY reason
            ORDER BY total_minutes DESC, reason ASC
            LIMIT 3
        """
        self.GET_REASON_FREQUENCY_FROM_TABLE = """
            SELECT reason, COUNT(*) as frequency
            FROM machine_stoppages
            WHERE machine = %s
              AND stop_at >= %s AND stop_at < %s
              AND stop_till IS NOT NULL
            GROUP BY reason
            ORDER BY frequency DESC, reason ASC
            LIMIT 3
        """
        self.GET_TOTAL_STOPPAGE_TIME_FROM_TABLE = """
            SELECT COALESCE(SUM(EXTRACT(EPOCH FROM duration)/60), 0)
            FROM machine_stoppages
            WHERE machine = %s
              AND stop_at >= %s AND stop_at < %s
              AND stop_till IS NOT NULL
              AND duration IS NOT NULL
        """
        self.INSERT_SHIFT_ROW = """
            INSERT INTO public.machine_stoppage_status
            (timestamp, shift, mc17,mc18,mc19,mc20,mc21,mc22,mc25,mc26,mc27,mc28,mc29,mc30)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (timestamp, shift) DO NOTHING
        """
        self.UPDATE_SHIFT_ROW = """
            UPDATE public.machine_stoppage_status
            SET mc17=%s,mc18=%s,mc19=%s,mc20=%s,mc21=%s,mc22=%s,
                mc25=%s,mc26=%s,mc27=%s,mc28=%s,mc29=%s,mc30=%s
            WHERE timestamp=%s AND shift=%s
        """

    def load_config(self, config_file_path):
        try:
            with open(config_file_path, 'r') as f:
                config = json.load(f)
                self.logger.info(f"Loaded configuration from {config_file_path}")
                return config
        except Exception as e:
            error_msg = f"Error loading config file {config_file_path}: {e}"
            self.error_logger.error(error_msg)
            self.error_logger.error(traceback.format_exc())
            print(error_msg, file=sys.stderr)
            sys.exit(1)

    def connect_db(self):
        try:
            self.read_conn = psycopg2.connect(**self.db_config)
            self.read_cursor = self.read_conn.cursor()
            self.write_conn = psycopg2.connect(**self.db_config)
            self.write_cursor = self.write_conn.cursor()
            self.logger.info("Connected to database successfully")
            return True
        except Exception as e:
            error_msg = f"Database connection error: {e}"
            self.error_logger.error(error_msg)
            self.error_logger.error(traceback.format_exc())
            print(error_msg, file=sys.stderr)
            sys.exit(1)

    def close_db(self):
        for cursor, conn in [(self.read_cursor, self.read_conn), (self.write_cursor, self.write_conn)]:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        self.logger.info("Database connections closed")

    # ... [All other methods unchanged until error logging points] ...

    def get_stoppage_count(self, mc, start_time, end_time):
        try:
            self.read_cursor.execute(self.GET_STOPPAGE_COUNT_FROM_TABLE, (mc, start_time, end_time))
            result = self.read_cursor.fetchone()
            count = int(result[0]) if result else 0
            return count
        except Exception as e:
            error_msg = f"Error getting stoppage count for {mc}: {e}"
            self.error_logger.error(error_msg)
            self.error_logger.error(traceback.format_exc())
            return 0

    def get_stoppage_time(self, mc, start_time, end_time):
        try:
            self.read_cursor.execute(self.GET_TOTAL_STOPPAGE_TIME_FROM_TABLE, (mc, start_time, end_time))
            result = self.read_cursor.fetchone()
            time_minutes = float(result[0]) if result else 0.0
            return int(round(time_minutes))
        except Exception as e:
            error_msg = f"Error getting stoppage time for {mc}: {e}"
            self.error_logger.error(error_msg)
            self.error_logger.error(traceback.format_exc())
            return 0

    def get_top_3_longest_durations(self, mc, start_time, end_time):
        try:
            self.read_cursor.execute(self.GET_TOP_3_DURATIONS_FROM_TABLE, (mc, start_time, end_time))
            results = self.read_cursor.fetchall()
            top_3 = [(int(row[0]), row[1] if row[1] else "Unknown") for row in results]
            while len(top_3) < 3:
                top_3.append((0, ""))
            return top_3
        except Exception as e:
            error_msg = f"Error getting top 3 durations for {mc}: {e}"
            self.error_logger.error(error_msg)
            self.error_logger.error(traceback.format_exc())
            return [(0, ""), (0, ""), (0, "")]

    def get_stoppage_reasons_with_count(self, mc, start_time, end_time):
        try:
            self.read_cursor.execute(self.GET_REASON_FREQUENCY_FROM_TABLE, (mc, start_time, end_time))
            results = self.read_cursor.fetchall()
            return [(int(row[1]), row[0] if row[0] else "Unknown") for row in results]
        except Exception as e:
            error_msg = f"Error getting stoppage reasons for {mc}: {e}"
            self.error_logger.error(error_msg)
            self.error_logger.error(traceback.format_exc())
            return []

    def insert_shift_row_if_missing(self):
        current_time = datetime.now()
        shift, shift_name, start_time, end_time = self.get_shift_and_time_range(current_time)
        row_ts = start_time
        try:
            self.write_cursor.execute(
                "SELECT 1 FROM public.machine_stoppage_status WHERE timestamp=%s AND shift=%s",
                (row_ts, shift)
            )
            if self.write_cursor.fetchone():
                self.logger.info(f"Row already exists for ts={row_ts}, shift={shift}. Skipping insert.")
                return
            defaults = [self._default_json() for _ in range(12)]
            self.write_cursor.execute(
                self.INSERT_SHIFT_ROW,
                (row_ts, shift, *defaults)
            )
            self.write_conn.commit()
            self.logger.info(f" INSERTED new shift row: ts={row_ts}, shift={shift}")
        except Exception as e:
            self.write_conn.rollback()
            error_msg = f"Error inserting shift row: {e}"
            self.error_logger.error(error_msg)
            self.error_logger.error(traceback.format_exc())

    def update_machine_data(self):
        current_time = datetime.now()
        shift, shift_name, start_time, end_time = self.get_shift_and_time_range(current_time)
        row_ts = start_time
        self.logger.info(f"Updating shift {shift} ({shift_name}) | Range: {start_time} → {end_time}")
        all_machines = ['mc17','mc18','mc19','mc20','mc21','mc22','mc25','mc26','mc27','mc28','mc29','mc30']
        machine_data = {}
        try:
            for mc in all_machines:
                stoppage_count = self.get_stoppage_count(mc, start_time, end_time)
                stoppage_time = self.get_stoppage_time(mc, start_time, end_time)
                top_3_durations = self.get_top_3_longest_durations(mc, start_time, end_time)
                stoppage_reasons = self.get_stoppage_reasons_with_count(mc, start_time, end_time)
                updated_durations = self.update_top_3_durations(top_3_durations)
                updated_highest_counts = self.update_highest_count_reasons(stoppage_reasons, top_3_durations)
                if stoppage_time > 0 and stoppage_count == 0:
                    stoppage_count = 1
                updated_highest_counts = self.synchronize_reasons_for_single_stoppage(
                    updated_highest_counts, updated_durations, stoppage_count
                )
                clean = {
                    "stoppage_count": stoppage_count,
                    "stoppage_time": stoppage_time,
                    "longest_duration": updated_durations,
                    "highest_count": updated_highest_counts
                }
                machine_data[mc] = json.dumps(clean)

            # Console pretty print (unchanged)
            print("\n" + "="*80)
            print(f"SHIFT: {shift} ({shift_name}) | TIME: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"RANGE: {start_time} → {end_time}")
            print("="*80)
            for mc in all_machines:
                print(f"\n{mc.upper()}:")
                print("-" * 60)
                data = json.loads(machine_data[mc])
                print(f" Stoppage Count: {data['stoppage_count']}")
                print(f" Stoppage Time: {data['stoppage_time']} minutes")
                print(f" Longest Durations:")
                for k, v in data['longest_duration'].items():
                    if v[0] > 0:
                        print(f" {k}. {v[0]} min - {v[1]}")
                print(f" Highest Count Reasons:")
                for k, v in data['highest_count'].items():
                    if v[0] > 0:
                        print(f" {k}. {v[0]} occ - {v[1]}")
            print("\n" + "="*80)

            # Database update
            self.write_cursor.execute(
                self.UPDATE_SHIFT_ROW,
                (
                    machine_data['mc17'], machine_data['mc18'], machine_data['mc19'],
                    machine_data['mc20'], machine_data['mc21'], machine_data['mc22'],
                    machine_data['mc25'], machine_data['mc26'], machine_data['mc27'],
                    machine_data['mc28'], machine_data['mc29'], machine_data['mc30'],
                    row_ts, shift
                )
            )
            self.write_conn.commit()
            self.logger.info(f" UPDATED row: ts={row_ts}, shift={shift}")
        except Exception as e:
            self.write_conn.rollback()
            error_msg = f"Error in update_machine_data: {e}"
            self.error_logger.error(error_msg)
            self.error_logger.error(traceback.format_exc())

    # ... [rest of the methods unchanged: get_shift_and_time_range, update_highest_count_reasons, etc.] ...

    def get_shift_and_time_range(self, current_time):
        ist = ZoneInfo("Asia/Kolkata")
        current_time = current_time.replace(tzinfo=ist)
        for shift_config in self.config.get('shifts', []):
            start_hour = shift_config['start_hour']
            end_hour = shift_config['end_hour']
            shift_name = shift_config['name']
            shift_map = {"Shift_1": "A", "Shift_2": "B", "Shift_3": "C"}
            shift = shift_map.get(shift_name, "C")
            if start_hour < end_hour:
                if start_hour <= current_time.hour < end_hour:
                    start_time = current_time.replace(hour=start_hour, minute=0, second=0, microsecond=0)
                    end_time = current_time.replace(hour=end_hour, minute=0, second=0, microsecond=0)
                    return shift, shift_name, start_time, end_time
            else:
                if current_time.hour >= start_hour or current_time.hour < end_hour:
                    if current_time.hour >= start_hour:
                        start_time = current_time.replace(hour=start_hour, minute=0, second=0, microsecond=0)
                        end_time = current_time.replace(hour=end_hour, minute=0, second=0, microsecond=0) + timedelta(days=1)
                    else:
                        start_time = (current_time - timedelta(days=1)).replace(hour=start_hour, minute=0, second=0, microsecond=0)
                        end_time = current_time.replace(hour=end_hour, minute=0, second=0, microsecond=0)
                    return shift, shift_name, start_time, end_time
        self.logger.warning("No matching shift found, defaulting to Shift C")
        start_time = current_time.replace(hour=23, minute=0, second=0, microsecond=0)
        end_time = current_time.replace(hour=7, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return "C", "Shift_3", start_time, end_time

    def update_highest_count_reasons(self, new_stoppage_reasons, top_3_durations):
        all_reasons = {}
        for count, reason in new_stoppage_reasons:
            if reason and count > 0:
                all_reasons[reason] = count
        for duration, reason in top_3_durations:
            if reason and duration > 0 and reason not in all_reasons:
                all_reasons[reason] = 1
        sorted_reasons = sorted(all_reasons.items(), key=lambda x: (-x[1], x[0]))[:3]
        updated = {}
        for idx, (reason, count) in enumerate(sorted_reasons, 1):
            updated[str(idx)] = [count, reason]
        while len(updated) < 3:
            updated[str(len(updated) + 1)] = [0, ""]
        return updated

    def update_top_3_durations(self, new_top_3_durations):
        updated = {}
        for idx, d in enumerate(new_top_3_durations[:3], 1):
            updated[str(idx)] = list(d)
        while len(updated) < 3:
            updated[str(len(updated) + 1)] = [0, ""]
        return updated

    def synchronize_reasons_for_single_stoppage(self, highest_counts, durations, stoppage_count):
        if stoppage_count == 1:
            reason = durations.get("1", [0, ""])[1]
            if reason:
                highest_counts = {"1": [1, reason], "2": [0, ""], "3": [0, ""]}
        return highest_counts

    def _default_json(self):
        return json.dumps({
            "stoppage_count": 0,
            "stoppage_time": 0,
            "longest_duration": {"1": [0, ""], "2": [0, ""], "3": [0, ""]},
            "highest_count": {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
        })

    def schedule_shift_inserts(self):
        ist = ZoneInfo("Asia/Kolkata")
        now = datetime.now(ist)
        for shift_config in self.config.get('shifts', []):
            start_hour = shift_config['start_hour']
            shift_name = shift_config['name']
            shift_map = {"Shift_1": "A", "Shift_2": "B", "Shift_3": "C"}
            shift = shift_map.get(shift_name, "C")
            today_start = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
            if today_start <= now:
                next_run = today_start + timedelta(days=1)
            else:
                next_run = today_start
            schedule.every().day.at(f"{start_hour:02d}:00").do(self.insert_shift_row_if_missing)
            self.logger.info(f" Scheduled INSERT for {shift_name} ({shift}) at {start_hour:02d}:00 daily")

    def run(self):
        if not self.connect_db():
            return
        try:
            self.logger.info(" Starting up - checking current shift row...")
            self.insert_shift_row_if_missing()
            self.schedule_shift_inserts()
            schedule.every(5).minutes.do(self.update_machine_data)
            self.logger.info(" Scheduler started:")
            self.logger.info(" → INSERT: Only at shift start (or if missing)")
            self.logger.info(" → UPDATE: Every 5 minutes")
            self.update_machine_data()
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info(" Stopped by user")
        except Exception as e:
            error_msg = f"Unexpected error in run loop: {e}"
            self.error_logger.error(error_msg)
            self.error_logger.error(traceback.format_exc())
        finally:
            self.close_db()

if __name__ == "__main__":
    monitor = MachineStoppageMonitorFromTable()
    monitor.run()
