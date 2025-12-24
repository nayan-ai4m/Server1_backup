import psycopg2
from datetime import datetime, timedelta
import time
import json
import logging
import os
import traceback

class MachineUptime:
    def __init__(self, config_file='config.json'):
        # === SETUP DEDICATED ERROR LOGGER (date-wise) ===
        self.error_logger = logging.getLogger('MachineUptimeErrorLogger')
        self.error_logger.setLevel(logging.ERROR)
        self.error_logger.propagate = False  # Avoid duplicate logging

        log_dir = "/home/ai4m/develop/ui_backend/logs"
        date_str = datetime.now().strftime("%Y-%m-%d")
        full_path = os.path.join(log_dir, date_str)
        os.makedirs(full_path, exist_ok=True)  # Auto-create date folder

        error_file = os.path.join(full_path, "machine_uptime.log")
        handler = logging.FileHandler(error_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
        handler.setFormatter(formatter)
        self.error_logger.addHandler(handler)

        # === Load config ===
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
        except Exception as e:
            error_msg = f"Failed to load config file '{config_file}': {e}"
            self.error_logger.error(error_msg)
            self.error_logger.error(traceback.format_exc())
            print(error_msg)
            raise

        self.conn_params = {
            key: config['database'][key]
            for key in ['dbname', 'user', 'password', 'host', 'port']
            if key in config['database']
        }

        self.machines = ["mc17", "mc18", "mc19", "mc20", "mc21", "mc22",
                         "mc25", "mc26", "mc27", "mc28", "mc29", "mc30"]
        self.shifts = config['shifts']
        self.cld_machines = config['machines']['cld']

    def get_current_shift(self):
        now = datetime.now()
        print("\nCurrent time = ", now)

        for shift in self.shifts:
            shift_name = shift['name']
            start_hour = shift['start_hour']
            end_hour = shift['end_hour']

            if start_hour > end_hour:  # Overnight shift
                if now.hour >= start_hour or now.hour < end_hour:
                    shift_start = (now if now.hour >= start_hour else now - timedelta(days=1)).replace(
                        hour=start_hour, minute=0, second=0, microsecond=0)
                    shift_end = (now if now.hour >= start_hour else now).replace(
                        hour=end_hour, minute=0, second=0, microsecond=0)
                    if now.hour < end_hour:
                        shift_end += timedelta(days=1)
                    return shift_start, shift_end, shift_name, now
            else:
                if start_hour <= now.hour < end_hour:
                    shift_start = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
                    shift_end = now.replace(hour=end_hour, minute=0, second=0, microsecond=0)
                    return shift_start, shift_end, shift_name, now

        print("Warning: No matching shift found, defaulting to Shift_3")
        shift = self.shifts[-1]
        shift_start = (now - timedelta(days=1)).replace(hour=shift['start_hour'], minute=0, second=0, microsecond=0)
        shift_end = shift_start + timedelta(hours=8)
        return shift_start, shift_end, shift['name'], now

    def calculate_and_update_uptime(self):
        while True:
            conn = None
            try:
                conn = psycopg2.connect(**self.conn_params)
                shift_start, shift_end, shift_label, now = self.get_current_shift()

                print("=" * 60)
                print(f"Shift Label: {shift_label}")
                print(f"Shift Start Time: {shift_start}")
                print(f"Current Time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 60)

                for machine in self.machines:
                    column_name = "state" if machine in self.cld_machines else "status"

                    query = f"""
                    WITH time_differences AS (
                        SELECT
                            timestamp, {column_name},
                            EXTRACT(EPOCH FROM (LEAD(timestamp) OVER (ORDER BY timestamp) - timestamp)) AS time_diff
                        FROM {machine}
                        WHERE timestamp >= %s AND timestamp < %s
                    )
                    SELECT SUM(CASE WHEN {column_name} = 1 THEN COALESCE(time_diff, 0) ELSE 0 END) AS running_seconds
                    FROM time_differences;
                    """

                    with conn.cursor() as cur:
                        cur.execute(query, (shift_start, shift_end))
                        result = cur.fetchone()
                        running_seconds = result[0] if result[0] else 0

                    hours, remainder = divmod(int(running_seconds), 3600)
                    minutes, seconds = divmod(remainder, 60)
                    formatted_uptime = f"{hours:02}:{minutes:02}:{seconds:02}"

                    print(f"{machine.upper()} Uptime: {formatted_uptime}")

                    update_query = """
                    UPDATE cld_history
                    SET machine_uptime = %s
                    WHERE machine_no = %s
                    AND timestamp = %s
                    AND shift = %s;
                    """

                    with conn.cursor() as cur:
                        cur.execute(update_query, (formatted_uptime, machine, shift_start, shift_label))
                        conn.commit()

                    print(f"Updated uptime for {machine.upper()} in cld_history.")

                print("=" * 60)

            except Exception as e:
                error_msg = f"Error in calculate_and_update_uptime: {e}"
                self.error_logger.error(error_msg)
                self.error_logger.error(traceback.format_exc())
                print(f"ERROR: {e}")  # Still show on console
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass

            print("Waiting for 5 sec before the next update...")
            time.sleep(5)


if __name__ == "__main__":
    try:
        uptime = MachineUptime()
        uptime.calculate_and_update_uptime()
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        # Final fallback error logging
        logger = logging.getLogger('MachineUptimeErrorLogger')
        date_str = datetime.now().strftime("%Y-%m-%d")
        full_path = os.path.join("/home/ai4m/develop/ui_backend/logs", date_str)
        os.makedirs(full_path, exist_ok=True)
        error_file = os.path.join(full_path, "machine_uptime.log")
        handler = logging.FileHandler(error_file)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        logger.error(f"Critical startup error: {e}")
        logger.error(traceback.format_exc())
        print(f"CRITICAL ERROR: {e}")
