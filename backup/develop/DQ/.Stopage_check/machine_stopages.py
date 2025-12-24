import json
import time
import psycopg2
from datetime import datetime, timedelta
import pytz
import logging
from pathlib import Path

def setup_dated_logger() -> logging.Logger:
    """
    Creates a logger that writes to:
        /home/ai4m/develop/ui_backend/logs/<YYYY-MM-DD>/machine_stopages.log
    Creates the directory if it doesn't exist.
    """
    log_root = Path("/home/ai4m/develop/ui_backend/logs")
    today_str = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d")
    log_dir = log_root / today_str
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "machine_stopages.log"

    logger = logging.getLogger("MachineStopages")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


log = setup_dated_logger()

print = log.info  


class MachineStoppageMonitor:
    def __init__(self, source_db, target_db, status_code_files):
        self.source_db = source_db
        self.target_db = target_db
        self.status_code_files = status_code_files
        self.previous_statuses = {}
        self.last_processed_timestamps = {}
        self.status_codes = {}
        self.machine_configs = {}
        self.source_conn = self._connect_to_db(source_db)
        self.target_conn = self._connect_to_db(target_db)
        self.machines = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22', 'mc25', 'mc26', 'mc27', 'mc30']
        self._initialize_machine_configs()
        self.load_status_codes()
        self.ist = pytz.timezone('Asia/Kolkata')
        self._initialize_monitoring()

    def _initialize_machine_configs(self):
        for machine in self.machines:
            if machine in ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']:
                group = 'loop3'
                status_code_col = 'status_code'
                table = machine
            elif machine in ['mc25', 'mc26']:
                group = 'mc25'
                status_code_col = 'status_code'
                table = f"{machine}_fast"
            elif machine == 'mc27':
                group = 'mc27'
                status_code_col = 'status_code'
                table = f"{machine}_fast"
            elif machine == 'mc30':
                group = 'mc30'
                status_code_col = 'status_code'
                table = f"{machine}_fast"
            self.machine_configs[machine] = {
                'group': group,
                'table': table,
                'status_code_col': status_code_col
            }

    def _connect_to_db(self, db_config):
        max_retries = 3
        retry_delay = 5
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(
                    dbname=db_config["dbname"],
                    user=db_config["user"],
                    password=db_config["password"],
                    host=db_config["host"],
                    port=db_config["port"],
                    connect_timeout=5
                )
                log.info(f"Successfully connected to database {db_config['dbname']}")
                return conn
            except psycopg2.OperationalError as e:
                log.error(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt == max_retries - 1:
                    log.critical("Max retries reached. Exiting.")
                    raise
                time.sleep(retry_delay)

    def _check_connection(self, conn):
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except psycopg2.InterfaceError:
            return False

    def _reconnect(self, db_config):
        try:
            new_conn = self._connect_to_db(db_config)
            return new_conn
        except psycopg2.Error as e:
            log.error(f"Failed to reconnect: {e}")
            return None

    def load_status_codes(self):
        """Load status codes from JSON files."""
        for machine_group, file_path in self.status_code_files.items():
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                    self.status_codes[machine_group] = {
                        str(item["fault_code"]): item["message"]
                        for item in data["faults"]
                    }
                log.info(f"Loaded {len(self.status_codes[machine_group])} fault codes for {machine_group}")
            except Exception as e:
                log.error(f"Error loading status codes for {machine_group}: {e}")
                self.status_codes[machine_group] = {}

    def _initialize_monitoring(self):
        for machine in self.machines:
            latest = self._get_latest_for_machine(machine)
            if latest:
                self.previous_statuses[machine] = {'status': latest['status']}
                self.last_processed_timestamps[machine] = latest['timestamp']
            else:
                self.previous_statuses[machine] = {'status': 1}
                self.last_processed_timestamps[machine] = datetime.now(self.ist) - timedelta(seconds=1)
            log.info(f"Initialized {machine}: status={self.previous_statuses[machine]['status']}")

    def _get_latest_for_machine(self, machine):
        config = self.machine_configs[machine]
        table = config['table']
        col = config['status_code_col']
        query = f"""
            SELECT status, {col} AS status_code, timestamp
            FROM {table}
            WHERE timestamp = (SELECT MAX(timestamp) FROM {table})
        """
        try:
            if not self._check_connection(self.source_conn):
                log.warning(f"Source connection lost for {machine}, reconnecting...")
                self.source_conn = self._reconnect(self.source_db)
                if not self.source_conn:
                    return None
            cursor = self.source_conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            if row:
                return {'status': row[0], 'status_code': row[1], 'timestamp': row[2]}
            return None
        except Exception as e:
            log.error(f"Error fetching latest for {machine}: {e}")
            return None

    def _get_recent_data_for_machine(self, machine, last_ts):
        config = self.machine_configs[machine]
        table = config['table']
        col = config['status_code_col']
        query = f"""
            SELECT timestamp, status, {col} AS status_code
            FROM {table}
            WHERE timestamp > %s
            ORDER BY timestamp ASC
        """
        try:
            if not self._check_connection(self.source_conn):
                log.warning(f"Source connection lost during fetch for {machine}, reconnecting...")
                self.source_conn = self._reconnect(self.source_db)
                if not self.source_conn:
                    return []
            cursor = self.source_conn.cursor()
            cursor.execute(query, (last_ts,))
            return cursor.fetchall()
        except Exception as e:
            log.error(f"Error fetching recent data for {machine}: {e}")
            return []

    def insert_stoppage(self, machine, reason, stop_at):
        query = """
        INSERT INTO machine_stoppages (machine, stop_at, reason) VALUES (%s, %s, %s);
        """
        try:
            if not self._check_connection(self.target_conn):
                log.warning("Target DB connection lost, reconnecting...")
                self.target_conn = self._reconnect(self.target_db)
                if not self.target_conn:
                    return False
            cursor = self.target_conn.cursor()
            cursor.execute(query, (machine, stop_at.replace(microsecond=0), reason))
            self.target_conn.commit()
            log.info(f"Inserted stoppage: Machine={machine}, Stop_At={stop_at}, Reason={reason}")
            return True
        except Exception as e:
            log.error(f"Error inserting stoppage for {machine}: {e}")
            return False

    def update_stoppage(self, machine, stop_till):
        try:
            if not self._check_connection(self.target_conn):
                log.warning("Target DB connection lost, reconnecting...")
                self.target_conn = self._reconnect(self.target_db)
                if not self.target_conn:
                    return False
            cursor = self.target_conn.cursor()
            cursor.execute("SELECT stop_at FROM machine_stoppages WHERE machine = %s AND stop_till IS NULL;", (machine,))
            stop_at = cursor.fetchone()
            if stop_at:
                stop_at_time = stop_at[0]
                stop_till_time = stop_till.replace(microsecond=0)
                duration = str(stop_till_time - stop_at_time).split('.')[0]
                query = """
                UPDATE machine_stoppages
                SET stop_till = %s, duration = %s
                WHERE machine = %s AND stop_till IS NULL;
                """
                cursor.execute(query, (stop_till_time, duration, machine))
                self.target_conn.commit()
                log.info(f"Updated stoppage: Machine={machine}, Stop_Till={stop_till_time}, Duration={duration}")
                return True
            else:
                log.warning(f"No active stoppage found for {machine}.")
                return False
        except Exception as e:
            log.error(f"Error updating stoppage for {machine}: {e}")
            return False

    def monitor_changes(self):
        log.info("Starting machine status monitoring...")
        while True:
            try:
                for machine in self.machines:
                    last_ts = self.last_processed_timestamps[machine]
                    rows = self._get_recent_data_for_machine(machine, last_ts)
                    if not rows:
                        continue
                    prev_status = self.previous_statuses[machine]['status']
                    status_code_dict = self.status_codes.get(self.machine_configs[machine]['group'], {})
                    for row in rows:
                        ts, status, status_code = row
                        if status is None or ts is None:
                            continue
                        current_status = status
                        current_code = int(status_code) if status_code is not None else 0
                        reason = status_code_dict.get(str(current_code), "Manual Stop")
                        if prev_status == 1 and current_status != 1:
                            self.insert_stoppage(machine.replace("_fast", ""), reason, ts)
                        elif prev_status != 1 and current_status == 1:
                            self.update_stoppage(machine.replace("_fast", ""), ts)
                        prev_status = current_status
                    self.previous_statuses[machine] = {'status': prev_status}
                    self.last_processed_timestamps[machine] = rows[-1][0]
                time.sleep(6)
            except KeyboardInterrupt:
                log.info("Monitoring stopped by user.")
                break
            except Exception as e:
                log.error(f"Unexpected error in monitoring loop: {e}")
                time.sleep(10)

    def close(self):
        if hasattr(self, 'source_conn') and self.source_conn and not self.source_conn.closed:
            self.source_conn.close()
        if hasattr(self, 'target_conn') and self.target_conn and not self.target_conn.closed:
            self.target_conn.close()
        log.info("Database connections closed.")


if __name__ == "__main__":
    source_db_config = {
        "dbname": "hul",
        "user": "postgres",
        "password": "ai4m2024",
        "host": "100.96.244.68",
        "port": "5432"
    }
    target_db_config = {
        "dbname": "hul",
        "user": "postgres",
        "password": "ai4m2024",
        "host": "100.96.244.68",
        "port": "5432"
    }
    status_code_files = {
        'loop3': 'loop3.json',
        'mc25': 'mc25.json',
        'mc26': 'mc25.json',
        'mc27': 'mc27.json',
        'mc30': 'mc30.json'
    }

    monitor = None
    try:
        monitor = MachineStoppageMonitor(source_db_config, target_db_config, status_code_files)
        monitor.monitor_changes()
    except KeyboardInterrupt:
        log.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        log.critical(f"Fatal error: {e}")
    finally:
        if monitor:
            monitor.close()
