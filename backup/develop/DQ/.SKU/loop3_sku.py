import multiprocessing as mp
import datetime
from pycomm3 import LogixDriver
import psycopg2
import time
import os
import threading
import logging
from psycopg2.extras import Json
import sys  
import signal  

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('pycomm3').setLevel(logging.CRITICAL)


class DatabaseConnection:
    def __init__(self, host, database, user, password):
        self.conn_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password
        }
        self.conn = None
        self.cursor = None
        self.connect_db()

    def connect_db(self):
        try:
            self.close_db_connection()
            self.conn = psycopg2.connect(**self.conn_params)
            self.cursor = self.conn.cursor()
            logging.info("Successfully connected to database")
            return True
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            return False

    def close_db_connection(self):
        try:
            if self.cursor and not self.cursor.closed:
                self.cursor.close()
        except Exception as e:
            logging.error(f"Error closing cursor: {e}")
        try:
            if self.conn and not self.conn.closed:
                self.conn.close()
        except Exception as e:
            logging.error(f"Error closing connection: {e}")
        self.cursor = None
        self.conn = None

    def ensure_db_connection(self):
        try:
            if self.conn is None or self.conn.closed != 0 or self.cursor is None or self.cursor.closed:
                logging.warning("Database connection lost, attempting to reconnect...")
                return self.connect_db()
         
            self.cursor.execute("SELECT 1")
            self.cursor.fetchone()
            return True
        except Exception as e:
            logging.error(f"Database connection test failed: {e}")
            return self.connect_db()

    def commit(self):
        if self.conn and not self.conn.closed:
            try:
                self.conn.commit()
                return True
            except Exception as e:
                logging.error(f"Commit failed: {e}")
                return False
        return False

    def rollback(self):
        if self.conn and not self.conn.closed:
            try:
                self.conn.rollback()
                return True
            except Exception as e:
                logging.error(f"Rollback failed: {e}")
                return False
        return False

    def execute_query(self, query, params=None, max_retries=3):
        retries = 0
        while retries < max_retries:
            try:
                if not self.ensure_db_connection():
                    logging.error("Failed to ensure database connection")
                    retries += 1
                    time.sleep(2)
                    continue
                
                self.cursor.execute(query, params)
                return True
            except psycopg2.OperationalError as e:
                logging.error(f"Operational error on attempt {retries+1}: {e}")
                self.rollback()
                self.connect_db()
                retries += 1
                time.sleep(2)
            except Exception as e:
                logging.error(f"Query execution failed on attempt {retries+1}: {e}")
                self.rollback()
                retries += 1
                time.sleep(2)
                
        logging.error(f"Query execution failed after {max_retries} attempts")
        return False


INSERT_QUERY = """INSERT INTO loop3_sku("timestamp", "ht_transfer_ts", "level", "primary_tank", "secondary_tank", "batch_no", "mfg_date", "batch_sku", "shift","motor_status") 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s);"""


def read_plc_tag(tag_queue, plc_ip, stop_event):
    # Adjusted tag names - make sure these match what's in your PLC
    tag_name = [
        "MX02_HT_Transfer_TS", "HTloop3_Level", "HTloop3_Primary_Tank",
        "HTloop3_Secondary_Tank", "MX02_Batch_No_D", "MX02_Batch_Mfg_Date",
        "MX02_Batch_SKU", "MX02_Shift_D","HT_CM5801.RunStopStatus"
    ]
    
    reconnect_delay = 5
    read_interval = 60
    consecutive_failures = 0
    max_consecutive_failures = 5

    while not stop_event.is_set():
        try:
            logging.info(f"Connecting to PLC at {plc_ip}")
            with LogixDriver(plc_ip) as plc:
                time.sleep(reconnect_delay)  # Allow PLC to initialize
                logging.info("PLC connection established")
                consecutive_failures = 0

                while not stop_event.is_set():
                    tags = {
                        "timestamp": datetime.datetime.now(),
                        "mx03_ht_transfer_ts": None,
                        "htloop4_level": None,
                        "htloop4_primary_tank": None,
                        "htloop4_secondary_tank": None,
                        "mx03_batch_no_d": None,
                        "mx03_batch_mfg_date": None,
                        "mx03_batch_sku": None,
                        "mx03_shift_d": None,
                        "runstopstatus":None,
                    }

                    success_count = 0
                    for i, tag in enumerate(tag_name):
                        try:
                            result = plc.read(tag)
                            if result is not None and result.value is not None:
                                success_count += 1
                                if i == 0:
                                    tags["mx03_ht_transfer_ts"] = result.value
                                elif i == 1:
                                    tags["htloop4_level"] = result.value
                                elif i == 2:
                                    tags["htloop4_primary_tank"] = result.value
                                elif i == 3:
                                    tags["htloop4_secondary_tank"] = result.value
                                elif i == 4:
                                    tags["mx03_batch_no_d"] = result.value
                                elif i == 5:
                                    tags["mx03_batch_mfg_date"] = result.value
                                elif i == 6:
                                    tags["mx03_batch_sku"] = result.value
                                elif i == 7:
                                    tags["mx03_shift_d"] = result.value
                                elif i ==8 :
                                    tags["runstopstatus"] = int(result.value)

                            else:
                                logging.warning(f"Read failed for tag: {tag}")
                        except Exception as e:
                            logging.error(f"Error reading tag {tag}: {e}")
                    
                    if success_count == 0:
                        consecutive_failures += 1
                        logging.error(f"Failed to read any tags. Failure count: {consecutive_failures}/{max_consecutive_failures}")
                        if consecutive_failures >= max_consecutive_failures:
                            logging.error("Exceeded maximum consecutive failures. Reconnecting to PLC...")
                            break
                    else:
                        consecutive_failures = 0
                        tag_queue.put(tags)
                        logging.info(f"Read {success_count} tags successfully. Data queued.")
                    
                    # Sleep for the read interval or until stop event is set
                    for _ in range(int(read_interval / 0.1)):
                        if stop_event.is_set():
                            break
                        time.sleep(0.1)

        except Exception as e:
            logging.error(f"PLC connection error: {e}")
            for _ in range(int(reconnect_delay * 10)):
                if stop_event.is_set():
                    break
                time.sleep(0.1)


def insert_into_postgres(tag_queue, db_conn, stop_event):
    consecutive_failures = 0
    max_consecutive_failures = 5
    
    while not stop_event.is_set():
        try:
            if not tag_queue.empty():
                data = tag_queue.get()

                # Count non-None values (excluding timestamp)
                non_none_values = sum(1 for k, v in data.items() if k != "timestamp" and v is not None)
                
                if non_none_values == 0:
                    logging.warning("Skipped inserting row with only timestamp (rest all None)")
                    continue

                logging.info(f"Attempting to insert data: {data}")
                
                if db_conn.execute_query(INSERT_QUERY, (
                    data['timestamp'],
                    data['mx03_ht_transfer_ts'],
                    data['htloop4_level'],
                    data['htloop4_primary_tank'],
                    data['htloop4_secondary_tank'],
                    data['mx03_batch_no_d'],
                    data['mx03_batch_mfg_date'],
                    data['mx03_batch_sku'],
                    data['mx03_shift_d'],
                    data['runstopstatus']
                )):
                    if db_conn.commit():
                        logging.info(f"Successfully inserted data with {non_none_values} non-null values")
                        consecutive_failures = 0
                    else:
                        consecutive_failures += 1
                        logging.error(f"Failed to commit data. Failure count: {consecutive_failures}/{max_consecutive_failures}")
                else:
                    consecutive_failures += 1
                    logging.error(f"Failed to execute query. Failure count: {consecutive_failures}/{max_consecutive_failures}")
                
                if consecutive_failures >= max_consecutive_failures:
                    logging.error("Exceeded maximum consecutive database failures. Reconnecting...")
                    db_conn.connect_db()
                    consecutive_failures = 0
            else:
                time.sleep(0.1)
        except Exception as e:
            logging.error(f"Error in database insertion thread: {e}")
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                logging.error("Exceeded maximum consecutive failures in database thread. Reconnecting...")
                db_conn.connect_db()
                consecutive_failures = 0
            time.sleep(1)


def connection_monitor(db_conn, processes, stop_event):
    while not stop_event.is_set():
        try:
           
            db_conn.ensure_db_connection()
            for p in processes:
                if not p.is_alive():
                    logging.error(f"Process {p.name} is not alive. Signaling shutdown...")
                    stop_event.set()
                    return
                    
            time.sleep(15)  
        except Exception as e:
            logging.error(f"Error in connection monitor: {e}")
            time.sleep(5)


def main():
    stop_event = threading.Event()
    
    def signal_handler(sig, frame):
        logging.info("Received shutdown signal. Cleaning up...")
        stop_event.set()
        time.sleep(2)  
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Create database connection
        db = DatabaseConnection(
            host='localhost',
            database='hul',
            user='postgres',
            password='ai4m2024'
        )

        manager = mp.Manager()
        tag_queue = manager.Queue()

        plc_ip = '141.141.142.11'

        processes = [
            mp.Process(target=read_plc_tag, args=(tag_queue, plc_ip, stop_event), name="PLC_Reader"),
            mp.Process(target=insert_into_postgres, args=(tag_queue, db, stop_event), name="DB_Writer")
        ]

        for p in processes:
            p.start()

        # Monitor thread to check connections and processes
        monitor_thread = threading.Thread(
            target=connection_monitor, 
            args=(db, processes, stop_event), 
            daemon=True, 
            name="Connection_Monitor"
        )
        monitor_thread.start()

        # Main thread waits here
        while not stop_event.is_set():
            alive = all(p.is_alive() for p in processes)
            if not alive:
                logging.error("One or more processes died. Shutting down...")
                stop_event.set()
                break
            time.sleep(5)

        # Cleanup
        logging.info("Waiting for processes to terminate...")
        for p in processes:
            p.join(timeout=10)
            if p.is_alive():
                logging.warning(f"Process {p.name} did not terminate gracefully. Terminating...")
                p.terminate()
        
        db.close_db_connection()
        logging.info("Clean shutdown completed")

    except Exception as e:
        logging.error(f"Unhandled exception in main: {e}")
        stop_event.set()
        sys.exit(1)


if __name__ == '__main__':
    main()
