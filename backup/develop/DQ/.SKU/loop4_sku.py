import multiprocessing as mp
import datetime
import logging
import os
import signal
import sys
import time
from pycomm3 import LogixDriver
import psycopg2

# Set up logging
log_dir = "/home/ai4m/develop/backup/develop/DQ/SKU/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "loop4_sku.log")

# Configure the root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

# Disable verbose logging from pycomm3
logging.getLogger('pycomm3').setLevel(logging.WARNING)
logging.getLogger('pycomm3.cip_driver').setLevel(logging.WARNING)
logger = logging.getLogger("loop4_sku")

# PostgreSQL connection settings
DB_SETTINGS = {
    'host': 'localhost',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'connect_timeout': 10
}

# Insert query for PostgreSQL
insert_query = """
INSERT INTO loop4_sku(
    "timestamp", 
    "ht_transfer_ts", 
    "level", 
    "primary_tank", 
    "secondary_tank", 
    "batch_no", 
    "mfg_date", 
    "batch_sku", 
    "shift"
) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

# Global exit flag for all processes
exit_flag = False

# Function to read tags from PLC
def read_plc_tag(tag_queue):
    """Read tags from PLC and put them into the queue."""
    plc_ip = '141.141.142.31'
    logger.info(f"Starting PLC reader process for IP: {plc_ip}")
    
    tag_names = [
        "MX03_HT_Transfer_TS", 
        "HTloop4_Level", 
        "HTloop4_Primary_Tank", 
        "HTloop4_Secondary_Tank", 
        "MX03_Batch_No_D", 
        "MX03_Batch_Mfg_date", 
        "MX03_Batch_SKU", 
        "MX03_Shift_D"
    ]
    
    consecutive_errors = 0
    max_consecutive_errors = 10
    connection_retry_delay = 5
    
    # Set up signal handler
    def signal_handler(sig, frame):
        global exit_flag
        logger.info("PLC process received termination signal")
        exit_flag = True
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    global exit_flag
    while not exit_flag:
        try:
            with LogixDriver(plc_ip) as plc:
                logger.info(f"Connected to PLC at {plc_ip}")
                consecutive_errors = 0
                
                while not exit_flag:
                    try:
                        # Read tags
                        tag_values = plc.read(*tag_names)
                        
                        # Check for tag read errors
                        if any(tag.error for tag in tag_values):
                            error_tags = [tag.tag for tag in tag_values if tag.error]
                            logger.warning(f"Error reading tags: {error_tags}")
                            consecutive_errors += 1
                            if consecutive_errors >= max_consecutive_errors:
                                logger.error(f"Too many consecutive errors ({consecutive_errors}), reconnecting to PLC")
                                break
                            time.sleep(5)
                            continue
                        
                        # Reset error counter on successful read
                        consecutive_errors = 0
                        
                        # Process tag values
                        tags = {
                            "timestamp": datetime.datetime.now(),
                            "mx03_ht_transfer_ts": tag_values[0].value,
                            "htloop4_level": tag_values[1].value,
                            "htloop4_primary_tank": tag_values[2].value,
                            "htloop4_secondary_tank": tag_values[3].value,
                            "mx03_batch_no_d": tag_values[4].value,
                            "mx03_batch_mfg_date": tag_values[5].value,
                            "mx03_batch_sku": tag_values[6].value,
                            "mx03_shift_d": tag_values[7].value,
                        }
                        
                        # Log values for debugging if needed
                        logger.debug(f"Read PLC tags: {tags}")
                        
                        # Put tags in queue for DB process
                        tag_queue.put(tags)
                        
                        # Wait before next poll
                        time.sleep(60)
                        
                    except Exception as e:
                        logger.error(f"PLC READ ERROR: {e}")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            logger.error(f"Too many consecutive errors ({consecutive_errors}), reconnecting to PLC")
                            break
                        time.sleep(5)
                        
        except Exception as e:
            logger.error(f"PLC CONNECTION ERROR: {e}")
            consecutive_errors += 1
            
            # If we've had too many errors, pause longer to avoid thrashing
            if consecutive_errors > max_consecutive_errors:
                logger.error(f"Too many connection errors ({consecutive_errors}), waiting longer before retry")
                time.sleep(60)  # Wait a full minute before trying again
            else:
                time.sleep(connection_retry_delay)
    
    logger.info("PLC reader process stopping")


# Function to insert tag values into PostgreSQL
def insert_into_postgres(tag_queue):
    """Insert tag values into PostgreSQL."""
    logger.info("Starting database insertion process")
    
    # Set up signal handler
    def signal_handler(sig, frame):
        global exit_flag
        logger.info("DB process received termination signal")
        exit_flag = True
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    conn = None
    cursor = None
    
    global exit_flag
    while not exit_flag:
        try:
            # Check if we need to reconnect
            if conn is None or cursor is None:
                # Connect to PostgreSQL
                conn = psycopg2.connect(**DB_SETTINGS)
                cursor = conn.cursor()
                logger.info("Connected to PostgreSQL database")
            
            # Try to get data from queue, with timeout
            try:
                # Non-blocking with timeout
                if not tag_queue.empty():
                    data = tag_queue.get(block=False)
                    
                    # Insert data into database
                    cursor.execute(insert_query, (
                        data['timestamp'],
                        data['mx03_ht_transfer_ts'],
                        data['htloop4_level'],
                        data['htloop4_primary_tank'],
                        data['htloop4_secondary_tank'],
                        data['mx03_batch_no_d'],
                        data['mx03_batch_mfg_date'],
                        data['mx03_batch_sku'],
                        data['mx03_shift_d']
                    ))
                    conn.commit()
                    logger.info(f"INSERTED: {data['timestamp']} - Batch: {data['mx03_batch_no_d']} - SKU: {data['mx03_batch_sku']}")
                else:
                    # No data in queue, wait a bit
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Queue error: {e}")
                time.sleep(1)
                
        except psycopg2.OperationalError as e:
            logger.error(f"Database connection error: {e}")
            # Close any broken connections
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
                cursor = None
            if conn:
                try:
                    conn.close()
                except:
                    pass
                conn = None
            # Wait before trying to reconnect
            time.sleep(10)
            
        except Exception as e:
            logger.error(f"POSTGRES INSERT ERROR: {e}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            time.sleep(5)
    
    # Clean up
    logger.info("DB insertion process stopping")
    if cursor:
        try:
            cursor.close()
        except:
            pass
    if conn:
        try:
            conn.close()
        except:
            pass


# Main function
def main():
    logger.info("Starting loop4_sku service")
    
    # Set multiprocessing start method if needed
    if mp.get_start_method(allow_none=True) is None:
        try:
            mp.set_start_method('spawn')
        except RuntimeError:
            logger.warning("Could not set multiprocessing start method to 'spawn'")
    
    # Create shared queue
    tag_queue = mp.Queue()
    
    # Create processes
    plc_process = mp.Process(target=read_plc_tag, args=(tag_queue,))
    db_process = mp.Process(target=insert_into_postgres, args=(tag_queue,))
    
    # Set up signal handler for main process
    def signal_handler(sig, frame):
        global exit_flag
        logger.info(f"Main process received signal {sig}")
        exit_flag = True
        
        # Give processes time to shut down gracefully
        logger.info("Waiting for processes to terminate...")
        time.sleep(5)
        
        # Terminate processes if they're still running
        if plc_process.is_alive():
            logger.warning("Terminating PLC process")
            plc_process.terminate()
        
        if db_process.is_alive():
            logger.warning("Terminating DB process")
            db_process.terminate()
            
        logger.info("Service shutdown complete")
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        global exit_flag
        exit_flag = False
        
        # Start processes
        plc_process.start()
        logger.info("Started PLC reader process")
        
        db_process.start()
        logger.info("Started database insertion process")
        
        # Monitor processes and keep main thread alive
        while not exit_flag and plc_process.is_alive() and db_process.is_alive():
            time.sleep(10)
        
        # Check if processes died unexpectedly
        if not exit_flag:
            if not plc_process.is_alive():
                logger.error("PLC process died unexpectedly")
            if not db_process.is_alive():
                logger.error("DB process died unexpectedly")
            exit_flag = True
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        exit_flag = True
    finally:
        # Clean up
        if plc_process.is_alive():
            plc_process.terminate()
        if db_process.is_alive():
            db_process.terminate()
        logger.info("loop4_sku service shutting down")


if __name__ == '__main__':
    main()
