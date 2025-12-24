import sys
import psycopg2
import pandas as pd
import numpy as np
from scipy.signal import savgol_filter, find_peaks
from datetime import datetime, timedelta
import time

import warnings
import argparse
import json
import logging
#from s.handlers import RotatingFileHandler
from logging.handlers import RotatingFileHandler
import threading
from pathlib import Path
warnings.filterwarnings('ignore')

# ===============================================================================
# CONFIGURATION AND LOGGING SETUP
# ===============================================================================
def load_db_config():
    """Load database configuration from config.json."""
    try:
        config_path = Path(__file__).parent / 'config.json'
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config.get('database', {})
    except FileNotFoundError:
        logging.error("config.json not found. Creating default configuration...")
        default_config = {
            "database": {
                "dbname": "short_data_hul",
                "user": "postgres",
                "password": "ai4m2024",
                "host": "localhost",
                "port": "5432"
            }
        }
        config_path = Path(__file__).parent / 'config.json'
        with open(config_path, 'w') as f:
            json.dump(default_config, f, indent=4)
        return default_config['database']
    except Exception as e:
        logging.error(f"Error loading config.json: {e}")
        return None

def setup_logger(machine_id=None):
    """Setup logging configuration with rotation."""
    log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)

    if machine_id:
        log_file = log_dir / f'jamming_{machine_id.lower()}.log'
        logger_name = f'jamming.{machine_id}'
    else:
        log_file = log_dir / 'jamming_unified.log'
        logger_name = 'jamming.main'

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # File handler with rotation (10MB max, keep 5 backup files)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Global database configuration
DB_CONFIG = load_db_config()

# ===============================================================================
# DATABASE SETUP AND CONFIGURATION
# ===============================================================================
def setup_configuration_table():
    """Create machine configuration table if it doesn't exist."""
    logger = logging.getLogger('jamming.main')
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS machine_jamming_config (
            machine_id VARCHAR(10) PRIMARY KEY,
            source_table VARCHAR(50) NOT NULL,
            alert_table VARCHAR(50) NOT NULL,
            current_column VARCHAR(50) NOT NULL,
            cam_column VARCHAR(50) NOT NULL,
            cycle_column VARCHAR(50) NOT NULL,
            status_column VARCHAR(50) NOT NULL,
            current_scale_factor FLOAT DEFAULT 10.0,
            cam_scale_factor FLOAT DEFAULT 1.0,
            auto_detect_cam_scale BOOLEAN DEFAULT TRUE,
            cam_min_range FLOAT DEFAULT 50.0,
            cam_max_range FLOAT DEFAULT 250.0,
            threshold_baseline_diff FLOAT DEFAULT 1.0,
            threshold_rate_of_change FLOAT DEFAULT 1.0,
            threshold_absolute_max FLOAT DEFAULT 2.0,
            baseline_cycles INTEGER DEFAULT 600,
            current_cycles INTEGER DEFAULT 7,
            fetch_limit INTEGER DEFAULT 1000,
            baseline_fetch_limit INTEGER DEFAULT 10000,
            update_interval FLOAT DEFAULT 1.0,
            alert_cooldown FLOAT DEFAULT 1.5,
            restart_wait_time INTEGER DEFAULT 30,
            savgol_window_length INTEGER DEFAULT 11,
            savgol_polyorder INTEGER DEFAULT 2,
            enable_peak_detection BOOLEAN DEFAULT FALSE,
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        conn.commit()
        logger.info("Configuration table created/verified successfully!")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error setting up configuration table: {e}", exc_info=True)
        return False

def insert_default_configurations():
    """Insert default configurations for all machines."""
    logger = logging.getLogger('jamming.main')
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        configs = [
            # MC17
            ('MC17', 'mc17_short_data', 'jamming_data', 'pulling_servo_current',
             'cam_position', 'spare1', 'status', 10.0, 1.0, False, 50, 250, 1.0, 1.0, 3.8, 600, 7),

            # MC18
            ('MC18', 'mc18_short_data', 'jamming_data_18', 'web_puller_current',
             'cam_position', 'spare1', 'status', 10.0, 1.0, False, 50, 250, 1.0, 1.0, 3.8, 600, 7),

            # MC19
            ('MC19', 'mc19_short_data', 'jamming_data_19', 'web_puller_current',
             'cam_position', 'spare1', 'status', 10.0, 1.0, False, 50, 250, 1.0, 1.0, 2.0, 600, 7),

            # MC20
            ('MC20', 'mc20_short_data', 'jamming_data_20', 'web_puller_current',
             'cam_position', 'spare1', 'status', 10.0, 1.0, False, 50, 250, 1.0, 1.0, 2.0, 600, 7),

            # MC21
            ('MC21', 'mc21_short_data', 'jamming_data_21', 'web_puller_current',
             'cam_position', 'spare1', 'status', 10.0, 1.0, False, 50, 250, 1.0, 1.0, 2.0, 600, 7),

            # MC22
            ('MC22', 'mc22_short_data', 'jamming_data_22', 'web_puller_current',
             'cam_position', 'spare1', 'status', 10.0, 1.0, False, 50, 250, 1.0, 1.0, 2.0, 600, 7),

            # MC25
            ('MC25', 'mc25_short_data', 'jamming_data_25', 'puller_tq',
             'cam_position', 'cycle_id', 'status', 1.0, 1.0, True, 130, 250, 1.0, 1.0, 2.0, 60, 7),

            # MC26
            ('MC26', 'mc26_short_data', 'jamming_data_26', 'puller_tq',
             'cam_position', 'cycle_id', 'status', 1.0, 1.0, True, 130, 250, 1.0, 1.0, 2.0, 60, 7),

            # MC27
            ('MC27', 'mc27_short_data', 'jamming_data_27', 'puller_motor_current_value',
             'cam_position', 'cycle_id', 'status', 10.0, 1.0, False, 50, 250, 1.0, 1.0, 3.8, 600, 7),

            # MC30
            ('MC30', 'mc30_short_data', 'jamming_data_30', 'puller_motor_current_value',
             'cam_position', 'cycle_id', 'status', 10.0, 1.0, False, 50, 250, 1.0, 1.0, 3.8, 600, 7),
        ]

        inserted_count = 0
        for config in configs:
            cursor.execute("""
            INSERT INTO machine_jamming_config
            (machine_id, source_table, alert_table, current_column, cam_column, cycle_column,
             status_column, current_scale_factor, cam_scale_factor, auto_detect_cam_scale,
             cam_min_range, cam_max_range, threshold_baseline_diff, threshold_rate_of_change,
             threshold_absolute_max, baseline_cycles, current_cycles)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (machine_id) DO NOTHING;
            """, config)
            if cursor.rowcount > 0:
                inserted_count += 1

        conn.commit()
        logger.info(f"Default configurations processed for {len(configs)} machines ({inserted_count} new)")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error inserting default configurations: {e}", exc_info=True)
        return False

def load_machine_config(machine_id):
    """Load configuration for a specific machine."""
    logger = logging.getLogger(f'jamming.{machine_id}')
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
        SELECT * FROM machine_jamming_config
        WHERE machine_id = %s AND active = TRUE;
        """, (machine_id,))

        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()

        cursor.close()
        conn.close()

        if row:
            return dict(zip(columns, row))
        else:
            logger.warning(f"No active configuration found for machine {machine_id}")
            return None
    except Exception as e:
        logger.error(f"Error loading machine configuration: {e}", exc_info=True)
        return None

def create_alert_table(table_name, logger):
    """Create alert table for a machine if it doesn't exist."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            average_current_value FLOAT,
            baseline_average_value FLOAT,
            rate_of_change FLOAT,
            reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Alert table {table_name} created/verified successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating alert table {table_name}: {e}", exc_info=True)
        return False

# ===============================================================================
# DATA FETCHING AND PROCESSING
# ===============================================================================
def fetch_latest_data(config, limit, logger):
    """Fetch the latest data from the database based on machine configuration."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = f"""
        SELECT {config['current_column']}/{config['current_scale_factor']} as current_value,
               {config['cam_column']} as cam_raw,
               {config['cycle_column']} as cycle_id,
               timestamp,
               {config['status_column']} as status
        FROM {config['source_table']}
        ORDER BY timestamp DESC LIMIT {limit};
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        columns = ['current_value', 'cam_raw', 'cycle_id', 'timestamp', 'status']
        df = pd.DataFrame(rows, columns=columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Handle cam position scaling
        if config['auto_detect_cam_scale']:
            mx = df['cam_raw'].max()
            if mx <= 360:
                scale_factor = 1.0
            elif mx <= 360_000:
                scale_factor = 1_000.0
            else:
                scale_factor = 100_000.0
            df['cam_position'] = df['cam_raw'] / scale_factor
        else:
            df['cam_position'] = df['cam_raw'] / config['cam_scale_factor']

        # Handle cycle_id if it's based on cam position resets (for MC27/MC30)
        if config['machine_id'] in ['MC27', 'MC30']:
            position_diff = df['cam_position'].diff()
            cycle_change = position_diff < -180
            df['cycle_id'] = cycle_change.cumsum()

        return df.sort_values('timestamp')
    except Exception as e:
        logger.error(f"Error fetching data: {e}", exc_info=True)
        return pd.DataFrame()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def log_alert_to_database(config, timestamp, avg_current, baseline_avg, rate_of_change, reason, logger):
    """Log alert information to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_query = f"""
        INSERT INTO {config['alert_table']}
        (timestamp, average_current_value, baseline_average_value, rate_of_change, reason)
        VALUES (%s, %s, %s, %s, %s);
        """

        cursor.execute(insert_query, (
            timestamp,
            float(avg_current),
            float(baseline_avg),
            float(rate_of_change),
            reason
        ))
        conn.commit()
        logger.info(f"Alert logged to database - Machine: {config['machine_id']}, "
                   f"Time: {timestamp}, Current: {avg_current:.4f}A, "
                   f"Baseline: {baseline_avg:.4f}A, ROC: {rate_of_change:.4f}A/s, "
                   f"Reason: {reason}")
        return True
    except Exception as e:
        logger.error(f"Database insertion failed: {e}", exc_info=True)
        if 'conn' in locals():
            conn.rollback()
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def log_alert(config, timestamp, avg_current, baseline_avg, rate_of_change, reason, logger):
    """Log alert information to the console and database."""
    logger.warning(f"ALERT DETECTED [{config['machine_id']}]: {reason}")
    logger.info(f"Timestamp: {timestamp}, Avg Current: {avg_current:.4f}A, "
               f"Baseline: {baseline_avg:.4f}A, ROC: {rate_of_change:.4f}A/s")

    success = log_alert_to_database(config, timestamp, avg_current, baseline_avg, rate_of_change, reason, logger)
    if not success:
        logger.error("Failed to log alert to database")

def calculate_average_and_roc(config, df):
    """Calculate the average current value and rate of change."""
    mask = (df['cam_position'] >= config['cam_min_range']) & (df['cam_position'] <= config['cam_max_range'])
    range_data = df[mask]
    current_avg = range_data['current_value'].mean() if not range_data.empty else 0.0
    one_sec_ago = df['timestamp'].max() - timedelta(seconds=1)
    past_data = range_data[range_data['timestamp'] < one_sec_ago]
    past_avg = past_data['current_value'].mean() if not past_data.empty else current_avg
    rate_of_change = (current_avg - past_avg) / 4  # per second
    return current_avg, rate_of_change

def calculate_baseline(config, df):
    """Calculate the baseline for current values based on cam position."""
    mask = (df['cam_position'] >= config['cam_min_range']) & (df['cam_position'] <= config['cam_max_range'])
    range_data = df[mask]
    baseline = range_data.groupby('cam_position')['current_value'].mean()

    # Apply Savitzky-Golay filter to baseline if enabled
    if config['enable_peak_detection'] and not baseline.empty:
        baseline_values = baseline.values
        baseline_values = adaptive_savgol_filter(
            baseline_values,
            config['savgol_window_length'],
            config['savgol_polyorder']
        )
        baseline = pd.Series(baseline_values, index=baseline.index)

    return baseline, range_data

def adaptive_savgol_filter(data, window_length, polyorder):
    """Apply a Savitzky-Golay filter with adaptive parameters for small datasets."""
    n_points = len(data)
    if n_points < window_length:
        window_length = n_points if n_points % 2 != 0 else n_points - 1
    if window_length < polyorder + 1:
        polyorder = window_length - 1 if window_length > 1 else 0

    if window_length > 2 and polyorder >= 0:
        return savgol_filter(data, window_length, polyorder)
    else:
        return data

def find_baseline_peaks(baseline):
    """Find peaks in the baseline data."""
    peaks, _ = find_peaks(baseline.values, height=0, distance=5)
    return baseline.index[peaks], baseline.values[peaks]

# ===============================================================================
# MAIN MONITORING LOOP
# ===============================================================================
def monitor_machine(machine_id):
    """Main monitoring function for a specific machine."""
    logger = setup_logger(machine_id)

    try:
        logger.info(f"Starting monitoring for {machine_id}...")
        config = load_machine_config(machine_id)

        if not config:
            logger.error(f"Failed to load configuration for {machine_id}")
            return

        # Create alert table if it doesn't exist
        create_alert_table(config['alert_table'], logger)

        logger.info(f"{machine_id} Monitoring System Started")
        logger.info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
        logger.info(f"Source Table: {config['source_table']}")
        logger.info(f"Alert Table: {config['alert_table']}")
        logger.info(f"Update interval: {config['update_interval']}s")
        logger.info(f"Thresholds - Baseline Diff: {config['threshold_baseline_diff']}A, "
                   f"ROC: {config['threshold_rate_of_change']}A/s, "
                   f"Absolute Max: {config['threshold_absolute_max']}A")
        logger.info(f"Cam Range: {config['cam_min_range']}-{config['cam_max_range']}")

        previous_status = None
        status_logged = False
        waiting_for_restart = False
        last_alert_time = None
        alert_cooldown = timedelta(seconds=config['alert_cooldown'])

        consecutive_errors = 0
        max_consecutive_errors = 10

        while True:
            try:
                display_data = fetch_latest_data(config, config['fetch_limit'], logger)
                if display_data.empty:
                    logger.warning("No data received from database")
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"Too many consecutive errors ({consecutive_errors}), stopping monitoring")
                        break
                    time.sleep(config['update_interval'])
                    continue

                # Reset error counter on successful data fetch
                consecutive_errors = 0

                latest_timestamp = display_data['timestamp'].iloc[-1]
                current_status = display_data['status'].iloc[-1]

                if previous_status is None:
                    previous_status = current_status

                # Handle machine status changes
                if current_status != previous_status:
                    if current_status != 1 and not status_logged:
                        log_alert(config, latest_timestamp, 0, 0, 0, "Machine Stopped", logger)
                        status_logged = True
                        waiting_for_restart = False
                    elif current_status == 1 and not waiting_for_restart:
                        waiting_for_restart = True
                        logger.info(f"Machine started - waiting {config['restart_wait_time']} seconds before resuming analysis...")
                        time.sleep(config['restart_wait_time'])
                        if not status_logged:
                            log_alert(config, latest_timestamp, 0, 0, 0, "Machine Started", logger)
                            status_logged = True
                        waiting_for_restart = False

                if current_status != previous_status:
                    status_logged = False

                previous_status = current_status

                # Only proceed with analysis if machine is running
                if current_status == 1 and not waiting_for_restart:
                    baseline_data = fetch_latest_data(config, config['baseline_fetch_limit'], logger)
                    if not baseline_data.empty:
                        last_n_cycles = baseline_data['cycle_id'].unique()[-config['baseline_cycles']:]
                        baseline_df = baseline_data[baseline_data['cycle_id'].isin(last_n_cycles)]
                        baseline, baseline_points = calculate_baseline(config, baseline_df)

                        # Find peaks in baseline if enabled
                        if config['enable_peak_detection'] and not baseline.empty:
                            peak_positions, peak_values = find_baseline_peaks(baseline)
                            if len(peak_positions) > 0:
                                logger.debug(f"Baseline peaks detected at positions: {peak_positions.tolist()}")

                        # Calculate metrics
                        last_current_cycles = display_data['cycle_id'].unique()[-config['current_cycles']:]
                        filtered_df = display_data[display_data['cycle_id'].isin(last_current_cycles)]
                        current_avg, rate_of_change = calculate_average_and_roc(config, filtered_df)
                        baseline_avg = baseline.mean() if not baseline.empty else 0.0
                        avg_difference = current_avg - baseline_avg

                        # Log stats
                        logger.debug(f"[{machine_id}] Time: {latest_timestamp}, "
                                   f"Current: {current_avg:.4f}A, ROC: {rate_of_change:.4f}A/s, "
                                   f"Baseline: {baseline_avg:.4f}A, Diff: {avg_difference:.4f}A")

                        # Alert logic
                        alert_condition_1 = current_avg > baseline_avg + config['threshold_baseline_diff']
                        alert_condition_2 = rate_of_change > config['threshold_rate_of_change']
                        alert_condition_3 = current_avg > config['threshold_absolute_max']
                        alert_condition = alert_condition_1 or alert_condition_2 or alert_condition_3

                        current_time = datetime.now()
                        should_log_alert = True
                        if last_alert_time is not None:
                            elapsed_time = current_time - last_alert_time
                            if elapsed_time < alert_cooldown:
                                should_log_alert = False

                        if alert_condition and should_log_alert:
                            reasons = []
                            if alert_condition_1:
                                reasons.append(f"Current avg > Baseline avg + {config['threshold_baseline_diff']}A")
                            if alert_condition_2:
                                reasons.append(f"Rate of change > {config['threshold_rate_of_change']}A/s")
                            if alert_condition_3:
                                reasons.append(f"Current avg > {config['threshold_absolute_max']}A")
                            reason_str = " & ".join(reasons)
                            log_alert(config, latest_timestamp, current_avg, baseline_avg, rate_of_change, reason_str, logger)
                            last_alert_time = current_time
                    else:
                        logger.warning("No baseline data available")
                else:
                    logger.debug(f"[{machine_id}] Machine stopped or waiting to resume analysis")

                time.sleep(config['update_interval'])

            except KeyboardInterrupt:
                logger.info(f"[{machine_id}] Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}", exc_info=True)
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive errors ({consecutive_errors}), stopping monitoring")
                    break
                time.sleep(config['update_interval'])
                continue

    except Exception as e:
        logger.critical(f"Critical error in monitor_machine for {machine_id}: {e}", exc_info=True)
    finally:
        logger.info(f"Monitoring stopped for {machine_id}")

def monitor_all_machines():
    """Monitor all active machines in separate threads."""
    main_logger = setup_logger()
    main_logger.info("Starting monitoring for all active machines...")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT machine_id FROM machine_jamming_config WHERE active = TRUE ORDER BY machine_id;")
        machines = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        if not machines:
            main_logger.warning("No active machines found in configuration")
            return

        main_logger.info(f"Found {len(machines)} active machines: {', '.join(machines)}")

        threads = []
        for machine_id in machines:
            thread = threading.Thread(
                target=monitor_machine,
                args=(machine_id,),
                name=f"Monitor-{machine_id}",
                daemon=False
            )
            thread.start()
            threads.append(thread)
            main_logger.info(f"Started monitoring thread for {machine_id}")
            time.sleep(1)  # Small delay between thread starts

        # Wait for all threads
        for thread in threads:
            thread.join()

    except KeyboardInterrupt:
        main_logger.info("Monitoring stopped by user (Ctrl+C)")
    except Exception as e:
        main_logger.error(f"Error in monitor_all_machines: {e}", exc_info=True)
    finally:
        main_logger.info("All monitoring threads stopped")

# ===============================================================================
# COMMAND LINE INTERFACE
# ===============================================================================
def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Unified Jamming Detection System')
    parser.add_argument('--setup', action='store_true', help='Setup configuration table and insert defaults')
    parser.add_argument('--machine', type=str, help='Machine ID to monitor (e.g., MC17, MC18)')
    parser.add_argument('--all', action='store_true', help='Monitor all active machines')
    parser.add_argument('--list', action='store_true', help='List all configured machines')

    args = parser.parse_args()

    main_logger = setup_logger()

    if not DB_CONFIG:
        main_logger.error("Failed to load database configuration. Exiting.")
        return

    if args.setup:
        main_logger.info("Setting up configuration table...")
        if setup_configuration_table():
            main_logger.info("Inserting default configurations...")
            insert_default_configurations()
        return

    if args.list:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("SELECT machine_id, source_table, active FROM machine_jamming_config ORDER BY machine_id;")
            machines = cursor.fetchall()
            print("\nConfigured Machines:")
            print("-" * 50)
            for machine_id, source_table, active in machines:
                status = "ACTIVE" if active else "INACTIVE"
                print(f"{machine_id:8} | {source_table:25} | {status}")
            print("-" * 50)
            cursor.close()
            conn.close()
        except Exception as e:
            main_logger.error(f"Error listing machines: {e}", exc_info=True)
        return

    if args.all:
        monitor_all_machines()
    elif args.machine:
        monitor_machine(args.machine.upper())
    else:
        parser.print_help()
        print("\nExample usage:")
        print("  python jamming_unified.py --setup              # Setup database tables")
        print("  python jamming_unified.py --list               # List all machines")
        print("  python jamming_unified.py --machine MC17       # Monitor MC17")
        print("  python jamming_unified.py --all                # Monitor all active machines")

if __name__ == "__main__":
    main()
