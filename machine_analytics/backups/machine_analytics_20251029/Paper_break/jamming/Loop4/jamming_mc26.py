import sys
import psycopg2
import pandas as pd
import numpy as np
from scipy.signal import savgol_filter, find_peaks
from datetime import datetime, timedelta
import time
import warnings
warnings.filterwarnings('ignore')

DB_CONFIG = {
    "dbname": "short_data_hul",
    "user": "postgres",
    "password": "ai4m2024",
    "host": "localhost",
    "port": "5432"
}

# MONITORING SETTINGS
FETCH_LIMIT = 60              # Number of records to fetch for analysis
BASELINE_CYCLES = 60          # Number of cycles for baseline calculation
CURRENT_CYCLES = 7             # Number of recent cycles for current data
UPDATE_INTERVAL = 1            # Update interval in seconds

# ============================================================================
# ALERT THRESHOLDS - MODIFY THESE VALUES TO CHANGE ALERT BEHAVIOR
# ============================================================================
THRESHOLD_BASELINE_DIFF = 1.0    # Alert when current exceeds baseline by this amount (Amps)
THRESHOLD_RATE_OF_CHANGE = 1.0   # Alert when rate of change exceeds this value (Amps/second)
THRESHOLD_ABSOLUTE_MAX = 2.0     # Alert when current exceeds this absolute value (Amps)
# ============================================================================

# SAVITZKY-GOLAY FILTER SETTINGS
SAVGOL_WINDOW_LENGTH = 11
SAVGOL_POLYORDER = 2

# ===============================================================================
# DATABASE AND DATA PROCESSING FUNCTIONS
# ===============================================================================
def test_db_connection():
    """Test the database connection and verify/create the jamming_data_26 table."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Check if jamming_data_26 table exists
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'jamming_data_26'
        );
        """)

        table_exists = cursor.fetchone()[0]
        if not table_exists:
            print("WARNING: The 'jamming_data_26' table does not exist!")
            print("Creating the table...")

            # Create the table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS jamming_data_26 (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP,
                average_current_value FLOAT,
                baseline_average_value FLOAT,
                rate_of_change FLOAT,
                reason TEXT
            );
            """)
            conn.commit()
            print("Table created successfully!")
        else:
            print("Connected to database successfully!")
            print("The 'jamming_data_26' table exists.")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Database connection error: {e}")
        return False

def fetch_latest_data(limit=600):
    """Fetch the latest data from the database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = f"""
        SELECT puller_tq,
               cam_position AS cam_raw,
               cycle_id,
               timestamp,
               status
        FROM mc26_short_data
        ORDER BY timestamp DESC LIMIT {limit};
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = ['puller_tq', 'cam_raw', 'cycle_id', 'timestamp', 'status']
        df = pd.DataFrame(rows, columns=columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Auto-detect scale factor
        scale_factor = None
        mx = df['cam_raw'].max()
        if mx <= 360:
            scale_factor = 1.0
        elif mx <= 360_000:
            scale_factor = 1_000.0
        else:
            scale_factor = 100_000.0
        df['cam_position'] = df['cam_raw'] / scale_factor

        return df.sort_values('timestamp')
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame(columns=['puller_tq', 'cam_raw', 'cycle_id', 'timestamp', 'status', 'cam_position'])
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def log_alert_to_database(timestamp, avg_current, baseline_avg, rate_of_change, reason):
    """Log alert information to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO jamming_data_26
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
        print(f"✓ SUCCESSFULLY INSERTED INTO DATABASE:")
        print(f"  - Timestamp: {timestamp}")
        print(f"  - Average Current: {avg_current:.4f}")
        print(f"  - Baseline Average: {baseline_avg:.4f}")
        print(f"  - Rate of Change: {rate_of_change:.4f}")
        print(f"  - Reason: {reason}")
        print(f"  - Table: jamming_data_26")
        return True
    except Exception as e:
        print(f"❌ DATABASE INSERTION FAILED: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def log_alert(timestamp, avg_current, baseline_avg, rate_of_change, reason):
    """Log alert information to the console and database."""
    print(f"----------------------------------")
    print(f"ALERT DETECTED: {reason}")
    print(f"Timestamp: {timestamp}")
    print(f"Average Current: {avg_current:.4f}")
    print(f"Baseline Average: {baseline_avg:.4f}")
    print(f"Rate of Change: {rate_of_change:.4f}")

    # Log to database and print insertion status
    success = log_alert_to_database(timestamp, avg_current, baseline_avg, rate_of_change, reason)
    if not success:
        print("❌ Failed to log alert to database")
    print(f"----------------------------------")

def calculate_average_and_roc(df):
    """Calculate the average current value and rate of change for cam position range 130-250."""
    mask = (df['cam_position'] >= 130) & (df['cam_position'] <= 250)
    range_data = df[mask]
    current_avg = range_data['puller_tq'].mean() if not range_data.empty else 0.0
    one_sec_ago = df['timestamp'].max() - timedelta(seconds=1)
    past_data = range_data[range_data['timestamp'] < one_sec_ago]
    past_avg = past_data['puller_tq'].mean() if not past_data.empty else current_avg
    rate_of_change = (current_avg - past_avg)
    return current_avg, rate_of_change

def calculate_baseline(df):
    """Calculate the baseline for current values based on cam position."""
    mask = (df['cam_position'] >= 130) & (df['cam_position'] <= 250)
    range_data = df[mask]
    baseline = range_data.groupby('cam_position')['puller_tq'].mean()

    # Apply Savitzky-Golay filter to baseline
    if not baseline.empty:
        baseline_values = baseline.values
        baseline_values = adaptive_savgol_filter(baseline_values, SAVGOL_WINDOW_LENGTH, SAVGOL_POLYORDER)
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
def main():
    """Main entry point for MC26 monitoring."""
    print("Testing database connection...")
    if not test_db_connection():
        print("Database connection failed. Please check your connection parameters.")
        return

    print("\n" + "="*60)
    print("MC25/MC26 Monitoring System Started")
    print("="*60)
    print(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    print(f"Update interval: {UPDATE_INTERVAL}s")
    print(f"\nThresholds Configuration:")
    print(f"  - Baseline Difference: {THRESHOLD_BASELINE_DIFF} A")
    print(f"  - Rate of Change: {THRESHOLD_RATE_OF_CHANGE} A/s")
    print(f"  - Absolute Maximum: {THRESHOLD_ABSOLUTE_MAX} A")
    print("="*60)
    print("\nMonitoring in progress... Press Ctrl+C to stop.\n")

    previous_status = None
    status_logged = False
    waiting_for_restart = False
    last_alert_time = None
    alert_cooldown = timedelta(seconds=1.5)

    while True:
        try:
            display_data = fetch_latest_data(limit=FETCH_LIMIT)
            if display_data.empty:
                print("No data received from database")
                time.sleep(UPDATE_INTERVAL)
                continue

            latest_timestamp = display_data['timestamp'].iloc[-1]
            current_status = display_data['status'].iloc[-1]

            if previous_status is None:
                previous_status = current_status

            # Handle machine status changes
            if current_status != previous_status:
                if current_status != 1 and not status_logged:
                    log_alert(latest_timestamp, 0, 0, 0, "Machine Stopped")
                    status_logged = True
                    waiting_for_restart = False
                elif current_status == 1 and not waiting_for_restart:
                    waiting_for_restart = True
                    print("Machine started - waiting 30 seconds before resuming analysis...")
                    time.sleep(30)
                    if not status_logged:
                        log_alert(latest_timestamp, 0, 0, 0, "Machine Started")
                        status_logged = True
                    waiting_for_restart = False

            if current_status != previous_status:
                status_logged = False

            previous_status = current_status

            # Only proceed with analysis if machine is running
            if current_status == 1 and not waiting_for_restart:
                baseline_data = fetch_latest_data(limit=BASELINE_CYCLES * 10)
                if not baseline_data.empty:
                    last_60_cycles = baseline_data['cycle_id'].unique()[-BASELINE_CYCLES:]
                    baseline_df = baseline_data[baseline_data['cycle_id'].isin(last_60_cycles)]
                    baseline, baseline_points = calculate_baseline(baseline_df)

                    # Find peaks in baseline
                    peak_positions, peak_values = find_baseline_peaks(baseline)
                    if len(peak_positions) > 0:
                        print(f"Baseline Peaks Detected at positions: {peak_positions.tolist()}")
                        print(f"Peak Values: {peak_values.tolist()}")

                    # Calculate metrics
                    last_5_cycles = display_data['cycle_id'].unique()[-CURRENT_CYCLES:]
                    filtered_df = display_data[display_data['cycle_id'].isin(last_5_cycles)]
                    current_avg, rate_of_change = calculate_average_and_roc(filtered_df)
                    baseline_avg = baseline.mean() if not baseline.empty else 0.0
                    avg_difference = current_avg - baseline_avg

                    # Print stats
                    print(f"\nLatest Timestamp: {latest_timestamp}")
                    print(f"Current Average (130-250): {current_avg:.4f} A")
                    print(f"Rate of Change (1s): {rate_of_change:.4f} A/s")
                    print(f"Baseline Average: {baseline_avg:.4f} A")
                    print(f"Avg Difference (Current - Baseline): {avg_difference:.4f} A")

                    # Alert logic
                    alert_condition_1 = current_avg > baseline_avg + THRESHOLD_BASELINE_DIFF
                    alert_condition_2 = rate_of_change > THRESHOLD_RATE_OF_CHANGE
                    alert_condition_3 = current_avg > THRESHOLD_ABSOLUTE_MAX
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
                            reasons.append(f"Current avg > Baseline avg + {THRESHOLD_BASELINE_DIFF}A")
                        if alert_condition_2:
                            reasons.append(f"Rate of change > {THRESHOLD_RATE_OF_CHANGE}A/s")
                        if alert_condition_3:
                            reasons.append(f"Current avg > {THRESHOLD_ABSOLUTE_MAX}A")
                        reason_str = " & ".join(reasons)
                        log_alert(latest_timestamp, current_avg, baseline_avg, rate_of_change, reason_str)
                        last_alert_time = current_time
                else:
                    print("No baseline data available")
            else:
                print("Machine Stopped or waiting to resume analysis.")

            time.sleep(UPDATE_INTERVAL)
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user.")
            break
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(UPDATE_INTERVAL)
            continue

if __name__ == "__main__":
    main()
