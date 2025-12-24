import sys
import psycopg2
import pandas as pd
import numpy as np
from scipy.signal import savgol_filter, find_peaks
from datetime import datetime, timedelta
import time

conn_params = {
    'dbname': 'short_data_hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432'
}

def test_db_connection():
    """Test the database connection and verify the jamming_data table exists."""
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Check if jamming_data table exists
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'jamming_data'
        );
        """)
        
        table_exists = cursor.fetchone()[0]
        if not table_exists:
            print("WARNING: The 'jamming_data' table does not exist!")
            print("Creating the table...")
            
            # Create the table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS jamming_data (
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
            print("The 'jamming_data' table exists.")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Database connection error: {e}")
        return False

def fetch_latest_data(limit=300):
    """Fetch the latest data from the database."""
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        query = f"""
        SELECT web_puller_current/10.0 as web_puller_current,
        cam_position,
        spare1,
        timestamp,
        status
        FROM mc18_short_data
        ORDER BY timestamp DESC LIMIT {limit};
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = ['web_puller_current', 'cam_position', 'spare1', 'timestamp', 'machine_status']
        df = pd.DataFrame(rows, columns=columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df.sort_values('timestamp')
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame(columns=['web_puller_current', 'cam_position', 'spare1', 'timestamp', 'machine_status'])
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def log_alert_to_database(timestamp, avg_current, baseline_avg, rate_of_change, reason):
    """Log alert information to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO jamming_data_18
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
        print(f"  - Table: jamming_data_18")
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
    """Calculate the average current value and rate of change."""
    mask = (df['cam_position'] >= 50) & (df['cam_position'] <= 250)
    range_data = df[mask]
    current_avg = range_data['web_puller_current'].mean()
    one_sec_ago = df['timestamp'].max() - timedelta(seconds=1)
    past_data = range_data[range_data['timestamp'] < one_sec_ago]
    past_avg = past_data['web_puller_current'].mean() if not past_data.empty else current_avg
    rate_of_change = (current_avg - past_avg) / 4  # per second
    return current_avg, rate_of_change

def calculate_baseline(df):
    """Calculate the baseline for current values based on cam position."""
    mask = (df['cam_position'] >= 50) & (df['cam_position'] <= 250)
    range_data = df[mask]
    baseline = range_data.groupby('cam_position')['web_puller_current'].mean()
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

def main():
    print("Testing database connection...")
    if not test_db_connection():
        print("Database connection failed. Please check your connection parameters.")
        return

    previous_machine_status = None
    status_logged = False
    waiting_for_restart = False
    last_alert_time = None
    alert_cooldown = timedelta(seconds=1.5)

    while True:
        display_data = fetch_latest_data(limit=1000)
        if display_data.empty:
            print("No data received from database")
            time.sleep(1)
            continue

        latest_timestamp = display_data['timestamp'].iloc[-1]
        current_machine_status = display_data['machine_status'].iloc[-1]

        if previous_machine_status is None:
            previous_machine_status = current_machine_status

        # Handle machine status changes
        if current_machine_status != previous_machine_status:
            if current_machine_status != 1 and not status_logged:
                log_alert(latest_timestamp, 0, 0, 0, "Machine Stopped")
                status_logged = True
                waiting_for_restart = False
            elif current_machine_status == 1 and not waiting_for_restart:
                waiting_for_restart = True
                print("Machine started - waiting 2 minutes before resuming analysis...")
                time.sleep(30)
                if not status_logged:
                    log_alert(latest_timestamp, 0, 0, 0, "Machine Started")
                    status_logged = True
                waiting_for_restart = False

        if current_machine_status != previous_machine_status:
            status_logged = False

        previous_machine_status = current_machine_status

        # Only proceed with analysis if machine is running
        if current_machine_status == 1 and not waiting_for_restart:
            baseline_data = fetch_latest_data(limit=10000)
            if not baseline_data.empty:
                last_600_cycles = baseline_data['spare1'].unique()[-600:]
                baseline_df = baseline_data[baseline_data['spare1'].isin(last_600_cycles)]
                baseline, baseline_points = calculate_baseline(baseline_df)

                # Calculate metrics
                last_5_cycles = display_data['spare1'].unique()[-7:]
                filtered_df = display_data[display_data['spare1'].isin(last_5_cycles)]
                current_avg, rate_of_change = calculate_average_and_roc(filtered_df)
                baseline_avg = baseline.mean()
                avg_difference = current_avg - baseline_avg

                # Print stats
                print(f"Latest Timestamp: {latest_timestamp}")
                print(f"Current Average (50-250): {current_avg:.4f} A")
                print(f"Rate of Change (1s): {rate_of_change:.4f} A/s")
                print(f"Baseline Average: {baseline_avg:.4f} A")
                print(f"Avg Difference (Current - Baseline): {avg_difference:.4f} A")

                # Alert logic
                alert_condition_1 = current_avg > baseline_avg + 1
                alert_condition_2 = rate_of_change > 1
                alert_condition_3 = current_avg > 3.8
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
                        reasons.append("Current avg > Baseline avg + 1A")
                    if alert_condition_2:
                        reasons.append("Rate of change > 1A/s")
                    if alert_condition_3:
                        reasons.append("Current avg > 2A")
                    reason_str = " & ".join(reasons)
                    log_alert(latest_timestamp, current_avg, baseline_avg, rate_of_change, reason_str)
                    last_alert_time = current_time

            else:
                print("No baseline data available")
        else:
            print("Machine Stopped or waiting to resume analysis.")

        time.sleep(1)

if __name__ == "__main__":
    main()

