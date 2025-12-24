import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import time
import csv
import os
from sqlalchemy import create_engine



def get_latest_3min_data(host, dbname, user, password, table='your_table', port=5432):
    try:
        # Create SQLAlchemy engine
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

        query = f"""
            SELECT timestamp, hopper_1_level, status, status_code
            FROM {table}
            WHERE timestamp >= NOW() - INTERVAL '6 minutes'
            ORDER BY timestamp DESC;
        """

        # Use engine instead of raw connection
        df = pd.read_sql_query(query, engine)
        return df

    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def check_status(df):

    return (df['status'].eq(1).all()) and (df['status_code'].eq(0).all())



def get_minima(df, level_threshold=70.0, lookback_seconds=10):
    
    if 'timestamp' not in df or 'hopper_1_level' not in df:
        raise ValueError("DataFrame must contain 'timestamp' and 'hopper_1_level' columns")

   
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])

   
    latest_timestamp = df[df['hopper_1_level'] < level_threshold]['timestamp'].max()

    if pd.isna(latest_timestamp):
        print("No timestamp found where hopper_1_level < threshold")
        return None, None

  
    start_time = latest_timestamp - pd.Timedelta(seconds=lookback_seconds)

  
    window_df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= latest_timestamp)]

    if window_df.empty:
        print("No data in the 50-second window before the latest low-level timestamp")
        return None, None

    
    min_row = window_df.loc[window_df['hopper_1_level'].idxmin()]

    print(f"endpoint of hopper level is at {min_row['timestamp']} and  level is {min_row['hopper_1_level']}")
    return min_row['hopper_1_level'], min_row['timestamp']




# min_level, min_time = get_minima(df)
# print(f"Minimum level: {min_level} at {min_time}")


def find_previous_minima(df,  reference_timestamp, level_threshold=70.0, lookback_seconds=10):
   
    if 'timestamp' not in df or 'hopper_1_level' not in df:
        raise ValueError("DataFrame must have 'timestamp' and 'hopper_1_level' columns")

    # Ensure datetime type
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Ensure reference timestamp is datetime
    reference_timestamp = pd.to_datetime(reference_timestamp)

    # Step 1: Get latest timestamp before the reference
    df = df[df['timestamp'] < reference_timestamp]
    if df.empty:
        print("No data before the reference timestamp")
        return None, None

    
    if 'timestamp' not in df or 'hopper_1_level' not in df:
        raise ValueError("DataFrame must contain 'timestamp' and 'hopper_1_level' columns")

   
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])

   
    latest_timestamp = df[df['hopper_1_level'] < level_threshold]['timestamp'].max()

    if pd.isna(latest_timestamp):
        print("No timestamp found where hopper_1_level < threshold")
        return None, None

  
    start_time = latest_timestamp - pd.Timedelta(seconds=lookback_seconds)

  
    window_df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= latest_timestamp)]

    if window_df.empty:
        print("No data in the 50-second window before the latest low-level timestamp")
        return None, None

    
    min_row = window_df.loc[window_df['hopper_1_level'].idxmin()]

    print(f"Start of hopper level is at {min_row['timestamp']} and  level is {min_row['hopper_1_level']}")
    return min_row['hopper_1_level'], min_row['timestamp']

# ref_time = '2025-05-12 12:00:00'
# min_level, min_time = find_previous_minima(df, ref_time)
# print(f"Min level: {min_level}, at: {min_time}")


def find_max_between_minima(df, minima_1, minima_2):
   
    # Ensure datetime types
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    minima_1 = pd.to_datetime(minima_1)
    minima_2 = pd.to_datetime(minima_2)

    # Ensure minima_1 < minima_2
    start_time = min(minima_1, minima_2)
    end_time = max(minima_1, minima_2)

    # Filter between timestamps
    mask = (df['timestamp'] > start_time) & (df['timestamp'] < end_time)
    window_df = df[mask]

    if window_df.empty:
        print("No data between the two minima timestamps.")
        return None, None

    # Find row with max hopper_1_level
    max_row = window_df.loc[window_df['hopper_1_level'].idxmax()]

    print(f"Maxima of hopper level is at {max_row['timestamp']} and  level is {max_row['hopper_1_level']}")

    return max_row['timestamp'], max_row['hopper_1_level']

# max_ts, max_val = find_max_between_minima(df, '2025-05-12 11:59:00', '2025-05-12 12:03:00')
# print(f"Max level: {max_val} at {max_ts}")


def calculate_slope_and_area(max_level, max_time, min_level, min_time):
    
    # Ensure timestamps are datetime
    max_time = pd.to_datetime(max_time)
    min_time = pd.to_datetime(min_time)

    # Calculate time difference in seconds
    time_diff = abs((max_time - min_time).total_seconds())

    if time_diff == 0:
        raise ValueError("Timestamps are equal — cannot compute slope.")

    # Height = level difference
    level_diff = max_level - min_level

    print(f"Level difference is {level_diff} and rise time is {time_diff}")
    # Slope = rise / run
    slope = level_diff / time_diff

    # Area of triangle = 0.5 * base * height
    area = 0.5 * time_diff * level_diff

    return slope, area, level_diff


def log_metrics(slope, area, maxima, level_diff, filename):
    
    # Prepare the full path to current directory
    file_path = os.path.join(os.getcwd(), filename)

    # Write header only if file is being created
    file_exists = os.path.exists(file_path)
    
    with open(file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(['timestamp', 'slope', 'area', 'maxima', 'level_filled'])
        writer.writerow([datetime.now(), slope, area, maxima, level_diff])

    print(f"Logged to {file_path}")



def get_hopper_level_params(host, dbname, user, password, table, port):
    try:
        data = get_latest_3min_data(host, dbname, user, password, table, port)

        if check_status(data):
            latest_min, latest_min_time = get_minima(data, level_threshold=70.0, lookback_seconds=10)

            ref_time = latest_min_time - pd.Timedelta(seconds=20)

            prev_min, prev_min_time = find_previous_minima(data,ref_time, level_threshold=70.0, lookback_seconds=10)

            max_ts, max_val = find_max_between_minima(data, prev_min_time, latest_min_time)

            slope, area, level_diff = calculate_slope_and_area(max_val, max_ts, prev_min, prev_min_time)

            print(f"Slope: {slope}, Area: {area}")

            log_metrics(slope, area, max_val, level_diff, 'hopper_level_cycle_params.csv')

            return slope, area, level_diff
        else:
            print('Status not ready. Waiting before next check...')
            return None, None, None

    except Exception as e:
        print(f"Error in calculating params: {e}")
        return None, None, None



if __name__ == "__main__":


    host = "localhost"
    dbname = "hul"
    user = "postgres"
    password = "ai4m2024"
    table = "mc17"
    port = 5432

    while True:
        slope, area, level_diff = get_hopper_level_params(host, dbname, user, password, table, port)

        if slope is not None and area is not None:
            print(f"Logged result: Slope = {slope:.4f}, Area = {area:.4f}")
        else:
            print("No valid result this cycle.")

        time.sleep(80)  # Wait before next cycle


