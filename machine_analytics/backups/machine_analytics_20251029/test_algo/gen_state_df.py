from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import pytz
import json
import os

# ======================== Configuration ==========================

host = 'localhost'
dbname = 'hul'
user = 'postgres'
password = 'ai4m2024'
port = 5432
table = 'mc17'
timezone = 'Asia/Kolkata'
json_file_path = 'loop3.json'  # Path to your fault code JSON
output_dir = os.path.join(os.getcwd(), "data", "state_csvs")
os.makedirs(output_dir, exist_ok=True)

# ======================== JSON Loader ==========================

def get_json(json_file_path):
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict) and 'faults' in data:
            return pd.DataFrame(data['faults'])
        else:
            raise ValueError("Unexpected JSON format.")
    except Exception as e:
        print(f"Error in get_json: {e}")
        return pd.DataFrame()

# ======================== SQL Data Fetcher ==========================

def fetch_status_groups_for_interval(start_time, end_time):
    start_ts_str = start_time.isoformat()
    end_ts_str = end_time.isoformat()
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

    query = f"""
    WITH ranked AS (
        SELECT 
            timestamp,
            status,
            status_code,
            LAG(status) OVER (ORDER BY timestamp) AS prev_status,
            LAG(status_code) OVER (ORDER BY timestamp) AS prev_status_code
        FROM {table}
        WHERE timestamp BETWEEN '{start_ts_str}' AND '{end_ts_str}'
    ),
    changes AS (
        SELECT
            timestamp,
            status,
            status_code,
            CASE 
                WHEN status != prev_status OR status_code != prev_status_code 
                     OR prev_status IS NULL OR prev_status_code IS NULL
                THEN 1 ELSE 0 
            END AS is_change
        FROM ranked
    ),
    groups AS (
        SELECT
            timestamp,
            status,
            status_code,
            SUM(is_change) OVER (ORDER BY timestamp) AS group_id
        FROM changes
    )
    SELECT
        MIN(timestamp) AS time_start,
        MAX(timestamp) AS time_end,
        status,
        status_code
    FROM groups
    GROUP BY group_id, status, status_code
    ORDER BY time_start ASC;
    """
    try:
        df = pd.read_sql_query(query, engine)
        df['time_start'] = pd.to_datetime(df['time_start']) + timedelta(hours=5, minutes=30)
        df['time_end'] = pd.to_datetime(df['time_end']) + timedelta(hours=5, minutes=30)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()

# ======================== Data Mapping ==========================

def get_state_dataframe(df, codes):
    try:
        state_df = df.merge(codes[['fault_code', 'message']], left_on='status_code', right_on='fault_code', how='left')
        state_df = state_df.drop(columns=['fault_code'])
        state_df.loc[(state_df['status'] == 1) & (state_df['status_code'] == 0), 'message'] = 'Running'
        state_df['duration'] = (state_df['time_end'] - state_df['time_start']).dt.total_seconds().apply(
            lambda x: '{:02}:{:02}:{:02}'.format(int(x // 3600), int((x % 3600) // 60), int(x % 60))
        )
        state_df['time_start'] = pd.to_datetime(state_df['time_start']).dt.floor('min')
        state_df['time_end'] = pd.to_datetime(state_df['time_end']).dt.floor('min')
        return state_df
    except Exception as e:
        print(f"Error in get_state_dataframe: {e}")
        return pd.DataFrame()

# ======================== Main Loop ==========================

def main():
    codes_df = get_json(json_file_path)
    tz = pytz.timezone(timezone)

    current = tz.localize(datetime(2025, 5, 28, 23, 0))
    end = tz.localize(datetime(2025, 6, 10, 7, 0))

    while current < end:
        next_time = current + timedelta(hours=8)
        print(f"Processing interval: {current} to {next_time}")

        raw_df = fetch_status_groups_for_interval(current, next_time)
        if not raw_df.empty:
            state_df = get_state_dataframe(raw_df, codes_df)
            if not state_df.empty:
                filename_time = current.strftime("%Y_%m_%d_%H-%M")
                output_path = os.path.join(output_dir, f"{filename_time}.xlsx")
                state_df.to_excel(output_path, index=False)
                print(f"Saved: {output_path}")
            else:
                print(f"No valid state data for interval: {current} to {next_time}")
        else:
            print(f"No raw data for interval: {current} to {next_time}")

        current = next_time

# ======================== Entry Point ==========================

if __name__ == "__main__":
    main()

