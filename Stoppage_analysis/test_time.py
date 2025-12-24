#!/usr/bin/env python3

import os
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import pytz
import time

# Configuration
OUTPUT_FOLDER = "/home/ai4m/develop/Stoppage_analysis/raw_csv"
IST = pytz.timezone('Asia/Kolkata')  # Indian Standard Time (UTC+5:30)
MACHINE_TABLES = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

# Ensure output directory exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def get_db_connection():
    """Create a database connection"""
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

def get_current_db_time():
    """Get the current time from the database to ensure consistency"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT NOW() AT TIME ZONE 'Asia/Kolkata'")
        db_time = cursor.fetchone()[0]
        return db_time
    finally:
        cursor.close()
        conn.close()

def get_shift_info(date_obj):
    """Define shift timings for the given date"""
    shifts = {
        'A': {
            'start': date_obj.replace(hour=7, minute=0, second=0, microsecond=0),
            'end': date_obj.replace(hour=15, minute=0, second=0, microsecond=0),
            'name': f"ShiftA_{date_obj.strftime('%d_%m_%Y')}"
        },
        'B': {
            'start': date_obj.replace(hour=15, minute=0, second=0, microsecond=0),
            'end': date_obj.replace(hour=23, minute=0, second=0, microsecond=0),
            'name': f"ShiftB_{date_obj.strftime('%d_%m_%Y')}"
        },
        'C': {
            'start': date_obj.replace(hour=23, minute=0, second=0, microsecond=0),
            'end': (date_obj + timedelta(days=1)).replace(hour=7, minute=0, second=0, microsecond=0),
            'name': f"ShiftC_{date_obj.strftime('%d_%m_%Y')}"
        }
    }
    return shifts

def execute_query_for_shift(conn, table_name, shift_start, shift_end):
    """Execute the SQL query for the given shift time range and table"""
    query = f"""
    WITH ranked AS (
        SELECT 
            timestamp, 
            status, 
            status_code, 
            LAG(status) OVER (ORDER BY timestamp) AS prev_status, 
            LAG(status_code) OVER (ORDER BY timestamp) AS prev_status_code 
        FROM {table_name} 
        WHERE timestamp BETWEEN %s AND %s
    ), 
    changes AS (
        SELECT 
            timestamp, 
            status, 
            status_code, 
            CASE 
                WHEN status != prev_status OR status_code != prev_status_code OR prev_status IS NULL OR prev_status_code IS NULL THEN 1 
                ELSE 0 
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
        ROUND(EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 60, 4) AS duration_mins,
        status, 
        status_code 
    FROM groups 
    GROUP BY group_id, status, status_code 
    ORDER BY time_start ASC;
    """
    
    cursor = conn.cursor()
    cursor.execute(query, (shift_start, shift_end))
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    return df

def process_shift(shift_key, shift_data, machine_table):
    """Process a specific shift for a specific machine table"""
    print(f"Processing {machine_table} for {shift_data['name']}...")
    
    conn = get_db_connection()
    try:
        # Get shift data
        df = execute_query_for_shift(conn, machine_table, shift_data['start'], shift_data['end'])
        
        # Save to CSV
        output_path = os.path.join(OUTPUT_FOLDER, f"{shift_data['name']}_{machine_table}.csv")
        df.to_csv(output_path, index=False)
        print(f"Saved to {output_path}")
    except Exception as e:
        print(f"Error processing {machine_table} for {shift_data['name']}: {e}")
    finally:
        conn.close()

def get_current_and_previous_shift():
    """Determine the current and previous shift based on database time"""
    # Get current time from database to ensure consistency
    db_now = get_current_db_time()
    
    # Get today's and yesterday's date at midnight
    today = db_now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)
    
    # Get shift definitions
    today_shifts = get_shift_info(today)
    yesterday_shifts = get_shift_info(yesterday)
    
    # Determine current shift
    current_shift = None
    current_shift_data = None
    
    # Check if we're in today's A shift (7:00-15:00)
    if today_shifts['A']['start'] <= db_now < today_shifts['A']['end']:
        current_shift = 'A'
        current_shift_data = today_shifts['A']
        
    # Check if we're in today's B shift (15:00-23:00)
    elif today_shifts['B']['start'] <= db_now < today_shifts['B']['end']:
        current_shift = 'B'
        current_shift_data = today_shifts['B']
        
    # Check if we're in today's C shift (23:00-07:00 next day)
    elif today_shifts['C']['start'] <= db_now:
        current_shift = 'C'
        current_shift_data = today_shifts['C']
        
    # Check if we're in yesterday's C shift (after midnight but before 7:00)
    elif db_now < today_shifts['A']['start']:
        current_shift = 'C'
        current_shift_data = yesterday_shifts['C']
    
    # Determine previous shift
    previous_shift = None
    previous_shift_data = None
    
    if current_shift == 'A':
        # Previous shift is yesterday's C
        previous_shift = 'C'
        previous_shift_data = yesterday_shifts['C']
    elif current_shift == 'B':
        # Previous shift is today's A
        previous_shift = 'A'
        previous_shift_data = today_shifts['A']
    elif current_shift == 'C':
        # Previous shift is today's B if after 23:00, otherwise yesterday's B
        if db_now >= today_shifts['C']['start']:
            previous_shift = 'B'
            previous_shift_data = today_shifts['B']
        else:
            previous_shift = 'B'
            previous_shift_data = yesterday_shifts['B']
    
    return current_shift, current_shift_data, previous_shift, previous_shift_data

def process_previous_shift():
    """Process only the shift that was before the current running shift"""
    # Get current and previous shift information
    current_shift, current_shift_data, previous_shift, previous_shift_data = get_current_and_previous_shift()
    
    print(f"Current time: {get_current_db_time()}")
    print(f"Current shift: {current_shift} ({current_shift_data['start']} to {current_shift_data['end']})")
    print(f"Previous shift: {previous_shift} ({previous_shift_data['start']} to {previous_shift_data['end']})")
    
    # Process previous shift for all machine tables
    for machine_table in MACHINE_TABLES:
        process_shift(previous_shift, previous_shift_data, machine_table)
    
    print(f"All {previous_shift} shift data processed and saved.")

def process_specific_date(date_str):
    """Process all shifts for a specific date (manual mode)"""
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=IST)
        shifts = get_shift_info(target_date)
        
        for shift_key, shift_data in shifts.items():
            for machine_table in MACHINE_TABLES:
                process_shift(shift_key, shift_data, machine_table)
        
        print(f"All shifts for {date_str} processed successfully.")
    except Exception as e:
        print(f"Error processing date {date_str}: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate shift reports.')
    parser.add_argument('--date', type=str, help='Date to process in YYYY-MM-DD format')
    args = parser.parse_args()
    
    if args.date:
        # Manual mode - process specific date
        process_specific_date(args.date)
    else:
        # Automatic mode - process the previous shift only
        process_previous_shift()
