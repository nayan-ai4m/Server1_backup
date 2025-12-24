import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Database connection parameters
conn_params = {
    "dbname": "hul",
    "user": "postgres",
    "password": "ai4m2024",
    "host": "localhost",
    "port": "5432"
}

# Time range
start_time = "2025-09-04 12:49:21+05:30"
end_time = "2025-09-04 13:53:23+05:30"

def fetch_combined_data():
    try:
        # Create SQLAlchemy engine
        engine = create_engine(f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}")
        
        # SQL query - ONLY temperature columns from mc22_mid table
        query = text("""
        SELECT 
            mc22.timestamp,
            mc22.cam_position,
            mc22.hor_sealer_current/10 AS hor_sealer_current,
            mc22.hor_sealer_position,
            mc22.hor_pressure,
            mc22.spare1,
            mc22_mid.hor_sealer_front_1_temp,
            mc22_mid.hor_sealer_rear_1_temp
        FROM mc22
        LEFT JOIN mc22_mid ON mc22.timestamp = mc22_mid.timestamp
        WHERE mc22.timestamp >= :start_time 
        AND mc22.timestamp <= :end_time
        ORDER BY mc22.timestamp
        """)
        
        # Execute query and fetch data
        df = pd.read_sql_query(query, engine, params={"start_time": start_time, "end_time": end_time})
        
        # Apply linear interpolation to temperature columns
        print("Applying linear interpolation to temperature columns...")
        
        # Before interpolation - count missing values
        front_temp_missing_before = df['hor_sealer_front_1_temp'].isna().sum()
        rear_temp_missing_before = df['hor_sealer_rear_1_temp'].isna().sum()
        
        # Linear interpolation for temperature columns
        df['hor_sealer_front_1_temp'] = df['hor_sealer_front_1_temp'].interpolate(method='linear')
        df['hor_sealer_rear_1_temp'] = df['hor_sealer_rear_1_temp'].interpolate(method='linear')
        
        # After interpolation - count remaining missing values
        front_temp_missing_after = df['hor_sealer_front_1_temp'].isna().sum()
        rear_temp_missing_after = df['hor_sealer_rear_1_temp'].isna().sum()
        
        print(f"Front temperature - Missing before: {front_temp_missing_before}, after: {front_temp_missing_after}")
        print(f"Rear temperature - Missing before: {rear_temp_missing_before}, after: {rear_temp_missing_after}")
        
        # Optional: Fill any remaining NaN values at the beginning/end with forward/backward fill
        if front_temp_missing_after > 0 or rear_temp_missing_after > 0:
            print("Filling remaining NaN values at edges with forward/backward fill...")
            df['hor_sealer_front_1_temp'] = df['hor_sealer_front_1_temp'].fillna(method='ffill').fillna(method='bfill')
            df['hor_sealer_rear_1_temp'] = df['hor_sealer_rear_1_temp'].fillna(method='ffill').fillna(method='bfill')
            
            final_front_missing = df['hor_sealer_front_1_temp'].isna().sum()
            final_rear_missing = df['hor_sealer_rear_1_temp'].isna().sum()
            print(f"Final missing values - Front: {final_front_missing}, Rear: {final_rear_missing}")
        
        engine.dispose()
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def fetch_with_custom_handling():
    """
    Alternative approach with more explicit handling of missing values
    """
    try:
        engine = create_engine(f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}")
        
        # More explicit query with COALESCE for handling NULL values
        query = text("""
        SELECT 
            mc22.timestamp,
            mc22.cam_position,
            mc22.hor_sealer_current/10 AS hor_sealer_current,
            mc22.hor_sealer_position,
            mc22.hor_pressure,
            mc22.spare1,
            COALESCE(mc22_mid.hor_sealer_front_1_temp, NULL) as hor_sealer_front_1_temp,
            COALESCE(mc22_mid.hor_sealer_rear_1_temp, NULL) as hor_sealer_rear_1_temp
        FROM mc22
        LEFT JOIN mc22_mid ON mc22.timestamp = mc22_mid.timestamp
        WHERE mc22.timestamp >= :start_time 
        AND mc22.timestamp <= :end_time
        ORDER BY mc22.timestamp
        """)
        
        df = pd.read_sql_query(query, engine, params={"start_time": start_time, "end_time": end_time})
        
        # Apply interpolation
        df['hor_sealer_front_1_temp'] = df['hor_sealer_front_1_temp'].interpolate(method='linear')
        df['hor_sealer_rear_1_temp'] = df['hor_sealer_rear_1_temp'].interpolate(method='linear')
        
        engine.dispose()
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        return None

# Main execution
if __name__ == "__main__":
    # Fetch the combined data
    result_df = fetch_combined_data()
    
    if result_df is not None:
        print(f"Data retrieved successfully!")
        print(f"Total rows: {len(result_df)}")
        print(f"Columns ({len(result_df.columns)}): {list(result_df.columns)}")
        
        # Verify we have only the expected 8 columns
        expected_columns = [
            'timestamp', 'cam_position', 'hor_sealer_current', 
            'hor_sealer_position', 'hor_pressure', 'spare1',
            'hor_sealer_front_1_temp', 'hor_sealer_rear_1_temp'
        ]
        print(f"\nExpected columns: {expected_columns}")
        print(f"Column count - Expected: 8, Actual: {len(result_df.columns)}")
        
        # Check temperature data (with safe formatting)
        if not result_df['hor_sealer_front_1_temp'].isna().all():
            front_min = result_df['hor_sealer_front_1_temp'].min()
            front_max = result_df['hor_sealer_front_1_temp'].max()
            rear_min = result_df['hor_sealer_rear_1_temp'].min()
            rear_max = result_df['hor_sealer_rear_1_temp'].max()
            
            print(f"\nTemperature data summary:")
            print(f"Front temperature range: {front_min:.2f} to {front_max:.2f}")
            print(f"Rear temperature range: {rear_min:.2f} to {rear_max:.2f}")
        
        print("\nFirst few rows after interpolation:")
        print(result_df.head(10))
        
        # Display summary statistics
        print(f"\nData Summary:")
        print(f"Date range: {start_time} to {end_time}")
        print(f"Total records: {len(result_df)}")
        
        # Save to CSV file with timestamp
        csv_filename = f'mc22_training_sample_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        result_df.to_csv(csv_filename, index=False)
        print(f"Data saved to '{csv_filename}'")

# Alternative: Direct SQL query if you prefer to run it directly in PostgreSQL
sql_query_only = f"""
-- Direct SQL query for PostgreSQL - Temperature columns ONLY from mc22_mid
SELECT 
    mc22.timestamp,
    mc22.cam_position,
    mc22.hor_sealer_current/10 AS hor_sealer_current,
    mc22.hor_sealer_position,
    mc22.hor_pressure,
    mc22.spare1,
    mc22_mid.hor_sealer_front_1_temp,
    mc22_mid.hor_sealer_rear_1_temp
FROM mc22
LEFT JOIN mc22_mid ON mc22.timestamp = mc22_mid.timestamp
WHERE mc22.timestamp >= '{start_time}' 
AND mc22.timestamp <= '{end_time}'
ORDER BY mc22.timestamp;
"""

print("\n" + "="*50)
print("Direct SQL Query:")
print("="*50)
print(sql_query_only)
