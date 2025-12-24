import numpy as np
import pandas as pd
import psycopg2
import time
import tritonclient.http as httpclient
from datetime import datetime 

short_data_conn_params = {
    'dbname': 'short_data_hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432'
}

hul_conn_params = {
    'dbname': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432'
}

MAE_table_name = 'mc26_autoencoder'


# Get the latest cycle_id
def get_latest_cycle_id():
    conn = psycopg2.connect(**short_data_conn_params)
    cursor = conn.cursor()
    query = "SELECT cycle_id FROM mc26_short_data ORDER BY timestamp DESC LIMIT 1;"
    cursor.execute(query)
    latest_cycle_id = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return latest_cycle_id


mid_data_conn_params = {
    'dbname': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432'
}

def fetch_data_by_cycle_id(cycle_id):
    # --- Step 1: Get short_data_hul features ---
    conn1 = psycopg2.connect(**short_data_conn_params)
    cursor1 = conn1.cursor()
    query1 = f"""
        SELECT
            timestamp,
            cam_position, 
            horizantal_motor_tq, 
            horizontal_sealer_position, 
            cycle_id
        FROM mc26_short_data
        WHERE cycle_id = {cycle_id}
        ORDER BY timestamp DESC LIMIT 1000;
    """
    cursor1.execute(query1)
    rows1 = cursor1.fetchall()
    cursor1.close()
    conn1.close()

    short_columns = ['timestamp', 'cam_position', 'horizantal_motor_tq', 
                     'horizontal_sealer_position', 'cycle_id']
    df_short = pd.DataFrame(rows1, columns=short_columns)

    if df_short.empty:
        print(f"No short_data found for cycle_id: {cycle_id}")
        return pd.DataFrame()

    # --- Step 2: Get latest mid table temps from hul DB ---
    conn2 = psycopg2.connect(**mid_data_conn_params)
    cursor2 = conn2.cursor()
    query2 = """
        SELECT hor_temp_27_pv_front, hor_temp_28_pv_rear
        FROM mc26_mid
        ORDER BY timestamp DESC LIMIT 1;
    """
    cursor2.execute(query2)
    row2 = cursor2.fetchone()
    cursor2.close()
    conn2.close()

    if row2:
        front_temp, rear_temp = row2
    else:
        front_temp, rear_temp = None, None

    # --- Step 3: Add temps to all rows in df_short ---
    df_short['hor_temp_27_pv_front'] = front_temp
    df_short['hor_temp_28_pv_rear'] = rear_temp

    print(f"Data fetched for cycle_id: {cycle_id}, rows: {len(df_short)} (with latest temps from mc26_mid)")
    return df_short


# Interpolation of all dictionaries
def interpolate_sensor_data(original_dict, num_points=37):
    """
    Interpolate sensor data to create exactly 37 equidistant points from 0 to 360.
    
    Args:
        original_dict: Dictionary with original timestamps as keys and sensor readings as values
        num_points: Number of points to interpolate (default: 37)
    
    Returns:
        Dictionary with new timestamps (0 to 360) and interpolated values
    """
    # Extract timestamps and readings
    timestamps = np.array(sorted(original_dict.keys()))
    readings = np.array([original_dict[k] for k in timestamps])
    
    # Convert timestamps and readings to float
    try:
        timestamps = timestamps.astype(float)
        readings = readings.astype(float)
    except ValueError:
        print("Converting timestamps to float (seconds since epoch)...")
        timestamps = np.array([ts.timestamp() if isinstance(ts, pd.Timestamp) else float(ts) for ts in timestamps])
        readings = readings.astype(float)
    
    # Create new timestamps from 0 to 360 with num_points equidistant points
    new_timestamps = np.linspace(0, 360, num_points)
    
    # Map the original timestamps to a 0-360 range for interpolation
    min_ts = timestamps.min()
    max_ts = timestamps.max()
    normalized_timestamps = (timestamps - min_ts) * (360 / (max_ts - min_ts))
    
    # Perform interpolation using the normalized timestamps
    interpolated_values = np.interp(new_timestamps, normalized_timestamps, readings)
    
    return dict(zip(new_timestamps, interpolated_values))

def create_dict_data(df):
    # Create dictionary dataframe with cycle id and cam_position: sample_value
    train_data = pd.DataFrame(columns=['cycle_id', 'horizantal_motor_tq', 'horizontal_sealer_position', 
                                     'hor_temp_27_pv_front', 'hor_temp_28_pv_rear'])

    # Get list of unique cycle_ids
    unique_cycles = df['cycle_id'].unique()

    # Columns to create dictionaries for
    feature_columns = ['horizantal_motor_tq', 'horizontal_sealer_position', 
                     'hor_temp_27_pv_front', 'hor_temp_28_pv_rear']

    # For each cycle_id, create a new row in train_data
    for i, cycle in enumerate(unique_cycles):
        # Filter data for this cycle_id
        cycle_data = df[df['cycle_id'] == cycle]

        # Create a new row for this cycle
        row = {'cycle_id': cycle}

        # For each feature column, create a dictionary mapping cam_position to value
        for col in feature_columns:
            position_value_dict = dict(zip(cycle_data['cam_position'], cycle_data[col]))
            row[col] = position_value_dict

        # Add the row to train_data
        train_data.loc[i] = row
    
    return train_data

def create_interpolated_df(train_data):
    # Columns containing dictionaries to convert to lists
    feature_columns = ['hor_temp_27_pv_front', 'hor_temp_28_pv_rear']

    # Apply interpolation function to each dictionary in each column
    for index, row in train_data.iterrows():
        for column in feature_columns:
            # Get the original dictionary
            original_dict = row[column]
            
            # Apply interpolation
            interpolated_dict = interpolate_sensor_data(original_dict)
            
            # Update the cell with the interpolated dictionary
            train_data.at[index, column] = interpolated_dict

    for index, row in train_data.iterrows():
        for column in feature_columns:
            # Get the dictionary
            data_dict = row[column]
            
            # Sort keys (cam positions) numerically
            sorted_keys = sorted(data_dict.keys())
            
            # Create ordered list of values
            ordered_values = [data_dict[key] for key in sorted_keys]
            
            # Replace dictionary with ordered list
            train_data.at[index, column] = ordered_values

    return train_data

def scaled_2D_df(norm):
    # Scale data between 0 to 1
    norm['horizantal_motor_tq'] = norm['horizantal_motor_tq'].apply(lambda x: [i / 10 for i in x])
    norm['horizontal_sealer_position'] = norm['horizontal_sealer_position'].apply(lambda x: [i / 80 for i in x])
    norm['hor_temp_27_pv_front'] = norm['hor_temp_27_pv_front'].apply(lambda x: [(i - 120) / 50 for i in x])
    norm['hor_temp_28_pv_rear'] = norm['hor_temp_28_pv_rear'].apply(lambda x: [(i - 120) / 50 for i in x])

    df_2D = pd.DataFrame({
        'cycle_id': norm['cycle_id'],
        'image': norm.apply(lambda row: np.array([
            row['horizantal_motor_tq'],
            row['horizontal_sealer_position'],
            row['hor_temp_27_pv_front'],
            row['hor_temp_28_pv_rear']
        ]), axis=1)
    })

    return df_2D

def reconstruct_tensor(input_array):
    client = httpclient.InferenceServerClient("192.168.1.149:8007")
    scaled_input = input_array.astype(np.float32)
    inputs = httpclient.InferInput('input', scaled_input.shape, 'FP32')
    inputs.set_data_from_numpy(scaled_input)
    outputs = httpclient.InferRequestedOutput('output')
    response = client.infer(model_name="reconstruction_mc26", inputs=[inputs], outputs=[outputs])
    prediction = response.as_numpy('output')
    return prediction

def calculate_mae(input_array, output_array):
    # Ensure both arrays have the same shape
    assert input_array.shape == output_array.shape, "Arrays must have the same shape"

    org_trimmed = input_array[:, 11:-9]
    rec_trimmed = output_array[:, 11:-9]
    
    # Calculate MAE for each row
    mae_list = np.mean(np.abs(org_trimmed - rec_trimmed), axis=1)
    mae_list = mae_list.tolist()
    return mae_list


def get_row_of_data(cycle_id, mae_list):
    """
    Prepare cycle_id, timestamp, and MAE list for database insertion.
    
    Parameters:
    - cycle_id: ID of the current cycle
    - mae_list: List of 4 MAEs
    """
    # Get the current timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    
    # Prepare the row data
    row = [cycle_id, timestamp] + mae_list
    
    return row


def log_MAE_to_DB(row, table_name, conn_params, vectors):
    try:
        # Establish connection
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        row.extend(vectors)
        print(row)
        
        # SQL Insert query with dynamic table name
        insert_query = f"""
        INSERT INTO {table_name} 
        (cycle_id, timestamp, mae_motor_tq, mae_sealer_position, mae_front_temp, mae_rear_temp, 
         motor_tq_vector, front_temp_vector, rear_temp_vector)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Insert the single row
        cursor.execute(insert_query, row)
        conn.commit()

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()


def main():
    current_cycle_id = None

    while True:
        try:
            latest_cycle_id = get_latest_cycle_id()
            if current_cycle_id is None:
                current_cycle_id = latest_cycle_id
                print(f"Initial cycle_id set to: {current_cycle_id}")

            elif latest_cycle_id != current_cycle_id:
                df = fetch_data_by_cycle_id(current_cycle_id)
                if not df.empty:
                    train_df1 = create_dict_data(df)
                    interpolated_df = create_interpolated_df(train_df1)

                    df_2D = scaled_2D_df(interpolated_df)

                    # === Debug: print vector lengths and sample values ===
                    print("\n[DEBUG] Checking feature vector lengths for current cycle:")
                    for col in ['horizantal_motor_tq', 'horizontal_sealer_position', 'hor_temp_27_pv_front', 'hor_temp_28_pv_rear']:
                        data = interpolated_df[col][0]
                        if isinstance(data, dict):
                            print(f"  {col}: dict with {len(data)} keys")
                        elif isinstance(data, list):
                            print(f"  {col}: list with {len(data)} values, first 5: {data[:5]}")
                        else:
                            print(f"  {col}: type={type(data)} -> {data}")
                    print("==============================================\n")

                    latest_image = df_2D['image'][0]
                    ip_img = np.expand_dims(latest_image, axis=0).astype(np.float32)

                    output_image = reconstruct_tensor(ip_img)
                    output_image = output_image.reshape(4, 37)
                    
                    mae = calculate_mae(latest_image, output_image)
                    
                    # Calculate vectors for motor torque, front temp, and rear temp
                    vectors = [
                        -1 * float(np.mean((output_image[0][15:22]) * 10 - (latest_image[0][15:22]) * 10)),
                        -1 * float(np.mean((output_image[2][14:22]) * 10 - (latest_image[2][14:22]) * 10)),
                        -1 * float(np.mean((output_image[3][14:22]) * 10 - (latest_image[3][14:22]) * 10))
                    ]
                    
                    row = get_row_of_data(df_2D['cycle_id'][0], mae)
                    log_MAE_to_DB(row, MAE_table_name, short_data_conn_params, vectors)
                else:
                    print(f"No data found for cycle_id: {current_cycle_id}")
                
                current_cycle_id = latest_cycle_id

            time.sleep(0.06)
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()

