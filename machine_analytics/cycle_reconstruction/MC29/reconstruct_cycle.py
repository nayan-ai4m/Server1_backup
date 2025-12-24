import numpy as np
import pandas as pd
import psycopg2
import time
import tritonclient.http as httpclient
import numpy as np 
from datetime import datetime 
import os
import csv

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

MAE_table_name = 'mc29_autoencoder'

# Get the latest spare1
def get_latest_spare1():
    #print("Fetching latest spare1...")
    conn = psycopg2.connect(**short_data_conn_params)  # Using short_data_hul database connection
    cursor = conn.cursor()
    query = "SELECT spare1 FROM mc29_short_data ORDER BY timestamp DESC LIMIT 1;"
    cursor.execute(query)
    latest_spare1 = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    #print(f"Latest spare1 fetched: {latest_spare1}")
    return latest_spare1

# Fetch data from PostgreSQL by spare1
def fetch_data_by_spare1(spare1):
    #print(f"Fetching data for spare1: {spare1}")
    conn = psycopg2.connect(**short_data_conn_params)
    cursor = conn.cursor()

    query = f"""
    SELECT
        timestamp,
        cam_position, 
        hor_sealer_current/10 AS hor_sealer_current, 
        hor_sealer_position, 
        hor_pressure, 
        hor_sealer_front_1_temp,
        hor_sealer_rear_1_temp,
        spare1
    FROM mc29_short_data
    WHERE spare1 = '{spare1}' ORDER BY timestamp DESC LIMIT 1000;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    #print(len(rows))
    cursor.close()
    conn.close()

    columns = ['timestamp', 
            'cam_position', 
            'hor_sealer_current', 
            'hor_sealer_position', 
            'hor_pressure', 
            'hor_sealer_front_1_temp',
            'hor_sealer_rear_1_temp',
            'spare1']
    #print(f"Data fetched for spare1: {spare1}, rows: {len(rows)}")
    return pd.DataFrame(rows, columns=columns)

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
    # This normalizes the original timestamps to the same range as our target
    min_ts = timestamps.min()
    max_ts = timestamps.max()
    normalized_timestamps = (timestamps - min_ts) * (360 / (max_ts - min_ts))
    
    # Perform interpolation using the normalized timestamps
    interpolated_values = np.interp(new_timestamps, normalized_timestamps, readings)
    
    return dict(zip(new_timestamps, interpolated_values))

def create_dict_data(df):                        # Pass df collected from query
    # Create dictionary dataframe with cycle id and cam_position: sample_value
    train_data = pd.DataFrame(columns=['cycle_id', 'hor_sealer_current', 'hor_sealer_position', 
                                     'hor_pressure', 'hor_sealer_front_1_temp', 'hor_sealer_rear_1_temp'])

    # Get list of unique cycle_ids
    df['cycle_id'] = df['spare1']  # Using spare1 as cycle_id
    unique_cycles = df['cycle_id'].unique()

    # Columns to create dictionaries for
    feature_columns = ['hor_sealer_current', 'hor_sealer_position', 'hor_pressure', 
                     'hor_sealer_front_1_temp', 'hor_sealer_rear_1_temp']

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

    # Display the result
    #print(f"Created train_data with {len(train_data)} rows")
    
    # Return the train_data DataFrame
    return train_data

def create_interpolated_df(train_data):
    # Columns containing dictionaries to convert to lists
    feature_columns = ['hor_sealer_current', 'hor_sealer_position', 'hor_pressure', 
                     'hor_sealer_front_1_temp', 'hor_sealer_rear_1_temp']

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

def scaled_2D_df(norm):               # Input is dataframe containing interpolated lists of len 37.
    # Scale data between 0 to 1
    norm['hor_sealer_current'] = norm['hor_sealer_current'].apply(lambda x: [i / 10 for i in x])
    norm['hor_pressure'] = norm['hor_pressure'].apply(lambda x: [i / 10 for i in x])
    norm['hor_sealer_position'] = norm['hor_sealer_position'].apply(lambda x: [i / 80 for i in x])
    norm['hor_sealer_front_1_temp'] = norm['hor_sealer_front_1_temp'].apply(lambda x: [(i - 120) / 50 for i in x])
    norm['hor_sealer_rear_1_temp'] = norm['hor_sealer_rear_1_temp'].apply(lambda x: [(i - 120) / 50 for i in x])

    df_2D = pd.DataFrame({
        'cycle_id': norm['cycle_id'],
        'image': norm.apply(lambda row: np.array([
            row['hor_sealer_current'],
            row['hor_sealer_position'],
            row['hor_pressure'],
            row['hor_sealer_front_1_temp'],
            row['hor_sealer_rear_1_temp']
        ]), axis=1)
    })

    return df_2D

def reconstruct_tensor(input_array):                                                      # Modify directory path

    client = httpclient.InferenceServerClient("192.168.1.149:8007")
    scaled_input = input_array.astype(np.float32) #np.array(input_array, dtype=np.float32).reshape(1, 1, 5, 37)  # Reshape it to match model input
    inputs = httpclient.InferInput('input', scaled_input.shape, 'FP32')
    #scaled_input = self.scaler.transform(averaged_data.drop(columns=['cam_position', 'spare1']))
    inputs.set_data_from_numpy(scaled_input)
    outputs = httpclient.InferRequestedOutput('output')
    response = client.infer(model_name="reconstruction_mc29", inputs=[inputs], outputs=[outputs])
    prediction = response.as_numpy('output')
    #print(f"Prediction: {prediction}")
    return prediction

def calculate_mae(input_array, output_array):
    
    # Ensure both arrays have the same shape
    assert input_array.shape == output_array.shape, "Arrays must have the same shape"

    org_trimmed = input_array[:, 11:-9]
    rec_trimmed = output_array[:, 11:-9]
    
    # Calculate MAE for each row
    mae_list = np.mean(np.abs(org_trimmed - rec_trimmed), axis=1)
    mae_list = mae_list.tolist()
    #print(mae_list)
    return mae_list


def get_row_of_data(cycle_id, mae_list):

    """
    Append cycle_id, timestamp, and MAE list to a CSV file in the current directory.
    
    Parameters:
    - cycle_id: ID of the current cycle
    - mae_list: List of 5 MAEs
    - filename: CSV filename (default: mae_results.csv)
    """
    # Get the current timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    
    # Prepare the row data
    row = [cycle_id, timestamp] + mae_list
    #print(row) 
    
    return row



def log_MAE_to_DB(row, table_name, conn_params,vectors):
 
    try:
        # Establish connection
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        row.extend(vectors)
        print(row)
        # SQL Insert query with dynamic table name
        insert_query = f"""
        INSERT INTO {table_name} 
        (cycle_id, timestamp, mae_seal_current, mae_sealer_position, mae_seal_pressure, mae_front_temp, mae_rear_temp,seal_current_vector, seal_pressure_vector, front_temp_vector, rear_temp_vector)
        VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s);
        """

        # Insert the single row
        cursor.execute(insert_query, row)
        conn.commit()

        #print(f"Row inserted successfully into {table_name}:", row)

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()



def main():
    current_spare1 = None

    while True:
        try:
            latest_spare1 = get_latest_spare1()
            if current_spare1 is None:
                current_spare1 = latest_spare1
                print(f"Initial spare1 set to: {current_spare1}")

            elif latest_spare1 != current_spare1:
                #print(f"Processing data for previous spare1: {current_spare1}")

                df = fetch_data_by_spare1(current_spare1)
                if not df.empty:
                    train_df1 = create_dict_data(df)
                    interpolated_df = create_interpolated_df(train_df1)

                    df_2D = scaled_2D_df(interpolated_df)
                    latest_image = df_2D['image'][0]
                    ip_img = np.expand_dims(latest_image, axis=0).astype(np.float32)

                    output_image = reconstruct_tensor(ip_img)
                    output_image = output_image.reshape(5, 37)
                    
                    mae = calculate_mae(latest_image, output_image)
                    vectors = [-1 *float(np.mean((output_image[0][15:22])*10 - (latest_image[0][15:22])*10)),-1 *float(np.mean((output_image[2][14:22])*10 - (latest_image[2][14:22])*10)),-1 * float(np.mean((output_image[3][14:22])*10 - (latest_image[3][14:22])*10)),-1 * float(np.mean((output_image[4][14:22])*10 - (latest_image[4][14:22])*10))]
                    row = get_row_of_data(df_2D['cycle_id'][0], mae)

                    log_MAE_to_DB(row, MAE_table_name, short_data_conn_params,vectors)

                    #print(latest_image[0])

                    #print((output_image[0][11:-9]-latest_image[0][ 11:-9])*10)

                    #print(f"Cycle ID: {df_2D['cycle_id'][0]}")
                    #print(f"Image shape: {latest_image.shape}")
                else:
                    print(f"No data found for spare1: {current_spare1}")
                
                current_spare1 = latest_spare1

            time.sleep(0.06)
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(5)  # Wait longer if there's an error

if __name__ == "__main__":
    main()

