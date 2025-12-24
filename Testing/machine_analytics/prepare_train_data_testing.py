import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import Error
import os
from datetime import datetime



# Function to read data from PostgreSQL database

def read_recent_data():
    # Database connection parameters
    db_config = {
        'host': 'localhost',
        'dbname': 'hul',
        'user': 'postgres',
        'password': 'ai4m2024',
        'port': '5432'
    }
    
    try:
        # Establish database connection
        connection = psycopg2.connect(**db_config)
        print("Successfully connected to PostgreSQL database")
        
        # SQL queries to fetch data from both tables
        query_fast = "SELECT timestamp, status, status_code, cam_position, horizontal_sealer_position, horizantal_motor_tq FROM mc25_fast"
        query_mid = "SELECT timestamp, hor_temp_27_pv_front, hor_temp_28_pv_rear FROM mc25_mid"
        
        # Read data into dataframes
        df_fast = pd.read_sql(query_fast, connection)
        df_mid = pd.read_sql(query_mid, connection)
        
        print(f"Successfully loaded {len(df_fast)} rows from mc25_fast")
        print(f"Successfully loaded {len(df_mid)} rows from mc25_mid")
        
        # Convert timestamps to datetime for both dataframes
        df_fast['timestamp'] = pd.to_datetime(df_fast['timestamp'])
        df_mid['timestamp'] = pd.to_datetime(df_mid['timestamp'])
        
        # Merge the dataframes on timestamp
        df = pd.merge(df_fast, df_mid, on='timestamp', how='inner')
        
        print(f"Merged dataframe has {len(df)} rows")
        print(f"minimum timestamp: {df['timestamp'].min()}, maximum timestamp: {df['timestamp'].max()}")
        print(f"Unique values in status_code are {df['status_code'].unique()}")
        
        # Close database connection
        connection.close()
        print("Database connection closed")

        return df
    
    except (Exception, Error) as e:
        print(f"Error connecting to PostgreSQL database: {str(e)}")
        if connection:
            connection.close()
        raise



#read_recent_data()


def add_cycle_id(df):
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        position_diff = df['cam_position'].diff()
        cycle_change = position_diff < -180
        df['cycle_id'] = cycle_change.cumsum()

        good = df[df['status'] == 1]

        good1 = good[['timestamp', 'cam_position', 'horizantal_motor_tq', 'horizontal_sealer_position', 
                    'hor_temp_27_pv_front', 'hor_temp_28_pv_rear', 'cycle_id']]

        max_cycle = good1['cycle_id'].max()
        good1 = good1[~good1['cycle_id'].isin([0, 1, 2, max_cycle, max_cycle - 1, max_cycle - 2])]

        print(good1[['cam_position', 'cycle_id']].head())

        return good1

    except Exception as e:
        print(f"Error in adding cycle ID: {str(e)}")
        raise


#add_cycle_id(df)


def create_dict_df(good1):
    try:
        # Create new DataFrame with dictionaries mapping cam_position to values
        train_data = pd.DataFrame(columns=['cycle_id', 'horizantal_motor_tq', 'horizontal_sealer_position', 
                                           'hor_temp_27_pv_front', 'hor_temp_28_pv_rear'])

        # Get list of unique cycle_ids
        unique_cycles = good1['cycle_id'].unique()

        # Columns to create dictionaries for
        feature_columns = ['horizantal_motor_tq', 'horizontal_sealer_position', 
                           'hor_temp_27_pv_front', 'hor_temp_28_pv_rear']

        # For each cycle_id, create a new row in train_data
        for i, cycle in enumerate(unique_cycles):
            cycle_data = good1[good1['cycle_id'] == cycle]
            row = {'cycle_id': cycle}
            # For each feature column, create a dictionary mapping cam_position to value
            for col in feature_columns:
                position_value_dict = dict(zip(cycle_data['cam_position'], cycle_data[col]))
                row[col] = position_value_dict
            
            # Add the row to train_data
            train_data.loc[i] = row

        # Display the result
        print(f"Created train_data with {len(train_data)} rows")
        print(train_data.head())

        return train_data

    except Exception as e:
        print(f"Error in creating train data: {str(e)}")
        raise

#create_dict_df(good1)



def create_interpolated_df(train_data):
    def interpolate_sensor_data(original_dict):
        timestamps = np.array(sorted(original_dict.keys())) # Ensure keys are sorted
        readings = np.array([original_dict[k] for k in timestamps]) # Get corresponding values
        new_timestamps = np.arange(0, 361, 10)
        interpolated_values = np.interp(new_timestamps, timestamps, readings)
        return dict(zip(new_timestamps, interpolated_values))

    try:
        # Columns that need interpolation
        feature_columns = ['horizantal_motor_tq', 'horizontal_sealer_position', 
                           'hor_temp_27_pv_front', 'hor_temp_28_pv_rear']

        # Apply interpolation function to each dictionary in each column
        for index, row in train_data.iterrows():
            for column in feature_columns:
                original_dict = row[column]
                interpolated_dict = interpolate_sensor_data(original_dict)
                train_data.at[index, column] = interpolated_dict

        # Verify the result
        print("Interpolation complete. Checking first row's dictionaries...")
        first_row = train_data.iloc[0]
        # for col in feature_columns:
        #     print(f"\n{col} has {len(first_row[col])} samples after interpolation")
        #     print(f"Sample positions: {list(first_row[col].keys())[:5]}...")

        # Columns containing dictionaries to convert to lists
        feature_columns = ['horizantal_motor_tq', 'horizontal_sealer_position', 
                          'hor_temp_27_pv_front', 'hor_temp_28_pv_rear']

        # For each row and column, convert dictionary to ordered list
        for index, row in train_data.iterrows():
            for column in feature_columns:
                data_dict = row[column]
                sorted_keys = sorted(data_dict.keys())
                ordered_values = [data_dict[key] for key in sorted_keys]
                train_data.at[index, column] = ordered_values

        # Verify the result
        print("Conversion complete. Checking first row's data...")
        first_row = train_data.iloc[0]
        for col in feature_columns:
            print(f"\n{col} now contains a list with {len(first_row[col])} values")
            print(f"First few values: {first_row[col][:5]}...")

        # Divide all values in the horizantal_motor_tq lists by 10
        for index, row in train_data.iterrows():
            current_list = row['horizantal_motor_tq']
            scaled_list = [value / 10 for value in current_list]
            train_data.at[index, 'horizantal_motor_tq'] = scaled_list

        # Verify the result
        print("Scaling complete for horizantal_motor_tq column")
        print(f"First row after scaling (first 5 values): {train_data.iloc[0]['horizantal_motor_tq'][:5]}")

        return train_data

    except Exception as e:
        print(f"Error in interpolation: {str(e)}")
        raise


#create_interpolated_df(train_data)


def prepare_2D_data(norm):
    try:
        # Scale data between 0 to 1
        norm['horizantal_motor_tq'] = norm['horizantal_motor_tq'].apply(lambda x: [i / 10 for i in x])
        norm['horizontal_sealer_position'] = norm['horizontal_sealer_position'].apply(lambda x: [i / 80 for i in x])
        norm['hor_temp_27_pv_front'] = norm['hor_temp_27_pv_front'].apply(lambda x: [(i - 120) / 50 for i in x])
        norm['hor_temp_28_pv_rear'] = norm['hor_temp_28_pv_rear'].apply(lambda x: [(i - 120) / 50 for i in x])

        # Stack lists to create 2D array
        train_data = pd.DataFrame({
            'cycle_id': norm['cycle_id'],
            'image': norm.apply(lambda row: np.array([
                row['horizantal_motor_tq'],
                row['horizontal_sealer_position'],
                row['hor_temp_27_pv_front'],
                row['hor_temp_28_pv_rear']
            ]), axis=1)
        })

        # Print result
        print(train_data.head())

        return train_data

    except Exception as e:
        print(f"Error in preparing 2D data: {str(e)}")
        raise


#prepare_2D_data(norm)


def save_dataset(df, split_ratio=0.75):
    try:
        # Determine split index
        split_idx = int(len(df) * split_ratio)

        # Time series split
        train_df = df.iloc[:split_idx]
        test_df = df.iloc[split_idx:]

        # Print shapes for confirmation
        print(f"Train shape: {train_df.shape}")
        print(f"Test shape: {test_df.shape}")

        # Create data directory
        os.makedirs('data', exist_ok=True)

        # Save datasets as JSON
        train_df.to_json('data/train_data.json', orient='records', lines=True)
        test_df.to_json('data/test_data.json', orient='records', lines=True)

        # Get current timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save data shapes and timestamp to data_params.txt
        with open('data/data_params.txt', mode='w') as f:
            f.write("Dataset Shapes and Info:\n")
            f.write(f"Timestamp: {timestamp}\n")
            f.write(f"Split ratio: {split_ratio}\n")
            f.write(f"train_data: {train_df.shape[0]} rows, {train_df.shape[1]} columns\n")
            f.write(f"test_data: {test_df.shape[0]} rows, {test_df.shape[1]} columns\n")

    except Exception as e:
        print(f"Error in saving dataset: {str(e)}")
        raise


##############  Main function #######################################################

def main(split_ratio):
    df = read_recent_data()
    
    df = add_cycle_id(df)

    good1 = create_dict_df(df)

    train_data = create_interpolated_df(good1)

    norm = prepare_2D_data(train_data)

    save_dataset(norm, split_ratio)

######################################################################################

if __name__ == "__main__":
    train_size = 0.75
    
    main(train_size)                   # Train dataset size
