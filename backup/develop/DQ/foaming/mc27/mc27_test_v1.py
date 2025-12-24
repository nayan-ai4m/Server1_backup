import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import os
import numpy as np
import logging

# Database configuration
DATABASE_URL = 'postgresql://postgres:ai4m2024@localhost/hul'
MC27_FOAM_DATABASE_URL = 'postgresql://postgres:ai4m2024@localhost/short_data_hul'

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("mc27_machine_monitor.log"),
        logging.StreamHandler()
    ]
)

# Store the most recently calculated average thresholds
recent_thresholds = {}
cache_file_path = "mc27_threshold_cache.json"

# Helper function to load cache from file
def load_cache():
    """
    Load the threshold cache from the JSON file.
    Returns a list of up to 10 sets of thresholds with their timestamps.
    """
    if os.path.exists(cache_file_path):
        with open(cache_file_path, 'r') as f:
            cache_data = json.load(f)
            logging.info("Cache loaded successfully.")
            return cache_data
    return []

# Helper function to save cache to file
def save_cache(latest_timestamp):
    """
    Save the recent thresholds and latest timestamp to the JSON file.
    Maintains a maximum of 10 sets in the cache.
    """
    global recent_thresholds

    # Load existing cache
    cache_data = load_cache()

    # Add the new set
    new_entry = {
        "latest_timestamp": latest_timestamp.isoformat(),
        "recent_thresholds": recent_thresholds  # This should be a dictionary of thresholds
    }

    # Ensure the cache is a list, not a dictionary
    if isinstance(cache_data, list):
        cache_data.append(new_entry)  # Only append to list
    else:
        # Initialize cache_data as a list if it is not already
        cache_data = [new_entry]

    # Keep only the last 10 sets
    if len(cache_data) > 10:
        cache_data.pop(0)

    # Save to file
    with open(cache_file_path, 'w') as f:
        json.dump(cache_data, f, indent=4)
        logging.info("Cache updated and saved successfully.")


def check_machine_status():
    """
    Check the current machine status from the hul table.
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT status, NOW() FROM mc27 ORDER BY timestamp DESC LIMIT 1;")
        result = cursor.fetchone()

        if not result:
            logging.warning("No status data available.")
            return None, None

        status, timestamp = result
        logging.info(f"Machine status: {status}, Timestamp: {timestamp}")
        return status, timestamp

    except Exception as e:
        logging.error(f"Error checking machine status: {e}")
        return None, None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def log_machine_stopped(timestamp):
    """
    Log an entry in the mc27_foam table when the machine is stopped.
    """
    try:
        conn = psycopg2.connect(MC27_FOAM_DATABASE_URL)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO mc27_foam (result_timestamp, cam_cycle_group, status, difference)
        VALUES (%s, NULL, 'Machine Stopped', NULL);
        """
        cursor.execute(insert_query, (timestamp,))
        conn.commit()
        logging.info(f"Machine stopped logged in mc27_foam at {timestamp}.")

    except Exception as e:
        logging.error(f"Error logging machine stopped: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def calculate_average_threshold():
    global recent_thresholds

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(timestamp) FROM mc27;")
        latest_timestamp = cursor.fetchone()[0]

        if not latest_timestamp:
            logging.warning("No data available in the database.")
            return None

        end_time = latest_timestamp - timedelta(minutes=2)
        start_time = end_time - timedelta(minutes=30)

        query = """
        SELECT timestamp, fill_piston_1_position, cam_position
        FROM mc27
        WHERE timestamp BETWEEN %s AND %s
        AND status = 1
        ORDER BY timestamp DESC;
        """
        cursor.execute(query, (start_time, end_time))
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=["timestamp", "fill_piston_1_position", "cam_position"])
        df["cam_position"] = pd.to_numeric(df["cam_position"], errors='coerce')
        df["fill_piston_1_position"] = pd.to_numeric(df["fill_piston_1_position"], errors='coerce')
        df = df.dropna()

        df['cam_cycle_group'] = (df['cam_position'] // 10).astype(int)

        group_avg = (
            df.groupby('cam_cycle_group')
            .agg(average_threshold=('fill_piston_1_position', 'mean'),
                 latest_timestamp=('timestamp', 'max'))
            .reset_index()
        )

        recent_thresholds = group_avg.set_index('cam_cycle_group')['average_threshold'].to_dict()

        logging.info(f"Calculated Averages for Cam-Cycle Groups from {start_time} to {end_time}")
        logging.info(group_avg)

        save_cache(latest_timestamp)

        return latest_timestamp

    except Exception as e:
        logging.error(f"Error calculating average threshold: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def check_latest_entry():
    global recent_thresholds

    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        # Get the latest entry in the database
        query = """
        SELECT timestamp, fill_piston_1_position, cam_position
        FROM mc27
        ORDER BY timestamp DESC
        LIMIT 1000;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            logging.warning("No data available in the database.")
            return

        # Convert the data into a Pandas DataFrame
        df = pd.DataFrame(rows, columns=["timestamp", "fill_piston_1_position", "cam_position"])

        # Ensure cam_position and fill_piston_1_position are numeric
        df["cam_position"] = pd.to_numeric(df["cam_position"], errors='coerce')
        df["fill_piston_1_position"] = pd.to_numeric(df["fill_piston_1_position"], errors='coerce')

        # Drop rows with invalid data
        df = df.dropna()

        # Group data by cam-cycle (0-359) into groups of 10 cam-cycles
        df['cam_cycle_group'] = (df['cam_position'] // 10).astype(int)

        # Calculate average fill_piston_1_position for each cam-cycle group
        group_avg = (
            df.groupby('cam_cycle_group')
            .agg(average_fill_piston=('fill_piston_1_position', 'mean'))
            .reset_index()
        )

        # Compare with the stored average thresholds for each group
                # Compare with the stored average thresholds for each group
        results = []
        for _, row in group_avg.iterrows():
            cam_cycle_group = row['cam_cycle_group']
            average_fill_piston = row['average_fill_piston']

            # Check if cam_cycle_group falls within the specified range (17 to 26)
            if 14 <= cam_cycle_group <= 26:
                # Get the corresponding average threshold
                compair_avg_threshold = recent_thresholds.get(cam_cycle_group)

                if compair_avg_threshold is None:
                    logging.info(f"No threshold available for cam-cycle group {cam_cycle_group}.")
                    continue

                # Compare the averaged fill_piston_1_position with the average threshold
                difference = average_fill_piston - compair_avg_threshold

                if abs(difference) <= 3.5:
                    status = "No action taken"
                    results.append((datetime.now(), cam_cycle_group, status, None))
                elif difference > 3.5:
                    status = f"ABOVE the average threshold by {difference:.2f}"
                    results.append((datetime.now(), cam_cycle_group, status, difference))
                else:
                    status = f"BELOW the average threshold by {difference:.2f}"
                    results.append((datetime.now(), cam_cycle_group, status, difference))
            else:
                # Skip processing for cam_cycle_group outside the range
                logging.info(f"Cam-cycle group {cam_cycle_group} is outside the specified range (17-26).")


        save_results_to_foam_db(results)

    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def save_results_to_foam_db(results):
    try:
        conn = psycopg2.connect(MC27_FOAM_DATABASE_URL)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO mc27_foam (result_timestamp, cam_cycle_group, status, difference)
        VALUES (%s, %s, %s, %s);
        """
        for result in results:
            result = tuple(
                float(value) if isinstance(value, (np.float32, np.float64)) else value
                for value in result
            )
            cursor.execute(insert_query, result)

        conn.commit()
        logging.info("Results saved to mc27_foam table successfully.")

    except Exception as e:
        logging.error(f"Error saving results to the target database: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def calculate_wait_time(latest_timestamp):
    """
    Calculate the number of seconds remaining until the next 2-minute interval.
    """
    #seconds_remaining = (2 - (latest_timestamp.minute % 2)) * 60 - latest_timestamp.second

    """
    Calculate the number of seconds remaining until the next 1-minute interval.
    """
    seconds_remaining = 30 - latest_timestamp.second
    return seconds_remaining

if __name__ == "__main__":


    while True:
        try:
            status, timestamp = check_machine_status()
            logging.info(f"Status:  {status} " + f" also TIME :: {timestamp}")
            last_status = 1

            if status is None:
                logging.warning("No status found. Retrying in 30 sec...")
                time.sleep(30)
                continue

            if status != 1 :
                logging.info("Machine is stopped. Logging the stop and retrying in 30 sec...")
                log_machine_stopped(timestamp)
                last_status = 0
                time.sleep(30)
                continue

            if status == 1:
                cache_data = load_cache()
                time.sleep(90)


                if last_status != 1:
                    logging.info("Status changed from STOPPED to RUNNING.")

                    if cache_data:
                        latest_set = cache_data[-1]  # Get the latest set from cache
                        recent_thresholds = latest_set["recent_thresholds"]
                        cache_latest_timestamp = datetime.fromisoformat(latest_set["latest_timestamp"])
                        logging.info(f"Loaded recent thresholds from cache: {recent_thresholds}")
                        logging.info(f"Loaded recent latest_timestamp from cache: {cache_latest_timestamp}")


                        time_diff = datetime.now() - cache_latest_timestamp
                        logging.info(f"Time difference since the last update: {time_diff}")
                        # Run the cache-related tasks for `time_diff + 31 minutes`
                        time_limit = timedelta(minutes=21)


                        if time_diff <= time_limit:
                            check_latest_entry()
                            wait_time = calculate_wait_time(latest_timestamp)
                            logging.info(f"Waiting for {wait_time} seconds until the next calculation...")
                            time.sleep(wait_time)

                        else:
                            logging.info("Cache expired or time exceeded 31 minutes. Updating thresholds...")
                            last_status = 1  # Set last_status to 1 after 31 minutes

                    else:
                        logging.info("No cache data available.")

                else:

                    logging.info("Cache expired. Updating thresholds from the database.")
                    latest_timestamp = calculate_average_threshold()
                    if latest_timestamp:
                        check_latest_entry()  # Update the cache with latest data from DB
                        wait_time = calculate_wait_time(latest_timestamp)
                        logging.info(f"Waiting for {wait_time} seconds until the next calculation...")
                        time.sleep(wait_time)
                        last_status = 1
                        continue

            else:
                logging.info("No data found. Retrying in 30 sec...")
                time.sleep(30)
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
            time.sleep(30)

