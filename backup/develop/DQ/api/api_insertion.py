import requests
import json
import logging
import sys
import os
import psycopg2
from request_body import build_telemetry_request_body  # Assuming you have this function in a separate file
from datetime import datetime
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection details
DB_HOST = 'localhost'
DB_NAME = 'sensor_db'
DB_USER = 'postgres'
DB_PASS = 'ai4m2024'

class APIClient:
    def __init__(self, auth_url, username, password):
        self.auth_url = auth_url
        self.username = username
        self.password = password
        self.token = None
        self.token_retrieved_time = None  # Track the token retrieval time

    def get_jwt_token(self):
        """Retrieve JWT token for authentication."""
        try:
            payload = {
                'username': self.username,
                'password': self.password
            }
            response = requests.post(self.auth_url, json=payload)
            response.raise_for_status()

            self.token = response.json().get('token')

            if self.token:
                logging.info("Successfully retrieved JWT token.")
                self.token_retrieved_time = time.time()  # Record the token retrieval time
                return self.token
            else:
                logging.warning("Token not found in response.")
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching JWT token: {e}")
            return None

    def refresh_token_if_needed(self):
        """Check if 45 minutes have passed since the last token retrieval and refresh the token if needed."""
        if self.token and self.token_retrieved_time:
            elapsed_time = time.time() - self.token_retrieved_time
            if elapsed_time >= 2700:  # 45 minutes in seconds
                logging.info("45 minutes have passed. Refreshing JWT token...")
                self.get_jwt_token()
        else:
            self.get_jwt_token()  # Get token if it's not set yet

    def fetch_data(self, endpoint):
        """Fetch data from the specified endpoint."""
        self.refresh_token_if_needed()  # Ensure the token is valid
        if not self.token:
            logging.error("No valid JWT token available. Please authenticate first.")
            return None

        headers = {
            'Authorization': f'Bearer {self.token}'
        }

        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from {endpoint}: {e}")
            return None

    def fetch_device_telemetry(self, entity_group_id):
        """Fetch telemetry data for the specified device group ID."""
        self.refresh_token_if_needed()  # Ensure the token is valid
        if not self.token:
            logging.error("No valid JWT token available. Please authenticate first.")
            return None

        url = "https://iedge360.cimcondigital.com/api/entitiesQuery/find"
        headers = {
            'X-authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }

        body = build_telemetry_request_body(entity_group_id)

        try:
            response = requests.post(url, headers=headers, json=body)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching device telemetry data: {e}")
            return None

    def extract_entity_id(self, data):
        """Extracts the 'id' from the entityGroup in the data."""
        if isinstance(data, list) and len(data) > 0:
            entity_id = data[0].get('id', {}).get('id')
            if entity_id:
                return entity_id
        return None

# Function to connect to the PostgreSQL database
def connect_db():
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        logging.info("Database connection successful.")
        return conn
    except Exception as e:
        logging.error("Database connection failed.")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(f"{exc_type} {fname} {exc_tb.tb_lineno}: {e}")

def insert_device_data(conn, data):
    with conn.cursor() as cursor:
        # Insert query for deviceid_data table
        insert_device_data_query = """
        INSERT INTO deviceid_data (entity_id, timestamp,entity_type, attribute_name, attribute_value, attribute_value_str)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        # Insert query for deviceid_timeseries table
        insert_metrics_query = """
        INSERT INTO deviceid_timeseries (device_id, timestamp, x_rms_vel, y_rms_vel, z_rms_vel, x_rms_acl, y_rms_acl, z_rms_acl, SPL_dB, LED_status, temp_c,label)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
        """

        # Loop over the telemetry data and insert into both tables
        for item in data['data']:
            print(item)
            entity_id = item['entityId']['id']
            entity_type = item['entityId']['entityType']

            # Insert data into deviceid_data table (as it was)
            if 'latest' in item and 'TIME_SERIES' in item['latest']:
                for attribute, value in item['latest']['TIME_SERIES'].items():
                    timestamp = datetime.fromtimestamp(value['ts'] / 1000)
                    attr_value = float(value['value']) if isinstance(value['value'], (int, float)) else None
                    attr_value_str = str(value['value']) if not isinstance(value['value'], (int, float)) else None
                    cursor.execute(insert_device_data_query, (entity_id,timestamp, entity_type, attribute, attr_value, attr_value_str))

            if 'CLIENT_ATTRIBUTE' in item['latest']:
                for attribute, value in item['latest']['CLIENT_ATTRIBUTE'].items():
                    timestamp = datetime.fromtimestamp(value['ts'] / 1000)
                    attr_value = float(value['value']) if isinstance(value['value'], (int, float)) else None
                    attr_value_str = str(value['value']) if not isinstance(value['value'], (int, float)) else None
                    cursor.execute(insert_device_data_query, (entity_id,timestamp, entity_type, attribute, attr_value, attr_value_str))

            if 'ENTITY_FIELD' in item['latest']:
                for attribute, value in item['latest']['ENTITY_FIELD'].items():
                    timestamp = datetime.fromtimestamp(value['ts'] / 1000)
                    attr_value = float(value['value']) if isinstance(value['value'], (int, float)) else None
                    attr_value_str = str(value['value']) if not isinstance(value['value'], (int, float)) else None
                    cursor.execute(insert_device_data_query, (entity_id, timestamp,entity_type, attribute, attr_value, attr_value_str))

            if 'SERVER_ATTRIBUTE' in item['latest']:
                for attribute, value in item['latest']['SERVER_ATTRIBUTE'].items():
                    timestamp = datetime.fromtimestamp(value['ts'] / 1000)
                    attr_value = float(value['value']) if isinstance(value['value'], (int, float)) else None
                    attr_value_str = str(value['value']) if not isinstance(value['value'], (int, float)) else None
                    cursor.execute(insert_device_data_query, (entity_id,timestamp, entity_type, attribute, attr_value, attr_value_str))


            # Insert data into deviceid_timeseries table
            if 'latest' in item and 'TIME_SERIES' in item['latest']:
                telemetry = item['latest']['TIME_SERIES']
                timestamp = None
                x_rms_vel = telemetry.get('x_rms_vel', {}).get('value')
                y_rms_vel = telemetry.get('y_rms_vel', {}).get('value')
                z_rms_vel = telemetry.get('z_rms_vel', {}).get('value')
                x_rms_acl = telemetry.get('x_rms_acl', {}).get('value')
                y_rms_acl = telemetry.get('y_rms_acl', {}).get('value')
                z_rms_acl = telemetry.get('z_rms_acl', {}).get('value')
                SPL_dB = telemetry.get('SPL_dB', {}).get('value')
                LED_status = telemetry.get('LED_status', {}).get('value')
                temp_c = telemetry.get('temp_c', {}).get('value')

                label = None
                if 'ENTITY_FIELD' in item['latest']:
                    label = item['latest']['ENTITY_FIELD'].get('label', {}).get('value')
            

                # Ensure that we have a timestamp from one of the attributes
                if 'x_rms_vel' in telemetry:
                    timestamp = datetime.fromtimestamp(telemetry['x_rms_vel']['ts'] / 1000)

                # Insert into deviceid_timeseries if timestamp is available
                if timestamp:
                    cursor.execute(insert_metrics_query, (
                        entity_id, timestamp, x_rms_vel, y_rms_vel, z_rms_vel, x_rms_acl, y_rms_acl, z_rms_acl, SPL_dB, LED_status, temp_c,label
                    ))

        # Commit the changes
        conn.commit()
        logging.info("Successfully inserted data into deviceid_data and deviceid_timeseries tables.")



def main():
    auth_url = "https://iedge360.cimcondigital.com/api/auth/login"
    username = "user@hulh.com"  # Replace with actual username
    password = "user@123"  # Replace with actual password

    client = APIClient(auth_url, username, password)
    while True:
        jwt_token = client.get_jwt_token()

        if jwt_token:
            asset_url = "https://iedge360.cimcondigital.com/api/entityGroups/ASSET"
            device_url = "https://iedge360.cimcondigital.com/api/entityGroups/DEVICE"

            # Fetch asset data
            asset_data = client.fetch_data(asset_url)
            if asset_data:
                asset_id = client.extract_entity_id(asset_data)
                if asset_id:
                    logging.info(f"Extracted Asset ID: {asset_id}")

                    # Fetch device data
                    device_data = client.fetch_data(device_url)
                    if device_data:
                        device_id = client.extract_entity_id(device_data)
                        if device_id:
                            logging.info(f"Extracted Device ID: {device_id}")

                            # Fetch telemetry data
                            telemetry_data = client.fetch_device_telemetry(device_id)
                            if telemetry_data:

                                conn = connect_db()
                                if conn:
                                    insert_device_data(conn, telemetry_data)  # Pass conn and telemetry_data to the function
                                    conn.close()  # Close the connection after data insertion
                                else:
                                    logging.error("Failed to connect to database.")
                            else:
                                logging.error("Failed to fetch telemetry data.")
                else:
                    logging.warning("No valid Asset ID found.")
            else:
                logging.error("Failed to fetch asset data.")
        else:
            logging.error("Failed to retrieve JWT token")

        time.sleep(60)  # Wait for 1 minute before fetching data again

if __name__ == "__main__":
    main()
