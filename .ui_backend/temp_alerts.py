import psycopg2
import json
import uuid
import time
from datetime import datetime

def process_data():
    # Database connection
    conn = psycopg2.connect(
        dbname="hul",
        user="postgres",
        password="ai4m2024",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # Fetch data
    cur.execute("SELECT mc17, mc18, mc19, mc20, mc21, mc22, mc25, mc26, mc27, mc28, mc29, mc30 FROM cls1_setpoints;")
    rows = cur.fetchall()

    # Column names
    columns = ["mc17", "mc18", "mc19", "mc20", "mc21", "mc22", "mc25", "mc26", "mc27", "mc28", "mc29", "mc30"]

    # Process each row
    for row in rows:
        for col_name, col_data in zip(columns, row):
            if col_data is None:
                continue  # Skip if column is empty

            # Check if data is already a dictionary, otherwise parse it
            if isinstance(col_data, dict):
                data = col_data
            else:
                try:
                    data = json.loads(col_data)  # Convert JSONB string to dict
                except json.JSONDecodeError as e:
                    print(f"JSON decode error in {col_name}: {e}")
                    continue

            # Ensure required keys exist
            if "data" in data and "TempDisplay" in data and "Bandwidth" in data:
                for key in data["data"]:
                    if key in data["TempDisplay"] and key in data["Bandwidth"]:
                        diff = abs(data["data"][key] - data["TempDisplay"][key])
                        bandwidth_threshold = data["Bandwidth"].get(key, 0)  # Default to 0 if key not found

                        if diff > bandwidth_threshold:  # Alert condition
                            timestamp = datetime.now().isoformat()
                            event_id = str(uuid.uuid4())  # Generate unique ID
                            zone = "PLC"
                            camera_id = col_name  # Column in which the event occurred

                            # **Remove "HMI" if present and replace underscores with spaces**
                            formatted_key = key.replace("_", " ")  # Replace _ with spaces
                            if formatted_key.startswith("HMI "):  # Remove "HMI " if it appears at the start
                                formatted_key = formatted_key[4:]

                            event_type = f"High Temperature at {formatted_key}"  # Event details
                            alert_type = "Breakdown"

                            # Insert into sample_event_table
                            insert_query = """
                            INSERT INTO sample_event_table (timestamp, event_id, zone, camera_id, event_type, alert_type)
                            VALUES (%s, %s, %s, %s, %s, %s);
                            """
                            cur.execute(insert_query, (timestamp, event_id, zone, camera_id, event_type, alert_type))
                            conn.commit()

    cur.close()
    conn.close()
    print("Alerts inserted successfully!")

# Run the script every 30 seconds
while True:
    process_data()
    time.sleep(30)

