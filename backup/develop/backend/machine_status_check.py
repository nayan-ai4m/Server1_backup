import psycopg2
import time
from datetime import datetime, timedelta


conn = psycopg2.connect(
    dbname="hul",
    user="postgres",
    password="ai4m2024",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()


tables = [f"mc{n}" for n in range(17, 31) if n not in (23, 24)]

try:
    while True:
        current_time = datetime.now()  
        buffer_time = current_time - timedelta(minutes=4)  
        print(f"\nChecking machine statuses at {current_time} (buffer: last 2 minutes)...\n")
        print(type(buffer_time))

        for table in tables:
            
            column_name = "state" if table in ("mc28", "mc29") else "status"

            query = f"SELECT timestamp, {column_name} FROM {table} ORDER BY timestamp DESC LIMIT 1"
            cursor.execute(query)
            row = cursor.fetchone() 

            if row:
                db_timestamp, column_value = row
                print(type(db_timestamp),"1")
                db_timestamp = db_timestamp.replace(tzinfo=None)
                print(type(db_timestamp),"2")
                
                if db_timestamp >= buffer_time:
                    print(f"Machine in {table} is ONLINE (timestamp: {db_timestamp}, {column_name}: {column_value})")
                else:
                    print(f"Machine in {table} is OFFLINE (last updated: {db_timestamp}, {column_name}: {column_value})")
            else:
                print(f"No data available for table {table}")

        print("\nWaiting for 20 seconds before the next check...\n")
        time.sleep(20)

except KeyboardInterrupt:
    print("Stopping status checks.")
finally:
    cursor.close()
    conn.close()
