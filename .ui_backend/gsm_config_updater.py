import psycopg2
import json
import os
from psycopg2.extensions import connection as Connection
import select

# Database connection parameters
db_params = {
    'dbname': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432'
}

def load_existing_config(file_path: str) -> dict:
    """Load existing config.json, return empty dict if file doesn't exist."""
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)
    return {}

def update_config_file(file_path: str, new_data: dict):
    """Update config.json with new data, preserving existing fields."""
    # Load existing config
    config = load_existing_config(file_path)

    # Update with new data (only the specified fields: d1, d2, d3, SIT)
    config.update(new_data)

    # Write back to file
    with open(file_path, 'w') as f:
        json.dump(config, f, indent=4)

def handle_notification(conn: Connection):
    """Listen for PostgreSQL notifications and update config.json."""
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("LISTEN config_json_update;")

    print("Listening for config_json_update notifications...")
    while True:
        select.select([conn], [], [])
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            payload = json.loads(notify.payload)
            machine = payload['machine']
            data = payload['data']
            file_path = payload['path']

            print(f"Received update for {machine}: {data}")
            try:
                update_config_file(file_path, data)
                print(f"Updated {file_path}")
            except Exception as e:
                print(f"Error updating {file_path}: {e}")

if __name__ == "__main__":
    try:
        conn = psycopg2.connect(**db_params)
        handle_notification(conn)
    except KeyboardInterrupt:
        print("Stopping listener...")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")
