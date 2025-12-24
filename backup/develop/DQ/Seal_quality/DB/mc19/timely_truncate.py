import psycopg2
import time
from datetime import datetime

# Database connection parameters
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'short_data_hul'
DB_USER = 'postgres'
DB_PASSWORD = 'ai4m2024'
TABLE_NAME = 'mc19_short_data'

def delete_old_records():
    db_params = {
        "host": DB_HOST,
        "port": DB_PORT,
        "database": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD
    }
    
    try:
        # Establish a connection to the database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Delete all rows except the newest 100
        delete_query = f"""
            DELETE FROM {TABLE_NAME}
            WHERE timestamp NOT IN (
                SELECT timestamp
                FROM {TABLE_NAME}
                ORDER BY timestamp DESC
                LIMIT 100
            )
        """
        
        # Execute the DELETE command
        cursor.execute(delete_query)
        
        # Get number of deleted rows
        deleted_rows = cursor.rowcount
        
        # Commit the transaction
        conn.commit()
        print(f"{datetime.now()}: Successfully deleted {deleted_rows} old rows from {TABLE_NAME}, keeping newest 100 rows.")
    
    except (Exception, psycopg2.Error) as error:
        print(f"{datetime.now()}: Error deleting rows: {error}")
    
    finally:
        # Close the database connection
        if conn:
            cursor.close()
            conn.close()

# Run the deletion operation every hour
if __name__ == "__main__":
    while True:
        delete_old_records()
        time.sleep(1 * 60 * 60)  # Sleep for 1 hour
