import psycopg2
from psycopg2 import sql
import time

# Source database connection details (hul database)
source_db_params = {
    'dbname': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432',
}

# Target database connection details (short_data_hul database)
target_db_params = {
    'dbname': 'short_data_hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432',
}

def transfer_latest_row():
    source_connection = None
    target_connection = None
    source_cursor = None
    target_cursor = None
    
    try:
        # Connect to source PostgreSQL database
        source_connection = psycopg2.connect(**source_db_params)
        source_cursor = source_connection.cursor()
        
        # Connect to target PostgreSQL database
        target_connection = psycopg2.connect(**target_db_params)
        target_cursor = target_connection.cursor()
        
        # Get the latest row from mc19 table ordered by timestamp
        query_get_latest_row = """
            SELECT * FROM mc19
            ORDER BY timestamp DESC LIMIT 1
        """
        source_cursor.execute(query_get_latest_row)
        latest_row = source_cursor.fetchone()
        
        if latest_row:
            # Get the column names from mc19
            source_cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'mc19'
                ORDER BY ordinal_position
            """)
            columns = [row[0] for row in source_cursor.fetchall()]
            
            # Build the insert query for mc19_short_data
            insert_query = sql.SQL("""
                INSERT INTO mc19_short_data ({columns})
                VALUES ({values})
            """).format(
                columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
                values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )
            
            # Execute the insert query on target database
            target_cursor.execute(insert_query, latest_row)
            
            # Commit the transaction
            target_connection.commit()
            print("Latest row transferred successfully.")
        else:
            print("No rows found in mc19 table.")
            
    except Exception as error:
        print(f"Error: {error}")
        if target_connection:
            target_connection.rollback()  # Rollback if there's an error
    
    finally:
        # Close all cursors and connections
        if source_cursor:
            source_cursor.close()
        if target_cursor:
            target_cursor.close()
        if source_connection:
            source_connection.close()
        if target_connection:
            target_connection.close()

def run_continuously(interval_seconds):
    while True:
        transfer_latest_row()
        time.sleep(interval_seconds)  # Pause for the specified interval

# Run the transfer process with a 0.03 second interval
if __name__ == "__main__":
    run_continuously(0.018)
