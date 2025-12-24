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

# Configuration - specify which table to transfer
SOURCE_TABLE = 'mc26_fast'  # Change this to your desired table
TARGET_TABLE = SOURCE_TABLE.replace('_fast', '') + '_short_data'

# Store last transferred timestamp to avoid duplicates
last_transferred_timestamp = None

def transfer_latest_row():
    global last_transferred_timestamp
    
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
        
        # On first run, get the last timestamp from target
        if last_transferred_timestamp is None:
            check_query = sql.SQL("SELECT MAX(timestamp) FROM {}").format(
                sql.Identifier(TARGET_TABLE)
            )
            target_cursor.execute(check_query)
            result = target_cursor.fetchone()
            last_transferred_timestamp = result[0] if result[0] else '1970-01-01'
            print(f"Starting from timestamp: {last_transferred_timestamp}")
        
        # Get new rows since last transfer
        query_get_new_rows = sql.SQL("""
            SELECT * FROM {}
            WHERE timestamp > %s
            ORDER BY timestamp ASC
            LIMIT 100
        """).format(sql.Identifier(SOURCE_TABLE))
        
        source_cursor.execute(query_get_new_rows, (last_transferred_timestamp,))
        new_rows = source_cursor.fetchall()
        
        if new_rows:
            # Get the column names
            source_cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (SOURCE_TABLE,))
            columns = [row[0] for row in source_cursor.fetchall()]
            
            # Find timestamp column index
            timestamp_index = columns.index('timestamp')
            
            # Build the insert query
            insert_query = sql.SQL("""
                INSERT INTO {target_table} ({columns})
                VALUES ({values})
                ON CONFLICT (timestamp) DO NOTHING
            """).format(
                target_table=sql.Identifier(TARGET_TABLE),
                columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
                values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )
            
            # Insert all new rows
            inserted_count = 0
            for row in new_rows:
                try:
                    target_cursor.execute(insert_query, row)
                    inserted_count += 1
                    # Update last transferred timestamp
                    last_transferred_timestamp = row[timestamp_index]
                except psycopg2.IntegrityError:
                    # Skip duplicates
                    target_connection.rollback()
                    continue
            
            # Commit the transaction
            target_connection.commit()
            
            if inserted_count > 0:
                print(f"Transferred {inserted_count} new rows (out of {len(new_rows)} found)")
        
    except Exception as error:
        print(f"Error: {error}")
        if target_connection:
            target_connection.rollback()
    
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

def setup_target_table():
    """Ensure target table has unique constraint on timestamp"""
    try:
        conn = psycopg2.connect(**target_db_params)
        cur = conn.cursor()
        
        # Add unique constraint if it doesn't exist
        constraint_name = f"{TARGET_TABLE}_timestamp_unique"
        cur.execute(sql.SQL("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conname = %s
                ) THEN
                    ALTER TABLE {} ADD CONSTRAINT {} UNIQUE (timestamp);
                END IF;
            END $$;
        """).format(
            sql.Identifier(TARGET_TABLE),
            sql.Identifier(constraint_name)
        ), (constraint_name,))
        
        conn.commit()
        print(f"Target table {TARGET_TABLE} is ready")
        conn.close()
    except Exception as e:
        print(f"Setup note: {e}")

def run_continuously(interval_seconds):
    print(f"Starting continuous transfer from {SOURCE_TABLE} to {TARGET_TABLE}")
    print(f"Transfer interval: {interval_seconds} seconds")
    print("Press Ctrl+C to stop...\n")
    
    # Setup target table
    setup_target_table()
    
    try:
        while True:
            transfer_latest_row()
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("\nTransfer stopped by user")

# Run the transfer process
if __name__ == "__main__":
    run_continuously(0.005)  # 5 milliseconds interval
