import psycopg2
from datetime import datetime
import csv
import sys

def get_data_in_timerange(
    table_name,
    output_file,
    start_timestamp,
    stop_timestamp,
    chunk_size=10000,
    dbname="hul",
    user="postgres",
    password="ai4m2024",
    host="localhost",
    port="5432"
):
    try:
        # Connect to database
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print("Connected to database successfully")

        # Get column names using a regular cursor
        with conn.cursor() as schema_cursor:
            schema_cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position;
            """, (table_name,))
            columns = [col[0] for col in schema_cursor.fetchall()]
            print(f"Found {len(columns)} columns")
            print("Columns:", columns)  # Debug print for columns

        # Open CSV file and write headers
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            
            # Query with debug prints
            query = f"""
                SELECT * 
                FROM {table_name} 
                WHERE timestamp >= timestamp with time zone %s 
                AND timestamp <= timestamp with time zone %s 
                ORDER BY timestamp DESC
            """
            
            print("\nExecuting query:")
            print(query)
            print(f"Start timestamp: {start_timestamp}")
            print(f"Stop timestamp: {stop_timestamp}")

            # Use named cursor for the main data fetch
            with conn.cursor('fetch_cursor') as data_cursor:
                data_cursor.execute(query, (start_timestamp, stop_timestamp))
                
                total_rows = 0
                # Debug: print first few rows
                print("\nFirst few rows of data:")
                while True:
                    rows = data_cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    
                    # Debug print for first 2 rows in each chunk
                    if total_rows == 0:
                        for i, row in enumerate(rows[:2]):
                            print(f"\nRow {i + 1}:")
                            for col, val in zip(columns, row):
                                print(f"{col}: {val}")
                    
                    writer.writerows(rows)
                    total_rows += len(rows)
                    print(f"Processed {total_rows:,} rows", end='\r')
                
                print(f"\nCompleted! Total rows written: {total_rows:,}")

                # If no rows found, try to debug why
                if total_rows == 0:
                    print("\nNo rows found. Checking table data:")
                    with conn.cursor() as debug_cursor:
                        # Check total rows in table
                        debug_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        total_table_rows = debug_cursor.fetchone()[0]
                        print(f"Total rows in table: {total_table_rows}")
                        
                        # Check timestamp range in table
                        debug_cursor.execute(f"""
                            SELECT MIN(timestamp), MAX(timestamp)
                            FROM {table_name}
                        """)
                        min_ts, max_ts = debug_cursor.fetchone()
                        print(f"Table timestamp range: {min_ts} to {max_ts}")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        print("\nFull error traceback:")
        print(traceback.format_exc())
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()
            print("\nDatabase connection closed")

def main():
    table_name = "mc22"
    start_timestamp = "2025-01-28 00:00:00.000000+05:30"
    stop_timestamp = "2025-01-28 23:59:59.000000+05:30"
    
    # Generate filename with timestamp
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{table_name}_data_{current_time}.csv"
    
    print(f"\nStarting data extraction:")
    print(f"Table: {table_name}")
    print(f"Output file: {output_file}")
    print(f"Time range: {start_timestamp} to {stop_timestamp}")
    
    get_data_in_timerange(
        table_name=table_name,
        output_file=output_file,
        start_timestamp=start_timestamp,
        stop_timestamp=stop_timestamp
    )

if __name__ == "__main__":
    main()
