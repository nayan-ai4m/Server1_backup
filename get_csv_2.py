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

        # Define the columns to fetch
        columns = ['status', 'status_code', 'timestamp']
        print(f"Selected columns: {columns}")

        # Open CSV file and write headers
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(columns)

            # Query only the selected columns
            query = f"""
                SELECT {', '.join(columns)}
                FROM {table_name}
                WHERE timestamp >= %s
                AND timestamp <= %s
                ORDER BY timestamp DESC
            """

            print("\nExecuting query:")
            print(query)
            print(f"Start timestamp: {start_timestamp}")
            print(f"Stop timestamp: {stop_timestamp}")

            # Use named cursor for large data fetching
            with conn.cursor('fetch_cursor') as data_cursor:
                data_cursor.execute(query, (start_timestamp, stop_timestamp))

                total_rows = 0
                while True:
                    rows = data_cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    
                    # Print first 2 rows for debugging
                    if total_rows == 0:
                        print("\nFirst few rows:")
                        for i, row in enumerate(rows[:2]):
                            print(f"Row {i + 1}: {row}")

                    writer.writerows(rows)
                    total_rows += len(rows)
                    print(f"Processed {total_rows:,} rows", end='\r')

                print(f"\nCompleted! Total rows written: {total_rows:,}")

                if total_rows == 0:
                    print("\nNo rows found. Checking table metadata:")
                    with conn.cursor() as debug_cursor:
                        debug_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        print(f"Total rows in table: {debug_cursor.fetchone()[0]}")

                        debug_cursor.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM {table_name}")
                        min_ts, max_ts = debug_cursor.fetchone()
                        print(f"Timestamp range: {min_ts} to {max_ts}")

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
    table_name = "mc18"
    start_timestamp = "2025-09-04 23:00:00.000000+05:30"
    stop_timestamp = "2025-09-05 07:30:00.000000+05:30"

    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{table_name}_status_data_{current_time}.csv"

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

