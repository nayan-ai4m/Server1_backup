#!/usr/bin/env python3

import psycopg2
from psycopg2 import sql
from datetime import datetime
import os
import csv

# ================================= Inputs ==========================================================
table_name = "mc17"
start_time = "2025-06-10 07:10:00+05:30"
end_time = "2025-06-10 08:10:00+05:30"
# ===================================================================================================

# Ensure 'data' folder exists in current directory
output_dir = os.path.join(os.getcwd(), "data")
os.makedirs(output_dir, exist_ok=True)

# Output CSV file path
output_file = os.path.join(output_dir, "recent_data.csv")

# Log file path
log_file = os.path.join(os.getcwd(), "retrain.log")

# === Logging Function ===
def log(message):
    with open(log_file, "a") as logf:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logf.write(f"[{timestamp}] {message}\n")

# === Export Logic ===
try:
    conn = psycopg2.connect(
        host='localhost',
        dbname='hul',
        user='postgres',
        password='ai4m2024',
        port='5432'
    )
    cur = conn.cursor()
    
    # First, execute the SELECT query
    query = sql.SQL("""
        SELECT *
        FROM {table}
        WHERE timestamp BETWEEN %s AND %s
        ORDER BY timestamp ASC
    """).format(table=sql.Identifier(table_name))
    
    cur.execute(query, (start_time, end_time))
    
    # Get column names for CSV header
    colnames = [desc[0] for desc in cur.description]
    
    # Write to CSV file using client-side approach
    with open(output_file, 'w', newline='') as csvfile:
        import csv
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(colnames)
        # Write data rows
        while True:
            rows = cur.fetchmany(1000)  # Fetch in batches for memory efficiency
            if not rows:
                break
            writer.writerows(rows)
    conn.commit()
    log(f"Data export successful: {output_file}")
    print(f"Data exported successfully to: {output_file}")
    
except Exception as e:
    log(f"Data export failed: {str(e)}")
    print(f"Error: {str(e)}")
    
finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
