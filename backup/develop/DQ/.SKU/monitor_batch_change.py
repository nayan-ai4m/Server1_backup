import psycopg2
from datetime import datetime

# Placeholder for database connection
def get_db_connection():
    return psycopg2.connect(
        dbname="hul",
        user="postgres",
        password="ai4m2024",
        host="192.168.1.149",
        port="5432"
    )

# SQL query to get the timestamp of the latest batch change
batch_change_query = """
SELECT timestamp, batch_status, batch_no, primary_tank, secondary_tank
FROM (
    SELECT timestamp, batch_no, primary_tank, secondary_tank,
           CASE
               WHEN LAG(batch_no, 1) OVER (ORDER BY timestamp DESC) != batch_no THEN 'change'
               ELSE 'nochange'
           END AS batch_status
    FROM loop3_sku 
    WHERE timestamp > CURRENT_DATE 
    ORDER BY timestamp DESC
) sub
WHERE batch_status = 'change'
LIMIT 1;
"""

def get_tank_levels(start_timestamp, primary_tank):
    level_query = """
    select time_bucket('3 minutes',timestamp) as timebucket ,
    ROUND(avg(primary_tank)::numeric) as primary,
    ROUND(avg(secondary_tank)::numeric) as secondary
    from loop3_sku 
    where timestamp > '{}'
    group by timebucket
    order by timebucket asc
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(level_query.format(start_timestamp.strftime('%Y-%m-%d %H:%M:%S')))
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

def track_level_increase():
    # Get the latest batch change timestamp and primary tank
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(batch_change_query)
        batch_result = cursor.fetchone()
        if not batch_result:
            print("No batch change found for today.")
            return
        
        start_timestamp, _, _, primary_tank, _ = batch_result
        print(f"Batch change detected at {start_timestamp} for primary tank {primary_tank}")
        
        # Get tank level data
        levels = get_tank_levels(start_timestamp, primary_tank)
        if not levels:
            print(f"No level data found for tank {primary_tank} after {start_timestamp}")
            return
        
        # Initialize variables
        start_level = levels[0][1]  # First level
        current_level = start_level
        max_level = start_level
        
        # Track level increase
        for _, level,_ in levels[1:]:
            if level > current_level:
                current_level = level
                max_level = level
            else:
                # Level stopped increasing
                break
        
        print(f"Start level: {start_level}")
        print(f"End level: {max_level}")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    track_level_increase()

