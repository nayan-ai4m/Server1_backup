import psycopg2
from datetime import datetime
from uuid import uuid4
import json
import time

class RollEndSensor_TP:
    def __init__(self):
        # Connection for touchpoint database (mc17_tp_status)
        self.tp_conn = psycopg2.connect(
            host="192.168.1.168",
            dbname="hul",
            user="postgres",
            password="ai4m2024"
        )
        self.tp_cursor = self.tp_conn.cursor()

        # Connection for event database (event_table)
        self.event_conn = psycopg2.connect(
            host="192.168.1.149",  
            dbname="hul",   
            user="postgres",     
            password="ai4m2024"  
        )
        self.event_cursor = self.event_conn.cursor()

        self.fetch_mq_query = "SELECT status FROM mc17 ORDER BY timestamp DESC LIMIT 1;"
        self.fetch_ls_query = '''
            SELECT 
                CASE 
                    WHEN AVG(
                        CASE 
                            WHEN leaping_value = 'true' THEN 1
                            WHEN leaping_value = 'false' THEN 0
                            ELSE NULL 
                        END
                    ) > 0.5 THEN 1
                    ELSE 0
                END AS leaping_result
            FROM (
                SELECT mc17 ->> 'LEAPING_SENSOR' AS leaping_value
                FROM loop3_checkpoints
                WHERE mc17 ->> 'LEAPING_SENSOR' IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 10
            ) subquery;
        '''
        self.check_tp_exists_query = "SELECT tp51 FROM mc17_tp_status;"
        self.update_tp_query = "UPDATE mc17_tp_status SET tp51 = %s;"
        self.insert_tp_query = "INSERT INTO mc17_tp_status (tp51) VALUES (%s);"
        self.insert_event_query = """INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s);"""

        self.last_active_timestamp = None
        self.ensure_tp51_column_exists()

    def ensure_tp51_column_exists(self):
        """Checks if the column tp51 exists in mc17_tp_status and adds it if not."""
        try:
            self.tp_cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='mc17_tp_status' AND column_name='tp51';
            """)
            column_exists = self.tp_cursor.fetchone()
            if not column_exists:
                print("Column 'tp51' does not exist. Creating it...")
                self.tp_cursor.execute("ALTER TABLE mc17_tp_status ADD COLUMN tp51 JSONB;")
                self.tp_conn.commit()
                print("Column 'tp51' created successfully.")
        except Exception as e:
            print(f"Error checking/creating column 'tp51': {e}")
            self.tp_conn.rollback()

    def format_event_data(self, is_problem):
        current_time = datetime.now()
        # Use current_time for active=1, last_active_timestamp for active=0
        timestamp = current_time if is_problem else (self.last_active_timestamp or current_time)
        event_data = {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": timestamp.isoformat(),
            "color_code": 3 if is_problem else 1
        }
        if not is_problem and self.last_active_timestamp is not None:
            event_data["updated_time"] = current_time.isoformat()
        return event_data, timestamp

    def run(self):
        print("Started Code for Leaping Sensor for MC17 (baumer)")
        previous_is_problem = None  

        while True:
            try:
                # Fetch mq from touchpoint database
                self.tp_cursor.execute(self.fetch_mq_query)
                mq_row = self.tp_cursor.fetchone()
                mq = int(mq_row[0]) if mq_row else None

                # Fetch ls from touchpoint database
                self.tp_cursor.execute(self.fetch_ls_query)
                ls_row = self.tp_cursor.fetchone()
                ls = int(ls_row[0]) if ls_row and ls_row[0] is not None else None

                print(f"mq={mq}, ls={ls}")

                if mq == 1 and ls == 0:
                    is_problem = True
                elif mq == 1 and ls == 1:
                    is_problem = False
                else:
                    is_problem = False

                # Only update if is_problem changes
                if is_problem is not None and is_problem != previous_is_problem:
                    # Update last_active_timestamp when active becomes 1
                    if is_problem:
                        self.last_active_timestamp = datetime.now()
                    event_data, event_timestamp = self.format_event_data(is_problem=is_problem)

                    # Insert event into event_table in event database
                    if is_problem:
                        data = (
                            event_timestamp, str(uuid4()), "PLC", 
                            "MC17", "Leaping Detected", "Quality"
                        )
                        print(f"Inserting event: {data}")
                        self.event_cursor.execute(self.insert_event_query, data)
                        self.event_conn.commit()

                    # Check if tp51 exists in touchpoint database
                    self.tp_cursor.execute(self.check_tp_exists_query)
                    exists = self.tp_cursor.fetchone()

                    if exists:
                        print(f"Updating tp51 with event data: {event_data}")
                        self.tp_cursor.execute(self.update_tp_query, (json.dumps(event_data),))
                    else:
                        print(f"Inserting new tp51 with event data: {event_data}")
                        self.tp_cursor.execute(self.insert_tp_query, (json.dumps(event_data),))
                    self.tp_conn.commit()

                    previous_is_problem = is_problem

                time.sleep(5)

            except psycopg2.Error as db_err:
                print(f"Database error: {db_err}")
                self.tp_conn.rollback()
                self.event_conn.rollback()
                time.sleep(5)
            except Exception as ex:
                print(f"Unexpected error: {ex}")
                time.sleep(5)

    def __del__(self):
        if hasattr(self, 'tp_cursor'):
            self.tp_cursor.close()
        if hasattr(self, 'tp_conn'):
            self.tp_conn.close()
        if hasattr(self, 'event_cursor'):
            self.event_cursor.close()
        if hasattr(self, 'event_conn'):
            self.event_conn.close()

if __name__ == '__main__':
    RollEndSensor_TP().run()
