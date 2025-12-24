import psycopg2
from uuid import uuid4
import json
import time
from datetime import datetime, timedelta

class JAMMING_TP:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            dbname="short_data_hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor = self.conn.cursor()

        self.conn1 = psycopg2.connect(
            host="localhost",
            dbname="hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor1 = self.conn1.cursor()

        self.fetch_jamming_query = '''SELECT timestamp, reason FROM jamming_data ORDER BY timestamp DESC LIMIT 1;'''

        self.check_tp_exists_query_cont = "SELECT tp54 FROM mc17_tp_status;"
        self.update_tp_query_cont = "UPDATE mc17_tp_status SET tp54 = %s;"
        self.insert_tp_query_cont = "INSERT INTO mc17_tp_status (tp54) VALUES (%s);"

        self.check_tp_exists_query_abrupt = "SELECT tp55 FROM mc17_tp_status;"
        self.update_tp_query_abrupt = "UPDATE mc17_tp_status SET tp55 = %s;"
        self.insert_tp_query_abrupt = "INSERT INTO mc17_tp_status (tp55) VALUES (%s);"

        self.insert_event_query = """INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s);"""
        
        self.ensure_tp_column_exists("tp54")
        self.ensure_tp_column_exists("tp55")

        # Store the timestamp when active was last 1
        self.last_active_timestamp = None

    def ensure_tp_column_exists(self, tp_column):
        """
        Checks if the given column exists in mc17_tp_status and adds it if not.

        Args:
            tp_column (str): Name of the column to check or add (e.g., 'tp54').
        """
        try:
            self.cursor1.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name='mc17_tp_status' AND column_name=%s;
                """, (tp_column,)
            )
            column_exists = self.cursor1.fetchone()
            if not column_exists:
                print(f"Column '{tp_column}' does not exist. Creating it...")
                self.cursor1.execute(f"ALTER TABLE mc17_tp_status ADD COLUMN {tp_column} JSONB;")
                self.conn1.commit()
                print(f"Column '{tp_column}' created successfully.")
        except Exception as e:
            print(f"Error checking/creating column '{tp_column}': {e}")
            self.conn1.rollback()

    @staticmethod   
    def format_event_data(is_problem, last_active_timestamp=None):
        current_time = datetime.now().isoformat()
        event_data = {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": current_time if is_problem else last_active_timestamp,
            "color_code": 3 if is_problem else 1
        }
        if not is_problem and last_active_timestamp:
            event_data["updated_time"] = current_time
        return event_data
    
    def run(self):
        print("Started Code to check Laminate Jamming in MC17")
        prev_flag = None
        refer_timestamp = None
        check_time = datetime.now()

        while True:
            try:
                self.cursor.execute(self.fetch_jamming_query)
                jamming_row = self.cursor.fetchone()
                if jamming_row and jamming_row[1] is not None:
                    jamming = jamming_row[1] 
                    current_timestamp = jamming_row[0]
                else:
                    jamming = None
                    current_timestamp = None

                if jamming is not None:
                    if jamming == "Current avg > 2A":
                        is_problem = True
                        flag = "continuous"
                    elif jamming == "Current avg > Baseline avg + 1A & Current avg > 2":
                        is_problem = True
                        flag = "abrupt"
                    else:
                        is_problem = None
                        flag = None
                else:
                    is_problem = None
                    flag = None
                
                # Only update if is_problem changes
                if flag is not None and prev_flag != flag and current_timestamp is not None and refer_timestamp != current_timestamp:
                    event_data = self.format_event_data(is_problem=is_problem)
                    
                    if is_problem:
                        self.last_active_timestamp = datetime.now().isoformat()
                        if flag == "continuous":
                            data = (
                                datetime.now(), str(uuid4()), "PLC", 
                                "MC17", "Continuous Laminate Jamming", "Quality"
                            )
                        elif flag == "abrupt":
                            data = (
                                datetime.now(), str(uuid4()), "PLC", 
                                "MC17", "Abrupt Laminate Jamming", "Quality"
                            )

                        print(data)
                        self.cursor1.execute(self.insert_event_query, data)
                        self.conn1.commit()

                    if flag == "continuous":
                        self.cursor1.execute(self.check_tp_exists_query_cont)
                        exists = self.cursor1.fetchone()
                        if exists:
                            print(f"Updating tp54 with event data: {event_data}")
                            self.cursor1.execute(self.update_tp_query_cont, (json.dumps(event_data),))
                        else:
                            print(f"Inserting new tp54 with event data: {event_data}")
                            self.cursor1.execute(self.insert_tp_query_cont, (json.dumps(event_data),))

                    if flag == "abrupt":
                        self.cursor1.execute(self.check_tp_exists_query_abrupt)
                        exists = self.cursor1.fetchone()
                        if exists:
                            print(f"Updating tp55 with event data: {event_data}")
                            self.cursor1.execute(self.update_tp_query_abrupt, (json.dumps(event_data),))
                        else:
                            print(f"Inserting new tp55 with event data: {event_data}")
                            self.cursor1.execute(self.insert_tp_query_abrupt, (json.dumps(event_data),))
                    
                    self.conn1.commit()
                    prev_flag = flag
                    check_time = datetime.now()
                    refer_timestamp = current_timestamp
                
                elif prev_flag is not None and (prev_flag == flag or flag is None):
                    elapsed = datetime.now() - check_time
                    if elapsed >= timedelta(minutes=5):
                        print("Previous Event occurred for 5 Mins")
                        event_data = self.format_event_data(is_problem=False, last_active_timestamp=self.last_active_timestamp)
                        if prev_flag == "continuous":
                            self.cursor1.execute(self.check_tp_exists_query_cont)
                            exists = self.cursor1.fetchone()
                            if exists:
                                print(f"Clearing previous event in tp54 with event data: {event_data}")
                                self.cursor1.execute(self.update_tp_query_cont, (json.dumps(event_data),))
                            else:
                                print(f"Inserting new tp54 with event data: {event_data}")
                                self.cursor1.execute(self.insert_tp_query_cont, (json.dumps(event_data),))

                        if prev_flag == "abrupt":
                            self.cursor1.execute(self.check_tp_exists_query_abrupt)
                            exists = self.cursor1.fetchone()
                            if exists:
                                print(f"Clearing previous event in tp55 with event data: {event_data}")
                                self.cursor1.execute(self.update_tp_query_abrupt, (json.dumps(event_data),))
                            else:
                                print(f"Inserting new tp55 with event data: {event_data}")
                                self.cursor1.execute(self.insert_tp_query_abrupt, (json.dumps(event_data),))
                        
                        prev_flag = None
                        self.conn1.commit()

                time.sleep(5)

            except psycopg2.Error as db_err:
                print(f"Database error: {db_err}")
                if hasattr(self, 'conn'):
                    self.conn.rollback()
                if hasattr(self, 'conn1'):
                    self.conn1.rollback()
                time.sleep(5)
            except Exception as ex:
                print(f"Unexpected error: {ex}")
                time.sleep(5)

    def __del__(self):
        for attr in ('cursor1', 'cursor', 'conn1', 'conn'):
            if hasattr(self, attr):
                getattr(self, attr).close()

if __name__ == '__main__':
    jamming_tp = JAMMING_TP()
    jamming_tp.run()
