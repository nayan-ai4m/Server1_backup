import psycopg2
from datetime import datetime, timedelta
from uuid import uuid4
import json
import time

class RollEndSensor_TP:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            dbname="hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor = self.conn.cursor()

        self.fetch_mq_query = "SELECT status FROM mc17 ORDER BY timestamp DESC LIMIT 1;"
        self.fetch_rq_query = '''
            SELECT timestamp, mc17 ->> 'ROLL_END_SENSOR' AS Roll_end
            FROM loop3_checkpoints
            WHERE mc17 ->> 'ROLL_END_SENSOR' IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1;
        '''
        self.insert_event_query = """INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s);"""

        # For tracking new laminate roll condition
        self.roll_end_triggered = False
        self.rq_true_start_time = None
        self.last_active_one_timestamp = None  # Store timestamp when active=1

    @staticmethod
    def format_event_data(is_problem, last_active_one_timestamp=None):
        current_time = datetime.now()
        timestamp = current_time.isoformat() if is_problem else last_active_one_timestamp.isoformat() if last_active_one_timestamp else current_time.isoformat()
        event_data = {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": timestamp,
            "color_code": 3 if is_problem else 1
        }
        if not is_problem and last_active_one_timestamp:
            event_data["updated_time"] = current_time.isoformat()
        return event_data

    def insert_or_update_tp(self, column, event_data):
        check_query = f"SELECT {column} FROM mc17_tp_status;"
        update_query = f"UPDATE mc17_tp_status SET {column} = %s;"
        insert_query = f"INSERT INTO mc17_tp_status ({column}) VALUES (%s);"

        self.cursor.execute(check_query)
        exists = self.cursor.fetchone()

        if exists:
            print(f"Updating {column} with event data: {event_data}")
            self.cursor.execute(update_query, (json.dumps(event_data),))
        else:
            print(f"Inserting new {column} with event data: {event_data}")
            self.cursor.execute(insert_query, (json.dumps(event_data),))

    def run(self):
        print("Code Started for MC17 Roll End")
        previous_is_problem = None

        while True:
            self.cursor.execute(self.fetch_mq_query)
            mq_row = self.cursor.fetchone()
            mq = int(mq_row[0]) if mq_row else None

            self.cursor.execute(self.fetch_rq_query)
            rq_row = self.cursor.fetchone()
            if rq_row and rq_row[1] is not None:
                rq = 1 if rq_row[1].lower() == 'true' else 0
            else:
                rq = None

            # Detect "roll end" condition
            if mq == 1 and rq == 0:
                is_problem = True
                self.roll_end_triggered = True
                self.rq_true_start_time = None
            elif mq == 1 and rq == 1:
                is_problem = False
            else:
                is_problem = False

            # Event for roll ending
            if is_problem is not None and is_problem != previous_is_problem:
                event_data = self.format_event_data(is_problem, self.last_active_one_timestamp)

                if is_problem:
                    self.last_active_one_timestamp = datetime.now()  # Store timestamp when active=1
                    data = (
                        self.last_active_one_timestamp, str(uuid4()), "PLC", 
                        "MC17", "Laminate roll about to end ", "quality"
                    )
                    self.cursor.execute(self.insert_event_query, data)
                    print("Inserting roll-end event:", data)
                    self.conn.commit()

                    self.insert_or_update_tp("tp50", event_data)
                    self.conn.commit()

            # Check for new laminate roll condition
            if self.roll_end_triggered and mq == 1 and rq == 1:
                if self.rq_true_start_time is None:
                    self.rq_true_start_time = datetime.now()
                    print("Started tracking rq == 1 for new laminate roll...")

                elif datetime.now() - self.rq_true_start_time >= timedelta(minutes=1):
                    event_data = self.format_event_data(is_problem=False, last_active_one_timestamp=self.last_active_one_timestamp)
                    data = (
                        self.last_active_one_timestamp if self.last_active_one_timestamp else datetime.now(), 
                        str(uuid4()), "PLC", 
                        "MC17", "New Laminate Roll added ", "quality"
                    )
                    print("Inserting new roll event:", data)
                    self.cursor.execute(self.insert_event_query, data)
                    self.insert_or_update_tp("tp72", event_data)
                    self.conn.commit()

                    # Reset the trigger
                    self.roll_end_triggered = False
                    self.rq_true_start_time = None

            elif rq != 1:
                self.rq_true_start_time = None  # reset timer if condition breaks

            time.sleep(5)

    def __del__(self):
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()

if __name__ == '__main__':
    RollEndSensor_TP().run()

