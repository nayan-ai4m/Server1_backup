import psycopg2
from datetime import datetime
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
        self.check_tp_exists_query = "SELECT tp50 FROM mc17_tp_status;"
        self.update_tp_query = f"UPDATE mc17_tp_status SET tp50 = %s;"
        self.insert_tp_query = "INSERT INTO mc17_tp_status (tp50) VALUES (%s);"

        self.insert_event_query = """INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s);"""

    @staticmethod
    def format_event_data(is_problem):
        timestamp = datetime.now().isoformat()
        return {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": timestamp,
            "color_code": 3 if is_problem else 1
        }
    
    def run(self):
        previous_is_problem = None  

        while True:
            self.cursor.execute(self.fetch_mq_query)
            mq_row = self.cursor.fetchone()
            #print(mq_row[0])
            mq = int(mq_row[0]) if mq_row else None

            self.cursor.execute(self.fetch_rq_query)
            rq_row = self.cursor.fetchone()
            #rq = int(bool(rq_row[1])) if rq_row and rq_row[1] is not None else None
            if rq_row and rq_row[1] is not None:
                rq = 1 if rq_row[1].lower() == 'true' else 0
            else:
                rq = None
            
            print(f"mq={mq}, rq={rq}")

            # tp_name = "tp50"
            
            if mq == 1 and rq == 0:
                is_problem = True
            elif mq == 1 and rq == 1:
                is_problem = False
            else:
                is_problem = False # None

            # Only update if is_problem changes
            if is_problem is not None and is_problem != previous_is_problem:
                event_data = self.format_event_data(is_problem=is_problem)

                if is_problem == True:
                    data = (
                        datetime.now(), str(uuid4()), "PLC", 
                        "MC17", "Laminate roll about to end ", "Quality"
                    )
                    print(data)
                    self.cursor.execute(self.insert_event_query, data)
                    self.conn.commit()

                # Check if tp<n> exists
                self.cursor.execute(self.check_tp_exists_query)
                exists = self.cursor.fetchone()

                if exists:
                    print(f"Updating tp50 with event data: {event_data}")
                    self.cursor.execute(self.update_tp_query, (json.dumps(event_data),))
                else:
                    print(f"Inserting new tp50 with event data: {event_data}")
                    self.cursor.execute(self.insert_tp_query, (json.dumps(event_data),))

                self.conn.commit()

                previous_is_problem = is_problem

            time.sleep(5)


    def __del__(self):
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()

if __name__ == '__main__':
    RollEndSensor_TP().run()
