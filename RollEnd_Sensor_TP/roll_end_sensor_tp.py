
import psycopg2
from datetime import datetime, timedelta
from uuid import uuid4
import json
import time

class RollEndSensor_TP:
    def __init__(self, config_file):
       
        with open(config_file, 'r') as f:
            config = json.load(f)
            self.machines = config['machines']
            self.db_config = config['databases']
            self.settings = config['settings']


        self.source_conn = psycopg2.connect(**self.db_config['source'])
        self.source_cursor = self.source_conn.cursor()


        self.target_conn = psycopg2.connect(**self.db_config['target'])
        self.target_cursor = self.target_conn.cursor()


        self.state = {
            machine: {
                'roll_end_triggered': False,
                'rq_true_start_time': None,
                'previous_is_problem': None
            } for machine in self.machines
        }

    
        self.insert_event_query = f"""INSERT INTO {self.settings['event_table_schema']}.{self.settings['event_table_name']}
                                    (timestamp, event_id, zone, camera_id, event_type, alert_type) 
                                    VALUES (%s, %s, %s, %s, %s, %s);"""

    @staticmethod
    def format_event_data(is_problem):
        timestamp = datetime.now().isoformat()
        return {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": timestamp,
            "color_code": 3 if is_problem else 1
        }

    def insert_or_update_tp(self, table, column, event_data):
        check_query = f"SELECT {column} FROM {table};"
        update_query = f"UPDATE {table} SET {column} = %s;"
        insert_query = f"INSERT INTO {table} ({column}) VALUES (%s);"

        self.target_cursor.execute(check_query)
        exists = self.target_cursor.fetchone()

        if exists:
            print(f"Updating {table}.{column} with event data: {event_data}")
            self.target_cursor.execute(update_query, (json.dumps(event_data),))
        else:
            print(f"Inserting new {table}.{column} with event data: {event_data}")
            self.target_cursor.execute(insert_query, (json.dumps(event_data),))

    def run(self):
        print("Code Started for Roll End Monitoring (Dual Databases)")
        while True:
            for machine, params in self.machines.items():
                try:
     
                    fetch_mq_query = f"SELECT status FROM {params['mq_table']} ORDER BY timestamp DESC LIMIT 1;"
                    self.source_cursor.execute(fetch_mq_query)
                    mq_row = self.source_cursor.fetchone()
                    mq = int(mq_row[0]) if mq_row else None

                    fetch_rq_query = f"""
                        SELECT timestamp, {machine.lower()} ->> '{params['rq_field']}' AS Roll_end
                        FROM {params['rq_table']}
                        WHERE {machine.lower()} ->> '{params['rq_field']}' IS NOT NULL
                        ORDER BY timestamp DESC
                        LIMIT 2;
                    """
                    self.source_cursor.execute(fetch_rq_query)
                    rq_row = self.source_cursor.fetchone()
                    rq = 1 if rq_row and rq_row[1].lower() == 'true' else 0 if rq_row else None

                    # Roll-end detection
                    if mq == 1 and rq == 0:
                        is_problem = True
                        self.state[machine]['roll_end_triggered'] = True
                        self.state[machine]['rq_true_start_time'] = None
                    elif mq == 1 and rq == 1:
                        is_problem = False
                    else:
                        is_problem = False

            
                    if is_problem != self.state[machine]['previous_is_problem']:
                        event_data = self.format_event_data(is_problem)
                        if is_problem:
                            data = (
                                datetime.now(), str(uuid4()), params['zone'],
                                params['camera_id'], params['roll_end_event'], params['alert_type']
                            )
                            self.target_cursor.execute(self.insert_event_query, data)
                            print(f"Inserting roll-end event for {machine}: {data}")
                            self.target_conn.commit()

                            self.insert_or_update_tp(params['tp_status_table'], params['tp_roll_end_column'], event_data)
                            self.target_conn.commit()

                        self.state[machine]['previous_is_problem'] = is_problem

                    # New roll detection
                    if self.state[machine]['roll_end_triggered'] and mq == 1 and rq == 1:
                        if self.state[machine]['rq_true_start_time'] is None:
                            self.state[machine]['rq_true_start_time'] = datetime.now()
                            print(f"Started tracking rq == 1 for new laminate roll on {machine}...")

                        elif datetime.now() - self.state[machine]['rq_true_start_time'] >= timedelta(minutes=self.settings['new_roll_timeout_minutes']):
                            event_data = self.format_event_data(is_problem=False)
                            data = (
                                datetime.now(), str(uuid4()), params['zone'],
                                params['camera_id'], params['new_roll_event'], params['alert_type']
                            )
                            print(f"Inserting new roll event for {machine}: {data}")
                            self.target_cursor.execute(self.insert_event_query, data)
                            self.insert_or_update_tp(params['tp_status_table'], params['tp_new_roll_column'], event_data)
                            self.target_conn.commit()

                            self.state[machine]['roll_end_triggered'] = False
                            self.state[machine]['rq_true_start_time'] = None

                    elif rq != 1:
                        self.state[machine]['rq_true_start_time'] = None

                except Exception as e:
                    print(f"Error processing {machine}: {e}")
                    self.source_conn.rollback()
                    self.target_conn.rollback()
                    continue

            time.sleep(self.settings['poll_interval_seconds'])

    def __del__(self):
        if hasattr(self, 'source_cursor'):
            self.source_cursor.close()
        if hasattr(self, 'source_conn'):
            self.source_conn.close()
        if hasattr(self, 'target_cursor'):
            self.target_cursor.close()
        if hasattr(self, 'target_conn'):
            self.target_conn.close()

if __name__ == '__main__':
    RollEndSensor_TP('roll_end_sensor_tp.json').run()

