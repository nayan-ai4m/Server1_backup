import psycopg2
from uuid import uuid4
import json
import time
from datetime import datetime, timedelta

class COMBINED_ALERT:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            dbname="short_data_hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor = self.conn.cursor()

        self.conn1 = psycopg2.connect(
            host="192.168.1.168",
            dbname="hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor1 = self.conn1.cursor()

        self.conn2 = psycopg2.connect(
            host="localhost",
            dbname="hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor2 = self.conn2.cursor()

        self.machines = [17, 18, 19, 20, 21, 22]

        self.previous_combined_status = {}
        self.check_times = {}
        self.last_alert_types = {}

        for machine in self.machines:
            self.previous_combined_status[machine] = None
            self.check_times[machine] = datetime.now()
            self.last_alert_types[machine] = None

        self.fetch_jamming_query = '''SELECT timestamp, reason FROM jamming_data ORDER BY timestamp DESC LIMIT 1;'''

        self.fetch_sync_query = '''
            SELECT vertical_horizontal_sync_avg, pulling_horizontal_sync_avg
            FROM sync_data
            ORDER BY timestamp DESC
            LIMIT 1;
        '''

        self.insert_event_query = """
            INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type)
            VALUES (%s, %s, %s, %s, %s, %s);
        """

        for machine in self.machines:
            self.ensure_tp_column_exists(f"tp65", machine)

    def ensure_tp_column_exists(self, tp_column, machine):
        table_name = f"mc{machine}_tp_status"
        try:
            self.cursor1.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name=%s AND column_name=%s;
            """, (table_name, tp_column))
            column_exists = self.cursor1.fetchone()
            if not column_exists:
                print(f"Column '{tp_column}' does not exist in {table_name}. Creating it...")
                self.cursor1.execute(f"ALTER TABLE {table_name} ADD COLUMN {tp_column} JSONB;")
                self.conn1.commit()
                print(f"Column '{tp_column}' created successfully in {table_name}.")
        except Exception as e:
            print(f"Error checking/creating column '{tp_column}' in {table_name}: {e}")
            self.conn1.rollback()

    @staticmethod
    def format_event_data(is_problem):
        timestamp = datetime.now().isoformat()
        return {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": timestamp,
            "color_code": 3 if is_problem else 1
        }

    def update_tp_status(self, machine, event_data):
        table_name = f"mc{machine}_tp_status"
        check_query = f"SELECT tp65 FROM {table_name};"
        update_query = f"UPDATE {table_name} SET tp65 = %s;"
        insert_query = f"INSERT INTO {table_name} (tp65) VALUES (%s);"

        try:
            self.cursor1.execute(check_query)
            exists = self.cursor1.fetchone()

            if exists:
                print(f"Updating tp65 in {table_name} with event data: {event_data}")
                self.cursor1.execute(update_query, (json.dumps(event_data),))
            else:
                print(f"Inserting new tp65 in {table_name} with event data: {event_data}")
                self.cursor1.execute(insert_query, (json.dumps(event_data),))
        except Exception as e:
            print(f"Error updating {table_name}: {e}")
            raise

    def check_jamming_condition(self, machine):
        if machine == 17:
            jamming_query = '''SELECT timestamp, reason FROM jamming_data ORDER BY timestamp DESC LIMIT 1;'''
        else:
            jamming_query = f'''SELECT timestamp, reason FROM jamming_data_{machine} ORDER BY timestamp DESC LIMIT 1;'''

        self.cursor.execute(jamming_query)
        jamming_row = self.cursor.fetchone()

        if jamming_row and jamming_row[1] is not None:
            jamming = jamming_row[1]
            current_timestamp = jamming_row[0]
            if jamming == "Current avg > Baseline avg + 1A & Current avg > 2A":
                return True, current_timestamp
        return False, None

    def check_sync_condition(self, machine):
        if machine == 17:
            sync_query = '''
                SELECT vertical_horizontal_sync_avg, pulling_horizontal_sync_avg
                FROM sync_data
                ORDER BY timestamp DESC
                LIMIT 1;
            '''
        else:
            sync_query = f'''
                SELECT vertical_horizontal_sync_avg, pulling_horizontal_sync_avg
                FROM sync_data_{machine}
                ORDER BY timestamp DESC
                LIMIT 1;
            '''

        self.cursor.execute(sync_query)
        sync_row = self.cursor.fetchone()

        if sync_row and sync_row[0] is not None:
            vhs = float(sync_row[0])
            phs = float(sync_row[1])

            if vhs < 70:
                return True, "Vertical and Horizontal axes out of sync"
            elif phs > 40:
                return True, "Pulling and Horizontal axes out of sync"

        return False, None

    def process_machine(self, machine):
        """Process alerts for a specific machine"""
        try:
            jamming_status, jamming_timestamp = self.check_jamming_condition(machine)
            sync_status, sync_event_type = self.check_sync_condition(machine)

            combined_status = jamming_status or sync_status
            camera_id = f"MC{machine}"

            if combined_status != self.previous_combined_status[machine]:
                event_data = self.format_event_data(is_problem=combined_status)

                if combined_status:
                    data = (
                        datetime.now(), str(uuid4()), "PLC",
                        camera_id, "Possibility of Paper breakage", "Breakdown"
                    )
                    print(f"[ALERT] {camera_id}: {data}")
                    self.cursor2.execute(self.insert_event_query, data)
                    self.conn2.commit()
                    self.last_alert_types[machine] = "Breakdown"
                else:
                    if self.previous_combined_status[machine]:
                        print(f"[RESOLVED] {camera_id}: Combined alert resolved (was: {self.last_alert_types[machine]})")

                self.update_tp_status(machine, event_data)
                self.conn1.commit()
                self.previous_combined_status[machine] = combined_status
                self.check_times[machine] = datetime.now()

            elif self.previous_combined_status[machine] and (datetime.now() - self.check_times[machine]) >= timedelta(minutes=5):
                print(f"{camera_id}: Alert has been active for 5 minutes - clearing")
                event_data = self.format_event_data(is_problem=False)
                self.update_tp_status(machine, event_data)
                self.conn1.commit()
                self.previous_combined_status[machine] = False
                self.check_times[machine] = datetime.now()

        except Exception as e:
            print(f"Error processing machine {machine}: {e}")
            raise

    def run(self):
        print(f"Starting monitoring for machines: {self.machines}")

        while True:
            try:
                for machine in self.machines:
                    self.process_machine(machine)

                time.sleep(0.2)

            except psycopg2.Error as db_err:
                print(f"Database error: {db_err}")
                self.conn.rollback()
                self.conn1.rollback()
                self.conn2.rollback()
                time.sleep(5)
            except Exception as ex:
                print(f"Unexpected error: {ex}")
                time.sleep(5)

    def __del__(self):
        for attr in ('cursor', 'conn', 'cursor1', 'conn1', 'cursor2', 'conn2'):
            if hasattr(self, attr):
                getattr(self, attr).close()

if __name__ == '__main__':
    COMBINED_ALERT().run()
