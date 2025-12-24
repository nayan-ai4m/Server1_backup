import psycopg2
from uuid import uuid4
import json
import time
from datetime import datetime, timedelta

class JAMMING_TP:
    def __init__(self):
        # Connection for reading jamming data (original short_data_hul)
        self.conn = psycopg2.connect(
            host="localhost",
            dbname="short_data_hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor = self.conn.cursor()

        # Connection for event_table (host: 100.96.244.68)
        self.conn_events = psycopg2.connect(
            host="localhost",
            dbname="hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor_events = self.conn_events.cursor()

        # Connection for tp_status table (host: 100.103.195.124)
        self.conn_tp = psycopg2.connect(
            host="192.168.1.168",
            dbname="hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor_tp = self.conn_tp.cursor()

        self.machines = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22','mc25','mc26','mc27','mc30']

        self.insert_event_query = """INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s);"""

        for machine in self.machines:
            tp_table = f"{machine}_tp_status"
            self.ensure_tp_column_exists(tp_table, "tp54")
            self.ensure_tp_column_exists(tp_table, "tp55")

        # Store per-machine data
        self.machine_data = {
            machine: {
                'prev_flag': None,
                'prev_reason': None,
                'refer_timestamp': None,
                'check_time': datetime.now(),
                'last_active_timestamp': None
            } for machine in self.machines
        }

    def ensure_tp_column_exists(self, tp_table, tp_column):
        """
        Checks if the given column exists in the specified tp_status table and adds it only if it does not exist.

        Args:
            tp_table (str): Name of the table (e.g., 'mc17_tp_status').
            tp_column (str): Name of the column to check or add (e.g., 'tp54').
        """
        try:
            # Check if the column exists in the table
            self.cursor_tp.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = %s
                    AND column_name = %s
                );
                """, (tp_table, tp_column)
            )
            column_exists = self.cursor_tp.fetchone()[0]

            if not column_exists:
                print(f"Column '{tp_column}' does not exist in table '{tp_table}'. Creating it...")
                #self.cursor_tp.execute(f"ALTER TABLE {tp_table} ADD COLUMN {tp_column} JSONB;")
                #self.conn_tp.commit()
                #print(f"Column '{tp_column}' created successfully in table '{tp_table}'.")
            else:
                print(f"Column '{tp_column}' already exists in table '{tp_table}'. No action needed.")
        except psycopg2.Error as e:
            print(f"Error checking/creating column '{tp_column}' in table '{tp_table}': {e}")
            self.conn_tp.rollback()

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
        print("Started Code to check Laminate Jamming for machines: " + ', '.join([m.upper() for m in self.machines]))

        while True:
            try:
                for machine in self.machines:
                    md = self.machine_data[machine]
                    prev_flag = md['prev_flag']
                    prev_reason = md['prev_reason']
                    refer_timestamp = md['refer_timestamp']
                    check_time = md['check_time']

                    jamming_table = 'jamming_data' if machine == 'mc17' else f'jamming_data_{machine[2:]}'
                    fetch_jamming_query = f'''SELECT timestamp, reason FROM {jamming_table} ORDER BY timestamp DESC LIMIT 1;'''

                    self.cursor.execute(fetch_jamming_query)
                    jamming_row = self.cursor.fetchone()
                    print(f"Query executed for {machine}: {fetch_jamming_query}")
                    print(f"Jamming row: {jamming_row}")

                    if jamming_row and jamming_row[1] is not None:
                        jamming = jamming_row[1]
                        current_timestamp = jamming_row[0]
                    else:
                        jamming = None
                        current_timestamp = None

                    if jamming is not None:
                        if jamming == "Current avg > 2.0A":
                            is_problem = True
                            flag = "continuous"
                        elif jamming == "Current avg > Baseline avg + 1.0A & Current avg > 2.0A":
                            is_problem = True
                            flag = "abrupt"
                        else:
                            is_problem = None
                            flag = None
                    else:
                        is_problem = None
                        flag = None

                    # Determine if this is a new row (based on timestamp change)
                    is_new = current_timestamp is not None and (refer_timestamp is None or current_timestamp != refer_timestamp)

                    if is_new and flag is not None:
                        # Check if reason is different or if 5 minutes have passed
                        elapsed = datetime.now() - check_time if prev_flag is not None else timedelta(minutes=6)
                        if prev_reason != jamming or elapsed >= timedelta(minutes=5):
                            md['refer_timestamp'] = current_timestamp
                            md['prev_reason'] = jamming

                            # New jamming event - update to active=1 with new timestamp/uuid
                            event_data = self.format_event_data(is_problem=True)

                            tp_table = f"{machine}_tp_status"
                            camera_id = machine.upper()

                            md['last_active_timestamp'] = datetime.now().isoformat()
                            if flag == "continuous":
                                event_type = "Continuous Laminate Jamming"
                            elif flag == "abrupt":
                                event_type = "Abrupt Laminate Jamming"
                            data = (
                                datetime.now(), str(uuid4()), "PLC",
                                camera_id, event_type, "Quality"
                            )
                            print(data)
                            # Insert event data into event_table
                            self.cursor_events.execute(self.insert_event_query, data)
                            self.conn_events.commit()

                            # Update tp_status
                            tp_column = "tp54" if flag == "continuous" else "tp55"
                            check_tp_exists_query = f"SELECT {tp_column} FROM {tp_table};"
                            update_tp_query = f"UPDATE {tp_table} SET {tp_column} = %s;"
                            insert_tp_query = f"INSERT INTO {tp_table} ({tp_column}) VALUES (%s);"

                            self.cursor_tp.execute(check_tp_exists_query)
                            exists = self.cursor_tp.fetchone()
                            if exists:
                                print(f"Updating {tp_column} for {machine} with event data: {event_data}")
                                self.cursor_tp.execute(update_tp_query, (json.dumps(event_data),))
                            else:
                                print(f"Inserting new {tp_column} for {machine} with event data: {event_data}")
                                self.cursor_tp.execute(insert_tp_query, (json.dumps(event_data),))

                            self.conn_tp.commit()
                            md['prev_flag'] = flag
                            md['check_time'] = datetime.now()

                    # Timeout check (separate from new row detection)
                    if prev_flag is not None:
                        elapsed = datetime.now() - check_time
                        if elapsed >= timedelta(minutes=5):
                            print(f"Previous Event for {machine} occurred for 5 Mins")
                            event_data = self.format_event_data(is_problem=False, last_active_timestamp=md['last_active_timestamp'])

                            tp_table = f"{machine}_tp_status"
                            tp_column = "tp54" if prev_flag == "continuous" else "tp55"
                            check_tp_exists_query = f"SELECT {tp_column} FROM {tp_table};"
                            update_tp_query = f"UPDATE {tp_table} SET {tp_column} = %s;"
                            insert_tp_query = f"INSERT INTO {tp_table} ({tp_column}) VALUES (%s);"

                            self.cursor_tp.execute(check_tp_exists_query)
                            exists = self.cursor_tp.fetchone()
                            if exists:
                                print(f"Clearing previous event in {tp_column} for {machine} with event data: {event_data}")
                                self.cursor_tp.execute(update_tp_query, (json.dumps(event_data),))
                            else:
                                print(f"Inserting new {tp_column} for {machine} with event data: {event_data}")
                                self.cursor_tp.execute(insert_tp_query, (json.dumps(event_data),))

                            self.conn_tp.commit()
                            md['prev_flag'] = None
                            md['prev_reason'] = None

                time.sleep(5)

            except psycopg2.Error as db_err:
                print(f"Database error: {db_err}")
                # Rollback all connections on database error
                for conn in [self.conn, self.conn_events, self.conn_tp]:
                    try:
                        conn.rollback()
                    except:
                        pass
                time.sleep(5)
            except Exception as ex:
                print(f"Unexpected error: {ex}")
                time.sleep(5)

    def __del__(self):
        # Close all connections and cursors
        for attr in ('cursor_tp', 'cursor_events', 'cursor', 'conn_tp', 'conn_events', 'conn'):
            if hasattr(self, attr):
                try:
                    getattr(self, attr).close()
                except:
                    pass

if __name__ == '__main__':
    jamming_tp = JAMMING_TP()
    jamming_tp.run()
