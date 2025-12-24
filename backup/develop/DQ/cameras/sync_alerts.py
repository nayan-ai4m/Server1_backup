import psycopg2
from datetime import datetime
from uuid import uuid4
import json
import time

class SYNCHRONIZATION_TP:
    def __init__(self):
        # Connection for sync_data (host: 100.96.244.68, database: short_data_hul)
        self.conn_sync = psycopg2.connect(
            host="100.96.244.68",
            dbname="short_data_hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor_sync = self.conn_sync.cursor()

        # Connection for event_table (host: 100.96.244.68, database: hul)
        self.conn_event = psycopg2.connect(
            host="100.96.244.68",
            dbname="hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor_event = self.conn_event.cursor()

        # Connection for tp_status tables (host: 192.168.1.168, database: hul)
        self.conn_tp = psycopg2.connect(
            host="192.168.1.168",
            dbname="hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor_tp = self.conn_tp.cursor()

        self.machines = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']

        self.insert_event_query = """
            INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type)
            VALUES (%s, %s, %s, %s, %s, %s);
        """

        for machine in self.machines:
            tp_table = f"{machine}_tp_status"
            for tp in ["tp56", "tp57", "tp58", "tp59"]:
                self.ensure_tp_column_exists(tp_table, tp)

        # Initialize per-machine state
        self.machine_states = {}
        for machine in self.machines:
            self.machine_states[machine] = {
                'previous_flags_triggered': set(),
                'last_active_timestamps': {
                    "vhs_sync": None,
                    "vrs_sync": None,
                    "frs_sync": None,
                    "phs_sync": None
                }
            }

    def ensure_tp_column_exists(self, tp_table, tp_column):
        try:
            self.cursor_tp.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name=%s AND column_name=%s;
            """, (tp_table, tp_column))
            column_exists = self.cursor_tp.fetchone()
            if not column_exists:
                print(f"Column '{tp_column}' does not exist in '{tp_table}'. Creating it...")
                self.cursor_tp.execute(f"ALTER TABLE {tp_table} ADD COLUMN {tp_column} JSONB;")
                self.conn_tp.commit()
                print(f"Column '{tp_column}' created successfully in '{tp_table}'.")
        except Exception as e:
            print(f"Error checking/creating column '{tp_column}' in '{tp_table}': {e}")
            self.conn_tp.rollback()

    def format_event_data(self, is_problem, flag, last_active_timestamps):
        current_time = datetime.now()
        # Use current_time for active=1, last_active_timestamp for active=0
        timestamp = current_time if is_problem else (last_active_timestamps[flag] or current_time)
        event_data = {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": timestamp.isoformat(),
            "color_code": 3 if is_problem else 1
        }
        if not is_problem and last_active_timestamps[flag] is not None:
            event_data["updated_time"] = current_time.isoformat()
        return event_data, timestamp

    def update_tp_column(self, tp_table, column, event_data):
        check_query = f"SELECT {column} FROM {tp_table};"
        update_query = f"UPDATE {tp_table} SET {column} = %s;"
        insert_query = f"INSERT INTO {tp_table} ({column}) VALUES (%s);"

        self.cursor_tp.execute(check_query)
        exists = self.cursor_tp.fetchone()

        if exists:
            print(f"Updating {column} in {tp_table} with event data: {event_data}")
            self.cursor_tp.execute(update_query, (json.dumps(event_data),))
        else:
            print(f"Inserting new {column} in {tp_table} with event data: {event_data}")
            self.cursor_tp.execute(insert_query, (json.dumps(event_data),))

    def run(self):
        print("Started Code for Synchronization Alerts for machines: " + ', '.join([m.upper() for m in self.machines]))

        while True:
            try:
                for machine in self.machines:
                    state = self.machine_states[machine]
                    previous_flags_triggered = state['previous_flags_triggered']
                    last_active_timestamps = state['last_active_timestamps']

                    sync_table = 'sync_data' if machine == 'mc17' else f'sync_data_{machine[2:]}'
                    fetch_sync_query = f'''
                        SELECT vertical_horizontal_sync_avg, vertical_rotational_sync_avg,
                               filling_rotational_sync_avg, pulling_horizontal_sync_avg
                        FROM {sync_table}
                        ORDER BY timestamp DESC
                        LIMIT 1;
                    '''

                    self.cursor_sync.execute(fetch_sync_query)
                    sync_row = self.cursor_sync.fetchone()

                    if sync_row and sync_row[0] is not None:
                        vhs = float(sync_row[0])
                        vrs = float(sync_row[1])
                        frs = float(sync_row[2])
                        phs = float(sync_row[3])
                    else:
                        vhs = vrs = frs = phs = None

                    #print(f"{machine}: vhs={vhs}, vrs={vrs}, frs={frs}, phs={phs}")

                    flags_triggered = []

                    if vhs is not None and vhs < 70:
                        flags_triggered.append("vhs_sync")
                    if vrs is not None and vrs < 70:
                        flags_triggered.append("vrs_sync")
                    if frs is not None and frs > 40:
                        flags_triggered.append("frs_sync")
                    if phs is not None and phs > 40:
                        flags_triggered.append("phs_sync")

                    flags_triggered_set = set(flags_triggered)

                    # Identify newly triggered and resolved flags
                    new_flags = flags_triggered_set - previous_flags_triggered
                    resolved_flags = previous_flags_triggered - flags_triggered_set

                    tp_table = f"{machine}_tp_status"
                    camera_id = machine.upper()

                    # Process new alerts
                    for flag in new_flags:
                        event_data, event_timestamp = self.format_event_data(is_problem=True, flag=flag, last_active_timestamps=last_active_timestamps)
                        last_active_timestamps[flag] = datetime.now()  # Update timestamp for active=1

                        if flag == "vhs_sync":
                            data = (event_timestamp, str(uuid4()), "PLC", camera_id, "Vertical and Horizontal axes out of sync", "Quality")
                            self.update_tp_column(tp_table, "tp56", event_data)
                        elif flag == "vrs_sync":
                            data = (event_timestamp, str(uuid4()), "PLC", camera_id, "Vertical and Rotational axes out of sync", "Quality")
                            self.update_tp_column(tp_table, "tp57", event_data)
                        elif flag == "frs_sync":
                            data = (event_timestamp, str(uuid4()), "PLC", camera_id, "Filling and Rotational axes out of sync", "Quality")
                            self.update_tp_column(tp_table, "tp58", event_data)
                        elif flag == "phs_sync":
                            data = (event_timestamp, str(uuid4()), "PLC", camera_id, "Pulling and Horizontal axes out of sync", "Quality")
                            self.update_tp_column(tp_table, "tp59", event_data)

                        print(f"[ALERT] {data}")
                        self.cursor_event.execute(self.insert_event_query, data)

                    # Process resolved alerts
                    for flag in resolved_flags:
                        event_data, event_timestamp = self.format_event_data(is_problem=False, flag=flag, last_active_timestamps=last_active_timestamps)

                        if flag == "vhs_sync":
                            data = (event_timestamp, str(uuid4()), "PLC", camera_id, "Vertical and Horizontal axes sync resolved", "Quality")
                            self.update_tp_column(tp_table, "tp56", event_data)
                        elif flag == "vrs_sync":
                            data = (event_timestamp, str(uuid4()), "PLC", camera_id, "Vertical and Rotational axes sync resolved", "Quality")
                            self.update_tp_column(tp_table, "tp57", event_data)
                        elif flag == "frs_sync":
                            data = (event_timestamp, str(uuid4()), "PLC", camera_id, "Filling and Rotational axes sync resolved", "Quality")
                            self.update_tp_column(tp_table, "tp58", event_data)
                        elif flag == "phs_sync":
                            data = (event_timestamp, str(uuid4()), "PLC", camera_id, "Pulling and Horizontal axes sync resolved", "Quality")
                            self.update_tp_column(tp_table, "tp59", event_data)

                        print(f"[RESOLVED] {data}")
                        self.cursor_event.execute(self.insert_event_query, data)

                    state['previous_flags_triggered'] = flags_triggered_set

                self.conn_event.commit()
                self.conn_tp.commit()

                time.sleep(5)

            except psycopg2.Error as db_err:
                print(f"Database error: {db_err}")
                if hasattr(self, 'conn_sync'):
                    self.conn_sync.rollback()
                if hasattr(self, 'conn_event'):
                    self.conn_event.rollback()
                if hasattr(self, 'conn_tp'):
                    self.conn_tp.rollback()
                time.sleep(5)
            except Exception as ex:
                print(f"Unexpected error: {ex}")
                time.sleep(5)

    def __del__(self):
        for attr in ('cursor_sync', 'cursor_event', 'cursor_tp', 'conn_sync', 'conn_event', 'conn_tp'):
            if hasattr(self, attr):
                getattr(self, attr).close()

if __name__ == '__main__':
    SYNCHRONIZATION_TP().run()
