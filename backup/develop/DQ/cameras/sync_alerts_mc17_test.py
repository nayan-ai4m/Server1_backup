import psycopg2
from datetime import datetime
from uuid import uuid4
import json
import time

class SYNCHRONIZATION_TP:
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

        self.fetch_sync_query = '''
            SELECT vertical_horizontal_sync_avg, vertical_rotational_sync_avg,
                   filling_rotational_sync_avg, pulling_horizontal_sync_avg
            FROM sync_data
            ORDER BY timestamp DESC
            LIMIT 1;
        '''

        self.insert_event_query = """
            INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type)
            VALUES (%s, %s, %s, %s, %s, %s);
        """

        # Initialize dictionary to store last active timestamps for each flag
        self.last_active_timestamps = {
            "vhs_sync": None,
            "vrs_sync": None,
            "frs_sync": None,
            "phs_sync": None
        }

        for tp in ["tp56", "tp57", "tp58", "tp59"]:
            self.ensure_tp_column_exists(tp)

    def ensure_tp_column_exists(self, tp_column):
        try:
            self.cursor1.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='mc17_tp_status' AND column_name=%s;
            """, (tp_column,))
            column_exists = self.cursor1.fetchone()
            if not column_exists:
                print(f"Column '{tp_column}' does not exist. Creating it...")
                self.cursor1.execute(f"ALTER TABLE mc17_tp_status ADD COLUMN {tp_column} JSONB;")
                self.conn1.commit()
                print(f"Column '{tp_column}' created successfully.")
        except Exception as e:
            print(f"Error checking/creating column '{tp_column}': {e}")
            self.conn1.rollback()

    def format_event_data(self, is_problem, flag):
        current_time = datetime.now()
        # Use current_time for active=1, last_active_timestamp for active=0
        timestamp = current_time if is_problem else (self.last_active_timestamps[flag] or current_time)
        event_data = {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": timestamp.isoformat(),
            "color_code": 3 if is_problem else 1
        }
        if not is_problem and self.last_active_timestamps[flag] is not None:
            event_data["updated_time"] = current_time.isoformat()
        return event_data, timestamp

    def update_tp_column(self, column, event_data):
        check_query = f"SELECT {column} FROM mc17_tp_status;"
        update_query = f"UPDATE mc17_tp_status SET {column} = %s;"
        insert_query = f"INSERT INTO mc17_tp_status ({column}) VALUES (%s);"

        self.cursor1.execute(check_query)
        exists = self.cursor1.fetchone()

        if exists:
            print(f"Updating {column} with event data: {event_data}")
            self.cursor1.execute(update_query, (json.dumps(event_data),))
        else:
            print(f"Inserting new {column} with event data: {event_data}")
            self.cursor1.execute(insert_query, (json.dumps(event_data),))

    def run(self):
        print("Started Code for Synchronization Alerts for MC17")
        previous_flags_triggered = set()

        while True:
            try:
                self.cursor.execute(self.fetch_sync_query)
                sync_row = self.cursor.fetchone()

                if sync_row and sync_row[0] is not None:
                    vhs = float(sync_row[0])
                    vrs = float(sync_row[1])
                    frs = float(sync_row[2])
                    phs = float(sync_row[3])
                else:
                    vhs = vrs = frs = phs = None

                #print(f"vhs={vhs}, vrs={vrs}, frs={frs}, phs={phs}")

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

                # Process new alerts
                for flag in new_flags:
                    event_data, event_timestamp = self.format_event_data(is_problem=True, flag=flag)
                    self.last_active_timestamps[flag] = datetime.now()  # Update timestamp for active=1

                    if flag == "vhs_sync":
                        data = (event_timestamp, str(uuid4()), "PLC", "MC17", "Vertical and Horizontal axes out of sync", "Quality")
                        self.update_tp_column("tp56", event_data)
                    elif flag == "vrs_sync":
                        data = (event_timestamp, str(uuid4()), "PLC", "MC17", "Vertical and Rotational axes out of sync", "Quality")
                        self.update_tp_column("tp57", event_data)
                    elif flag == "frs_sync":
                        data = (event_timestamp, str(uuid4()), "PLC", "MC17", "Filling and Rotational axes out of sync", "Quality")
                        self.update_tp_column("tp58", event_data)
                    elif flag == "phs_sync":
                        data = (event_timestamp, str(uuid4()), "PLC", "MC17", "Pulling and Horizontal axes out of sync", "Quality")
                        self.update_tp_column("tp59", event_data)

                    print(f"[ALERT] {data}")
                    self.cursor1.execute(self.insert_event_query, data)

                # Process resolved alerts
                for flag in resolved_flags:
                    event_data, event_timestamp = self.format_event_data(is_problem=False, flag=flag)

                    if flag == "vhs_sync":
                        data = (event_timestamp, str(uuid4()), "PLC", "MC17", "Vertical and Horizontal axes sync resolved", "Quality")
                        self.update_tp_column("tp56", event_data)
                    elif flag == "vrs_sync":
                        data = (event_timestamp, str(uuid4()), "PLC", "MC17", "Vertical and Rotational axes sync resolved", "Quality")
                        self.update_tp_column("tp57", event_data)
                    elif flag == "frs_sync":
                        data = (event_timestamp, str(uuid4()), "PLC", "MC17", "Filling and Rotational axes sync resolved", "Quality")
                        self.update_tp_column("tp58", event_data)
                    elif flag == "phs_sync":
                        data = (event_timestamp, str(uuid4()), "PLC", "MC17", "Pulling and Horizontal axes sync resolved", "Quality")
                        self.update_tp_column("tp59", event_data)

                    print(f"[RESOLVED] {data}")
                    self.cursor1.execute(self.insert_event_query, data)

                self.conn1.commit()
                previous_flags_triggered = flags_triggered_set

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
    SYNCHRONIZATION_TP().run()
