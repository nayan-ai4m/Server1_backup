import sys
import traceback
import psycopg2
import pandas as pd
import numpy as np
from collections import deque
import time

# Database connection parameters for hul
hul_conn_params = {
    'dbname': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432'
}

# Database connection parameters for short_data_hul
short_data_hul_conn_params = {
    'dbname': 'short_data_hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432'
}

def collect_data():  # Get recent 300 rows from table and return as dataframe
    try:
        conn = psycopg2.connect(**hul_conn_params)
        cursor = conn.cursor()
        query = "SELECT * FROM mc27_fast ORDER BY timestamp DESC LIMIT 300;"
        cursor.execute(query)

        # Fetch all rows
        rows = cursor.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        cursor.close()
        conn.close()

        # Create DataFrame
        df = pd.DataFrame(rows, columns=column_names)

        # Convert timestamp and add cycle_id in column 'cycle_id'
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        position_diff = df['cam_position'].diff()
        cycle_change = position_diff < -180
        df['cycle_id'] = cycle_change.cumsum()

        print(f"Collected {len(df)} rows from mc27_fast, unique cycle_id values: {df['cycle_id'].unique()}")
        return df
    except Exception as e:
        print(f"Error in collect_data at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame()

def fetch_data_by_cycle_id(cycle_id):  # Get data for specific cycle_id from dataframe
    try:
        df = collect_data()
        if df.empty:
            print(f"No data fetched from mc27_fast for cycle_id {cycle_id}")
            return pd.DataFrame()

        # Filter data for the specific cycle_id value
        filtered_df = df[df['cycle_id'] == cycle_id]

        # Select specific columns
        columns = [
            'horizontal_sealer_position',
            'vertical_sealer_position',
            'rotary_valve_position',
            'piston_position',
            'puller_instant_load',
            'cam_position',
            'puller_real_current_value_l',
            'timestamp',
            'cycle_id',
            'machine_status'
        ]

        filtered_df = filtered_df[columns].reset_index(drop=True)
        print(f"Fetch for cycle_id {cycle_id}: {len(filtered_df)} rows")
        return filtered_df
    except Exception as e:
        print(f"Error in fetch_data_by_cycle_id at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame()

def check_in_phase(series1, series2):
    try:
        if len(series1) < 2 or len(series2) < 2:
            print("Insufficient data for check_in_phase")
            return 0
        diff1 = np.diff(series1)
        diff2 = np.diff(series2)
        non_zero_mask = (np.sign(diff1) != 0) & (np.sign(diff2) != 0)
        same_direction = np.sign(diff1[non_zero_mask]) == np.sign(diff2[non_zero_mask])
        result = np.sum(same_direction) / len(same_direction) if len(same_direction) > 0 else 0
        return result
    except Exception as e:
        print(f"Error in check_in_phase at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
        traceback.print_exc()
        return 0

def insert_into_sync_data(cycle_id, vs_hs_sync, vs_hs_avg, vs_rv_sync, vs_rv_avg,
                          fp_rv_sync, fp_rv_avg, ps_hs_sync, ps_hs_avg, timestamp, machine_status):
    try:
        conn = psycopg2.connect(**short_data_hul_conn_params)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO sync_data_27 (cycle_id, vertical_horizontal_sync, vertical_horizontal_sync_avg,
                               vertical_rotational_sync, vertical_rotational_sync_avg,
                               filling_rotational_sync, filling_rotational_sync_avg,
                               pulling_horizontal_sync, pulling_horizontal_sync_avg, timestamp,machine_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        cursor.execute(insert_query, (
            cycle_id, vs_hs_sync, vs_hs_avg, vs_rv_sync, vs_rv_avg,
            fp_rv_sync, fp_rv_avg, ps_hs_sync, ps_hs_avg, timestamp, machine_status
        ))

        conn.commit()
        print(f"Inserted sync data for cycle_id {cycle_id} into sync_data_27 at {timestamp}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error in insert_into_sync_data at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
            conn.close()

def insert_event(cycle_id, event, timestamp):
    try:
        conn = psycopg2.connect(**hul_conn_params)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO events (cycle_id, event_description, timestamp)
        VALUES (%s, %s, %s);
        """

        cursor.execute(insert_query, (cycle_id, event, timestamp))

        conn.commit()
        print(f"Inserted event for cycle_id {cycle_id}: {event} at {timestamp}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error in insert_event at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
            conn.close()

def get_latest_cycle_id():  # Get cycle_id value from second most recent unique cycle
    try:
        df = collect_data()
        if df.empty:
            print("No data available in collect_data for get_latest_cycle_id")
            return None

        # Get unique cycle_id values in order (most recent first due to DESC ordering)
        unique_cycle_id_values = df['cycle_id'].drop_duplicates()

        # Check if we have at least 2 unique values
        if len(unique_cycle_id_values) >= 2:
            second_last_cycle_id = unique_cycle_id_values.iloc[1]  # Second unique value
        else:
            second_last_cycle_id = unique_cycle_id_values.iloc[0] if len(unique_cycle_id_values) > 0 else None
        print(f"Latest cycle_id: {second_last_cycle_id}")
        return second_last_cycle_id
    except Exception as e:
        print(f"Error in get_latest_cycle_id at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
        traceback.print_exc()
        return None

class SyncAnalysis:
    def __init__(self):
        self.cycle_buffer = deque(maxlen=3)
        self.avg_buffers = {
            'vs_vs_hs': deque(maxlen=100),
            'fp_vs_rv': deque(maxlen=100),
            'ps_vs_hs': deque(maxlen=100),
            'rv_vs_vs': deque(maxlen=100)
        }
        self.active_alerts = set()
        self.machine_running = True

    def update_analysis(self):
        try:
            latest_cycle_id = get_latest_cycle_id()
            print(f"Cycle buffer: {list(self.cycle_buffer)}")
            if latest_cycle_id is not None and (not self.cycle_buffer or self.cycle_buffer[-1] != latest_cycle_id):
                self.cycle_buffer.append(latest_cycle_id)

            if len(self.cycle_buffer) == 3:
                cycle_to_process = self.cycle_buffer.popleft()
                print(f"Processing cycle_id (2-cycle delay): {cycle_to_process}")

                df = fetch_data_by_cycle_id(cycle_to_process)
                if not df.empty:
                    self.process_cycle(df, cycle_to_process)
                else:
                    print(f"Skipping cycle {cycle_to_process}: No data available")
        except Exception as e:
            print(f"Error in update_analysis at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()

    def process_cycle(self, df, cycle_id):
        try:
            machine_status = df['machine_status'].iloc[0]
            self.machine_running = bool(machine_status)
            timestamp = df['timestamp'].iloc[-1]
            print(f"Processing cycle {cycle_id}, machine_status: {machine_status}, timestamp: {timestamp}")

            new_alerts = set()

            if self.machine_running:
                vs_hs_sync = check_in_phase(df['vertical_sealer_position'], df['horizontal_sealer_position'])
                fp_rv_sync = check_in_phase(df['piston_position'], df['rotary_valve_position'])
                ps_hs_sync = check_in_phase(df['puller_real_current_value_l'], df['horizontal_sealer_position'])
                rv_vs_sync = check_in_phase(df['rotary_valve_position'], df['vertical_sealer_position'])

                self.avg_buffers['vs_vs_hs'].append(vs_hs_sync * 100)
                self.avg_buffers['fp_vs_rv'].append(fp_rv_sync * 100)
                self.avg_buffers['ps_vs_hs'].append(ps_hs_sync * 100)
                self.avg_buffers['rv_vs_vs'].append(rv_vs_sync * 100)

                avg_vs_hs = np.mean(self.avg_buffers['vs_vs_hs'])
                avg_fp_rv = np.mean(self.avg_buffers['fp_vs_rv'])
                avg_ps_hs = np.mean(self.avg_buffers['ps_vs_hs'])
                avg_rv_vs = np.mean(self.avg_buffers['rv_vs_vs'])

                insert_into_sync_data(
                    cycle_id, vs_hs_sync * 100, avg_vs_hs, rv_vs_sync * 100, avg_rv_vs,
                    fp_rv_sync * 100, avg_fp_rv, ps_hs_sync * 100, avg_ps_hs, timestamp, machine_status
                )

                self.check_sync_alert('vs_vs_hs', 70, False, 'Vertical Servo vs Horizontal Servo', new_alerts)
                self.check_sync_alert('fp_vs_rv', 40, True, 'Fill Piston vs Rotational Valve', new_alerts)
                self.check_sync_alert('ps_vs_hs', 40, True, 'Pulling Servo vs Horizontal Servo', new_alerts)
                self.check_sync_alert('rv_vs_vs', 70, False, 'Rotational Valve vs Vertical Servo', new_alerts)
            else:
                new_alerts.add("Machine Stopped")

            for alert in new_alerts:
                insert_event(cycle_id, alert, timestamp)

            self.active_alerts = new_alerts
            print("Active alerts:", self.active_alerts)
        except Exception as e:
            print(f"Error in process_cycle for cycle_id {cycle_id} at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()

    def check_sync_alert(self, buffer_key, threshold, alert_above, title, new_alerts):
        try:
            avg_sync = np.mean(self.avg_buffers[buffer_key])
            if (alert_above and avg_sync > threshold) or (not alert_above and avg_sync < threshold):
                new_alerts.add(f"{title} out of sync")
        except Exception as e:
            print(f"Error in check_sync_alert for {buffer_key} at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()

def main():
    try:
        analyzer = SyncAnalysis()
        while True:
            analyzer.update_analysis()
            time.sleep(0.1)  # Update every 100 ms
    except Exception as e:
        print(f"Error in main at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
