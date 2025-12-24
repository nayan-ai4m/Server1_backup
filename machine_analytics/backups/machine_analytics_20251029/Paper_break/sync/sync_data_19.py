import psycopg2
import pandas as pd
import numpy as np
from collections import deque
import time

# Database connection parameters
conn_params = {
    'dbname': 'short_data_hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': '192.168.1.149',
    'port': '5432'
}

def fetch_data_by_spare1(spare1):
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    query = """
    SELECT hor_sealer_position,
           ver_sealer_position,
           rot_valve_1_position,
           fill_piston_1_position,
           web_puller_position,
           cam_position,
           web_puller_current/10.0 as web_puller_current,
           timestamp,
           spare1,
           status
    FROM mc19_short_data
    WHERE spare1 = %s
    ORDER BY timestamp DESC;
    """
    cursor.execute(query, (spare1,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    columns = ['hor_sealer_position',
               'ver_sealer_position',
               'rot_valve_1_position',
               'fill_piston_1_position',
               'web_puller_position',
               'cam_position',
               'web_puller_current',
               'timestamp',
               'spare1',
               'status']

    return pd.DataFrame(rows, columns=columns)

def check_in_phase(series1, series2):
    diff1 = np.diff(series1)
    diff2 = np.diff(series2)
    non_zero_mask = (np.sign(diff1) != 0) & (np.sign(diff2) != 0)
    same_direction = np.sign(diff1[non_zero_mask]) == np.sign(diff2[non_zero_mask])
    return np.sum(same_direction) / len(same_direction) if len(same_direction) > 0 else 0

def insert_into_sync_data(spare1, vs_hs_sync, vs_hs_avg, vs_rv_sync, vs_rv_avg,
                          fp_rv_sync, fp_rv_avg, ps_hs_sync, ps_hs_avg, timestamp):
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO sync_data_19 (spare1, vertical_horizontal_sync, vertical_horizontal_sync_avg,
                           vertical_rotational_sync, vertical_rotational_sync_avg,
                           filling_rotational_sync, filling_rotational_sync_avg,
                           pulling_horizontal_sync, pulling_horizontal_sync_avg, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    cursor.execute(insert_query, (
        spare1, vs_hs_sync, vs_hs_avg, vs_rv_sync, vs_rv_avg,
        fp_rv_sync, fp_rv_avg, ps_hs_sync, ps_hs_avg, timestamp
    ))

    conn.commit()
    cursor.close()
    conn.close()

def get_latest_spare1():
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    query = "SELECT spare1 FROM mc19_short_data ORDER BY timestamp DESC LIMIT 1;"
    cursor.execute(query)
    result = cursor.fetchone()
    latest_spare1 = result[0] if result else None
    cursor.close()
    conn.close()
    return latest_spare1

def process_cycle(df, spare1, avg_buffers):
    status = df['status'].iloc[0]
    machine_running = bool(status)

    if machine_running:
        vs_hs_sync = check_in_phase(df['ver_sealer_position'], df['hor_sealer_position'])
        fp_rv_sync = check_in_phase(df['fill_piston_1_position'], df['rot_valve_1_position'])
        ps_hs_sync = check_in_phase(df['web_puller_current'], df['hor_sealer_position'])
        rv_vs_sync = check_in_phase(df['rot_valve_1_position'], df['ver_sealer_position'])

        avg_buffers['vs_vs_hs'].append(vs_hs_sync * 100)
        avg_buffers['fp_vs_rv'].append(fp_rv_sync * 100)
        avg_buffers['ps_vs_hs'].append(ps_hs_sync * 100)
        avg_buffers['rv_vs_vs'].append(rv_vs_sync * 100)

        avg_vs_hs = np.mean(avg_buffers['vs_vs_hs'])
        avg_fp_rv = np.mean(avg_buffers['fp_vs_rv'])
        avg_ps_hs = np.mean(avg_buffers['ps_vs_hs'])
        avg_rv_vs = np.mean(avg_buffers['rv_vs_vs'])

        timestamp = df['timestamp'].iloc[-1]

        insert_into_sync_data(
            spare1, vs_hs_sync * 100, avg_vs_hs, rv_vs_sync * 100, avg_rv_vs,
            fp_rv_sync * 100, avg_fp_rv, ps_hs_sync * 100, avg_ps_hs, timestamp
        )

        print(f"Inserted data for spare1 {spare1}:")
        print(f"  Vertical-Horizontal Sync: {vs_hs_sync * 100:.2f}% (Avg: {avg_vs_hs:.2f}%)")
        print(f"  Vertical-Rotational Sync: {rv_vs_sync * 100:.2f}% (Avg: {avg_rv_vs:.2f}%)")
        print(f"  Filling-Rotational Sync: {fp_rv_sync * 100:.2f}% (Avg: {avg_fp_rv:.2f}%)")
        print(f"  Pulling-Horizontal Sync: {ps_hs_sync * 100:.2f}% (Avg: {avg_ps_hs:.2f}%)")
    else:
        print(f"Machine stopped for spare1 {spare1}. No data inserted.")

def main():
    cycle_buffer = deque(maxlen=3)
    avg_buffers = {
        'vs_vs_hs': deque(maxlen=100),
        'fp_vs_rv': deque(maxlen=100),
        'ps_vs_hs': deque(maxlen=100),
        'rv_vs_vs': deque(maxlen=100)
    }

    print("Starting synchronization analysis for machine 19...")
    while True:
        try:
            latest_spare1 = get_latest_spare1()
            if latest_spare1 is None:
                print("No data found in mc19_short_data. Waiting...")
                time.sleep(1)
                continue

            if not cycle_buffer or cycle_buffer[-1] != latest_spare1:
                cycle_buffer.append(latest_spare1)

            if len(cycle_buffer) == 3:
                cycle_to_process = cycle_buffer.popleft()
                print(f"Processing spare1 (2-cycle delay): {cycle_to_process}")

                df = fetch_data_by_spare1(cycle_to_process)
                process_cycle(df, cycle_to_process, avg_buffers)

            time.sleep(0.1)  # Check every 100 ms, matching original timer
        except KeyboardInterrupt:
            print("Stopping synchronization analysis...")
            break
        except Exception as e:
            print(f"Error occurred: {e}")
            time.sleep(1)  # Wait before retrying on error

if __name__ == "__main__":
    main()
