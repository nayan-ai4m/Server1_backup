import sys
import json
import psycopg2
import pandas as pd
import numpy as np
import time
import warnings
import threading
from datetime import datetime, timedelta
from prettytable import PrettyTable
import os

from suggestion_module import get_suggestions
from logger_module import log_event

# Dash imports
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_table

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Machine list
MACHINES = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']

# Global data storage for console & UI
machine_data = {}
data_lock = threading.Lock()


# ===== CONFIG LOADING =====
def load_config(machine_id):
    """Load config for specific machine"""
    config_file = f"config_{machine_id}.json"
    try:
        with open(config_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Config file {config_file} not found!")
        return None
    except Exception as e:
        print(f"Error loading config for {machine_id}: {e}")
        return None


# ===== DB FETCH =====
def fetch_data_from_postgres(machine_id):
    """Fetch data for specific machine"""
    try:
        conn_short = psycopg2.connect(
            dbname="short_data_hul",
            user="postgres",
            password="ai4m2024",
            host="100.96.244.68",
            port="5432"
        )
        query_short = f"""
        SELECT cam_position,
               spare1,
               status,
               timestamp,
               hor_pressure
        FROM public.{machine_id}_short_data
        ORDER BY timestamp DESC
        LIMIT 1000;
        """
        df_short = pd.read_sql_query(query_short, conn_short)
        conn_short.close()

        conn_mid = psycopg2.connect(
            dbname="hul",
            user="postgres",
            password="ai4m2024",
            host="100.96.244.68",
            port="5432"
        )
        query_mid = f"""
        SELECT spare1,
               timestamp,
               hor_sealer_rear_1_temp,
               hor_sealer_front_1_temp
        FROM public.{machine_id}_mid
        ORDER BY timestamp DESC
        LIMIT 1000;
        """
        df_mid = pd.read_sql_query(query_mid, conn_mid)
        conn_mid.close()

        df_short['timestamp'] = pd.to_datetime(df_short['timestamp'])
        df_mid['timestamp'] = pd.to_datetime(df_mid['timestamp'])

        df = pd.merge(df_short, df_mid, on='spare1', how='left', suffixes=('', '_mid'))
        df = df.sort_values('timestamp').reset_index(drop=True)
        df[['hor_sealer_rear_1_temp', 'hor_sealer_front_1_temp']] = df[
            ['hor_sealer_rear_1_temp', 'hor_sealer_front_1_temp']
        ].ffill()

        return df
    except Exception as e:
        print(f"Database error for {machine_id}: {e}")
        return pd.DataFrame()


# ===== HEAT ENERGY =====
def calculate_heat_energy(T_C, P, cfg):
    A = cfg["A"]
    alpha = cfg["alpha"]
    beta = cfg["beta"]
    C = cfg["C"]
    ambient_temp = cfg["ambient_temp"]
    t = cfg["t"]
    k_A = cfg["k_A"]
    k_B = cfg["k_B"]
    k_C = cfg["k_C"]
    d1 = cfg["d1"]
    d2 = cfg["d2"]
    d3 = cfg["d3"]

    T_K = T_C + 273.15
    T_K -= 30  # adjustment
    ambient_K = ambient_temp + 273.15

    denominator = (d1 / k_A) + (d2 / k_B) + (d3 / k_C)
    heat_term = ((alpha * T_K) - (ambient_K + C)) * A * beta * t / denominator
    return heat_term


# ===== PROCESS DATA =====
def process_data(df, cfg):
    df = df.copy()
    df.rename(columns={'spare1': 'cycle_id'}, inplace=True)
    df['cycle_id'] = pd.to_numeric(df['cycle_id'], errors='coerce')
    df['temperature_C'] = (df['hor_sealer_rear_1_temp'] + df['hor_sealer_front_1_temp']) / 2

    pressure_per_cycle = (
        df[(df['cam_position'] >= 150) & (df['cam_position'] <= 210)]
        .groupby('cycle_id')['hor_pressure']
        .mean()
        .reset_index(name='pressure_avg')
    )

    cycle_data = (
        df.groupby('cycle_id', dropna=True)
          .agg({'temperature_C': 'mean', 'timestamp': 'max'})
          .reset_index()
          .rename(columns={'timestamp': 'last_ts'})
    )

    cycle_data = cycle_data.merge(pressure_per_cycle, on='cycle_id', how='left')
    cycle_data['heat_energy'] = cycle_data.apply(
        lambda row: calculate_heat_energy(row['temperature_C'], row['pressure_avg'], cfg), axis=1
    )
    cycle_data = cycle_data.sort_values('last_ts', ascending=True).reset_index(drop=True)
    return cycle_data


# ===== CONFIG CHANGE DETECTION =====
def check_config_changes(current_config, previous_config):
    if previous_config is None:
        return False
    for param in ['d1', 'd2', 'd3']:
        if current_config.get(param) != previous_config.get(param):
            return True
    return False


# ===== MACHINE MONITORING FUNCTION =====
def monitor_machine(machine_id):
    print(f"Starting monitoring for {machine_id}...")
    last_valid_pressure = None
    last_status = None
    last_suggestion = None
    previous_config = None
    heat_issue_logged = None
    pressure_issue_logged = None
    
    while True:
        cfg = load_config(machine_id)
        if cfg is None:
            time.sleep(2)
            continue

        config_changed = check_config_changes(cfg, previous_config)

        try:
            df = fetch_data_from_postgres(machine_id)
        except Exception as e:
            log_event(f"DB fetch failed: {e}", time.strftime("%Y-%m-%d %H:%M:%S"), machine_id)
            time.sleep(2)
            continue

        if df.empty:
            with data_lock:
                current_ist = datetime.now() + timedelta(hours=5, minutes=30)
                machine_data[machine_id] = {
                    'status': 'NO DATA', 'timestamp': current_ist.strftime("%H:%M:%S"),
                    'cycle_id': 'N/A', 'heat': 'N/A', 'pressure': 'N/A', 'temp': 'N/A',
                    'seal': 'N/A', 'seal_reason': 'N/A',
                    'suggested_temp': 'N/A', 'suggested_s1': 'N/A', 'suggested_s2': 'N/A'
                }
            time.sleep(2)
            continue

        latest_machine_ts = df.iloc[0]['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
        current_status = df.iloc[0]['status']
        machine_running = (current_status == 1)

        if last_status is None:
            last_status = current_status
        elif last_status != current_status:
            if current_status == 1:
                log_event("Machine started", latest_machine_ts, machine_id)
            else:
                log_event("Machine stopped", latest_machine_ts, machine_id)
            last_status = current_status

        cycle_df = process_data(df, cfg)

        pressures = cycle_df['pressure_avg'].copy()
        for i in range(len(pressures)):
            if pd.isna(pressures.iloc[i]) and last_valid_pressure is not None:
                pressures.iloc[i] = last_valid_pressure
            else:
                last_valid_pressure = pressures.iloc[i]
        cycle_df['pressure_avg'] = pressures

        heat_lo = cfg["GOOD_HEAT_TARGET"] * (1 - cfg["GOOD_HEAT_TOLERANCE"])
        heat_hi = cfg["GOOD_HEAT_TARGET"] * (1 + cfg["GOOD_HEAT_TOLERANCE"])
        pres_lo, pres_hi = cfg["GOOD_PRESSURE_MIN"], cfg["GOOD_PRESSURE_MAX"]

        latest_heat = float(cycle_df['heat_energy'].iloc[-1]) if len(cycle_df) else np.nan
        latest_pressure = float(pressures.iloc[-1]) if len(cycle_df) else np.nan
        latest_temp = float(cycle_df['temperature_C'].iloc[-1]) if len(cycle_df) else np.nan
        latest_cycle_id = df.iloc[0]['spare1']
        suggestions = get_suggestions(machine_id)

        # === Seal Quality Logic ===
        seal_quality = "N/A"
        seal_reason = "N/A"

        if machine_running and not (np.isnan(latest_heat) or np.isnan(latest_pressure)):
            heat_ok = (heat_lo <= latest_heat <= heat_hi)
            pressure_ok = (pres_lo <= latest_pressure <= pres_hi)
            seal_quality = "GOOD SEAL" if (heat_ok and pressure_ok) else "BAD SEAL"

            if not (heat_ok and pressure_ok):
                reasons = []
                if latest_heat < heat_lo:
                    reasons.append("Low Heat")
                elif latest_heat > heat_hi:
                    reasons.append("High Heat")

                if latest_pressure < pres_lo:
                    reasons.append("Low Pressure")
                elif latest_pressure > pres_hi:
                    reasons.append("High Pressure")

                seal_reason = ", ".join(reasons) if reasons else "Unknown"
            else:
                seal_reason = "--"

        # Update global machine_data for UI & console
        with data_lock:
            try:
                ts_datetime = datetime.strptime(latest_machine_ts, "%Y-%m-%d %H:%M:%S")
                ist_datetime = ts_datetime + timedelta(hours=5, minutes=30)
                display_time = ist_datetime.strftime("%H:%M:%S")
            except:
                display_time = "N/A"
            
            machine_data[machine_id] = {
                'status': 'RUNNING' if machine_running else 'STOPPED',
                'timestamp': display_time,
                'cycle_id': str(latest_cycle_id),
                'heat': f"{latest_heat:.2f}" if not np.isnan(latest_heat) else "N/A",
                'pressure': f"{latest_pressure:.2f}" if not np.isnan(latest_pressure) else "N/A",
                'temp': f"{latest_temp:.2f}" if not np.isnan(latest_temp) else "N/A",
                'seal': seal_quality,
                'seal_reason': seal_reason,
                'suggested_temp': f"{suggestions['suggested_temp']:.2f}" if suggestions and 'error' not in suggestions else "N/A",
                'suggested_s1': f"{suggestions['suggested_s1']:.3f}" if suggestions and 'error' not in suggestions else "N/A",
                'suggested_s2': f"{suggestions['suggested_s2']:.3f}" if suggestions and 'error' not in suggestions else "N/A"
            }
        previous_config = cfg.copy() if cfg else None
        time.sleep(2)


# ===== CONSOLE DISPLAY =====
def display_console():
    while True:
        os.system('clear' if os.name == 'posix' else 'cls')
        print("=" * 180)
        print("Loop 3 - Leakage Prediction and Suggestion".center(180))
        print("=" * 180)
        table = PrettyTable()
        table.field_names = ["Machine", "Status", "Time (IST)", "Cycle", "Heat (J)", "Pressure (bar)",
                             "Temp (°C)", "Seal Quality", "Reason", "Sugg Temp", "Sugg S1", "Sugg S2"]
        table.align = "c"
        with data_lock:
            for machine_id in MACHINES:
                if machine_id in machine_data:
                    d = machine_data[machine_id]
                    table.add_row([machine_id.upper(), d['status'], d['timestamp'], d['cycle_id'],
                                   d['heat'], d['pressure'], d['temp'], d['seal'], d['seal_reason'],
                                   d['suggested_temp'], d['suggested_s1'], d['suggested_s2']])
                else:
                    table.add_row([machine_id.upper()] + ["N/A"]*11)
        print(table)
        current_ist = datetime.now() + timedelta(hours=5, minutes=30)
        print(f"\nLast Updated: {current_ist.strftime('%Y-%m-%d %H:%M:%S')} (IST)")
        print("Press Ctrl+C to stop monitoring")
        time.sleep(2)


# ===== DASH UI =====
app = dash.Dash(__name__)
server = app.server

app.layout = html.Div([
    html.H1("Loop 3 - Leakage Prediction and Suggestion"),
    dcc.Interval(id='interval-component', interval=2000, n_intervals=0),
    dash_table.DataTable(
        id='live-table',
        columns=[{"name": col, "id": col} for col in
                 ["Machine", "Status", "Time (IST)", "Cycle", "Heat (J)", "Pressure (bar)",
                  "Temp (°C)", "Seal Quality", "Reason", "Sugg Temp", "Sugg S1", "Sugg S2"]],
        data=[],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'center', 'padding': '5px'},
        style_header={'backgroundColor': 'lightgrey', 'fontWeight': 'bold'}
    )
])


@app.callback(
    Output('live-table', 'data'),
    Input('interval-component', 'n_intervals')
)
def update_table(n):
    rows = []
    with data_lock:
        for machine_id in MACHINES:
            d = machine_data.get(machine_id, {})
            rows.append({
                "Machine": machine_id.upper(),
                "Status": d.get('status', "N/A"),
                "Time (IST)": d.get('timestamp', "N/A"),
                "Cycle": d.get('cycle_id', "N/A"),
                "Heat (J)": d.get('heat', "N/A"),
                "Pressure (bar)": d.get('pressure', "N/A"),
                "Temp (°C)": d.get('temp', "N/A"),
                "Seal Quality": d.get('seal', "N/A"),
                "Reason": d.get('seal_reason', "N/A"),
                "Sugg Temp": d.get('suggested_temp', "N/A"),
                "Sugg S1": d.get('suggested_s1', "N/A"),
                "Sugg S2": d.get('suggested_s2', "N/A")
            })
    return rows


# ===== MAIN FUNCTION =====
def main():
    print("Starting Multi-Machine Monitoring System...")
    print(f"Monitoring machines: {', '.join(MACHINES)}")
    with data_lock:
        for machine_id in MACHINES:
            machine_data[machine_id] = {k: "N/A" for k in
                ['status','timestamp','cycle_id','heat','pressure','temp','seal',
                 'seal_reason','suggested_temp','suggested_s1','suggested_s2']}
    # Start monitoring threads
    for m in MACHINES:
        threading.Thread(target=monitor_machine, args=(m,), daemon=True).start()
        time.sleep(0.1)
    # Start console display thread
    threading.Thread(target=display_console, daemon=True).start()
    # Start Dash server
    app.run_server(host="0.0.0.0", port=8050, debug=False)


if __name__ == "__main__":
    main()
