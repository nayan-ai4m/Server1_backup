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

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Machine list
MACHINES = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']

# Global data storage for console display
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
        # --- Connect to short_data_hul (for machine short_data) ---
        conn_short = psycopg2.connect(
            dbname="short_data_hul",
            user="postgres",
            password="ai4m2024",
            host="localhost",
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

        # --- Connect to hul (for machine mid) ---
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

        # --- Convert timestamps ---
        df_short['timestamp'] = pd.to_datetime(df_short['timestamp'])
        df_mid['timestamp'] = pd.to_datetime(df_mid['timestamp'])

        # --- Merge on spare1 ---
        df = pd.merge(df_short, df_mid, on='spare1', how='left', suffixes=('', '_mid'))

        # --- Forward fill missing temps (use last known until new one appears) ---
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
    """Calculate heat energy for given temperature, pressure and config"""
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
    """Process dataframe to get cycle-based data"""
    df = df.copy()
    df.rename(columns={'spare1': 'cycle_id'}, inplace=True)
    df['cycle_id'] = pd.to_numeric(df['cycle_id'], errors='coerce')
    df['temperature_C'] = (df['hor_sealer_rear_1_temp'] + df['hor_sealer_front_1_temp']) / 2

    # Average pressure for cam 150–210
    pressure_per_cycle = (
        df[(df['cam_position'] >= 150) & (df['cam_position'] <= 210)]
        .groupby('cycle_id')['hor_pressure']
        .mean()
        .reset_index(name='pressure_avg')
    )

    # Per-cycle temperature and last timestamp
    cycle_data = (
        df.groupby('cycle_id', dropna=True)
          .agg({'temperature_C': 'mean', 'timestamp': 'max'})
          .reset_index()
          .rename(columns={'timestamp': 'last_ts'})
    )

    cycle_data = cycle_data.merge(pressure_per_cycle, on='cycle_id', how='left')

    # Heat energy
    cycle_data['heat_energy'] = cycle_data.apply(
        lambda row: calculate_heat_energy(row['temperature_C'], row['pressure_avg'], cfg), axis=1
    )

    cycle_data = cycle_data.sort_values('last_ts', ascending=True).reset_index(drop=True)
    return cycle_data


# ===== CONFIG CHANGE DETECTION =====
def check_config_changes(current_config, previous_config):
    """
    Check if d1, d2, or d3 have changed in the config
    Returns True if any of these parameters changed, False otherwise
    """
    if previous_config is None:
        return False
    
    config_params_to_monitor = ['d1', 'd2', 'd3']
    
    for param in config_params_to_monitor:
        if current_config.get(param) != previous_config.get(param):
            return True
    
    return False


# ===== MACHINE MONITORING FUNCTION =====
def monitor_machine(machine_id):
    """Monitor a single machine in a separate thread"""
    print(f"Starting monitoring for {machine_id}...")
    
    # Machine-specific state variables
    last_valid_pressure = None
    last_status = None
    last_suggestion = None
    previous_config = None
    heat_issue_logged = None  # None, "low", "high"
    pressure_issue_logged = None  # None, "low", "high"
    
    while True:
        cfg = load_config(machine_id)
        if cfg is None:
            time.sleep(2)
            continue
        
        # Check for config changes in d1, d2, d3
        config_changed = check_config_changes(cfg, previous_config)
        
        try:
            df = fetch_data_from_postgres(machine_id)
        except Exception as e:
            log_event(f"DB fetch failed: {e}", time.strftime("%Y-%m-%d %H:%M:%S"), machine_id)
            time.sleep(2)
            continue

        if df.empty:
                # Update global data with error status (with IST time)
            with data_lock:
                current_ist = datetime.now() + timedelta(hours=5, minutes=30)
                machine_data[machine_id] = {
                    'status': 'NO DATA',
                    'timestamp': current_ist.strftime("%H:%M:%S"),
                    'cycle_id': 'N/A',
                    'heat': 'N/A',
                    'pressure': 'N/A',
                    'temp': 'N/A',
                    'seal': 'N/A',
                    'seal_reason': 'N/A',
                    'suggested_temp': 'N/A',
                    'suggested_s1': 'N/A',
                    'suggested_s2': 'N/A'
                }
            time.sleep(2)
            continue

        latest_machine_ts = df.iloc[0]['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
        current_status = df.iloc[0]['status']
        machine_running = (current_status == 1)

        # Log start/stop
        if last_status is None:
            last_status = current_status
        elif last_status != current_status:
            if current_status == 1:
                log_event("Machine started", latest_machine_ts, machine_id)
            else:
                log_event("Machine stopped", latest_machine_ts, machine_id)
            last_status = current_status

        # Process cycles
        cycle_df = process_data(df, cfg)

        # Fill NaN pressures with last valid pressure
        pressures = cycle_df['pressure_avg'].copy()
        for i in range(len(pressures)):
            if pd.isna(pressures.iloc[i]):
                if last_valid_pressure is not None:
                    pressures.iloc[i] = last_valid_pressure
            else:
                last_valid_pressure = pressures.iloc[i]
        cycle_df['pressure_avg'] = pressures

        # Ranges
        heat_lo = cfg["GOOD_HEAT_TARGET"] * (1 - cfg["GOOD_HEAT_TOLERANCE"])
        heat_hi = cfg["GOOD_HEAT_TARGET"] * (1 + cfg["GOOD_HEAT_TOLERANCE"])
        pres_lo, pres_hi = cfg["GOOD_PRESSURE_MIN"], cfg["GOOD_PRESSURE_MAX"]

        # Latest values
        latest_heat = float(cycle_df['heat_energy'].iloc[-1]) if len(cycle_df) else np.nan
        latest_pressure = float(pressures.iloc[-1]) if len(cycle_df) else np.nan
        latest_temp = float(cycle_df['temperature_C'].iloc[-1]) if len(cycle_df) else np.nan
        latest_cycle_id = df.iloc[0]['spare1']

        # Suggestions
        suggestions = get_suggestions(machine_id)

        # === CONFIG CHANGE SUGGESTIONS (Works regardless of machine status) ===
        if config_changed and previous_config is not None:
            changed_params = []
            for param in ['d1', 'd2', 'd3']:
                if cfg.get(param) != previous_config.get(param):
                    changed_params.append(f"{param}: {previous_config.get(param)} → {cfg.get(param)}")
            
            log_event(
                f"Config parameter(s) changed: {', '.join(changed_params)}. "
                f"New suggestions - Temp: {suggestions['suggested_temp']:.2f}°C, "
                f"Stroke1: {suggestions['suggested_s1']:.3f}, "
                f"Stroke2: {suggestions['suggested_s2']:.3f}",
                latest_machine_ts,
                machine_id
            )
            last_suggestion = suggestions  # Update to prevent duplicate logging

        # === MACHINE RUNNING SUGGESTIONS ===
        seal_quality = "N/A"
        seal_reason = "N/A"
        
        if machine_running and not (np.isnan(latest_heat) or np.isnan(latest_pressure)):
            heat_ok = (heat_lo <= latest_heat <= heat_hi)
            pressure_ok = (pres_lo <= latest_pressure <= pres_hi)
            seal_quality = "GOOD SEAL" if (heat_ok and pressure_ok) else "BAD SEAL"
            
            # Determine reason for bad seal
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

            # --- Heat energy log ---
            if not heat_ok:
                if latest_heat < heat_lo and heat_issue_logged != "low":
                    log_event(f"Low heat energy, change the temperature to {suggestions['suggested_temp']:.2f}", latest_machine_ts, machine_id)
                    heat_issue_logged = "low"
                elif latest_heat > heat_hi and heat_issue_logged != "high":
                    log_event(f"High heat energy, change the temperature to {suggestions['suggested_temp']:.2f}", latest_machine_ts, machine_id)
                    heat_issue_logged = "high"
            else:
                heat_issue_logged = None

            # --- Pressure log ---
            if not pressure_ok:
                if latest_pressure < pres_lo and pressure_issue_logged != "low":
                    log_event(
                        f"Low pressure, change stroke1 to {suggestions['suggested_s1']:.3f}, stroke2 to {suggestions['suggested_s2']:.3f}",
                        latest_machine_ts,
                        machine_id
                    )
                    pressure_issue_logged = "low"
                elif latest_pressure > pres_hi and pressure_issue_logged != "high":
                    log_event(
                        f"High pressure, change stroke1 to {suggestions['suggested_s1']:.3f}, stroke2 to {suggestions['suggested_s2']:.3f}",
                        latest_machine_ts,
                        machine_id
                    )
                    pressure_issue_logged = "high"
            else:
                pressure_issue_logged = None

            # --- Suggestion log (only for non-config changes) ---
            if last_suggestion != suggestions and not config_changed:
                log_event(
                    "New suggestion given - "
                    f"Temp: {suggestions['suggested_temp']:.2f}°C, "
                    f"Stroke1: {suggestions['suggested_s1']:.3f}, "
                    f"Stroke2: {suggestions['suggested_s2']:.3f}",
                    latest_machine_ts,
                    machine_id
                )
                last_suggestion = suggestions

        # Update global data for console display
        with data_lock:
            # Convert timestamp to IST (+5:30)
            try:
                if isinstance(latest_machine_ts, str) and ' ' in latest_machine_ts:
                    ts_datetime = datetime.strptime(latest_machine_ts, "%Y-%m-%d %H:%M:%S")
                    ist_datetime = ts_datetime + timedelta(hours=5, minutes=30)
                    display_time = ist_datetime.strftime("%H:%M:%S")
                    suggestion_time = ist_datetime.strftime("%H:%M:%S")
                else:
                    display_time = "N/A"
                    suggestion_time = "N/A"
            except:
                display_time = "N/A"
                suggestion_time = "N/A"
            
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
        
        # Update previous config for next iteration
        previous_config = cfg.copy() if cfg else None

        time.sleep(2)


# ===== CONSOLE DISPLAY =====
def display_console():
    """Display consolidated machine data in table format"""
    while True:
        os.system('clear' if os.name == 'posix' else 'cls')  # Clear console
        
        print("=" * 180)
        print("MULTI-MACHINE MONITORING DASHBOARD")
        print("=" * 180)
        
        table = PrettyTable()
        table.field_names = ["Machine", "Status", "Time (IST)", "Cycle", "Heat (J)", "Pressure (bar)", 
                            "Temp (°C)", "Seal Quality", "Reason", "Sugg Temp", "Sugg S1", "Sugg S2"]
        table.align = "c"
        
        with data_lock:
            for machine_id in MACHINES:
                if machine_id in machine_data:
                    data = machine_data[machine_id]
                    table.add_row([
                        machine_id.upper(),
                        data['status'],
                        data['timestamp'],
                        data['cycle_id'],
                        data['heat'],
                        data['pressure'],
                        data['temp'],
                        data['seal'],
                        data['seal_reason'],
                        data['suggested_temp'],
                        data['suggested_s1'],
                        data['suggested_s2']
                    ])
                else:
                    table.add_row([machine_id.upper(), "INITIALIZING", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A"])
        
        print(table)
        current_ist = datetime.now() + timedelta(hours=5, minutes=30)
        print(f"\nLast Updated: {current_ist.strftime('%Y-%m-%d %H:%M:%S')} (IST)")
        print("Press Ctrl+C to stop monitoring")
        
        time.sleep(2)


# ===== MAIN FUNCTION =====
def main():
    """Main function to start all monitoring threads"""
    print("Starting Multi-Machine Monitoring System...")
    print(f"Monitoring machines: {', '.join(MACHINES)}")
    
    # Initialize machine data
    with data_lock:
        for machine_id in MACHINES:
            machine_data[machine_id] = {
                'status': 'INITIALIZING',
                'timestamp': 'N/A',
                'cycle_id': 'N/A',
                'heat': 'N/A',
                'pressure': 'N/A',
                'temp': 'N/A',
                'seal': 'N/A',
                'seal_reason': 'N/A',
                'suggested_temp': 'N/A',
                'suggested_s1': 'N/A',
                'suggested_s2': 'N/A'
            }
    
    # Create and start monitoring threads for each machine
    threads = []
    for machine_id in MACHINES:
        thread = threading.Thread(target=monitor_machine, args=(machine_id,), daemon=True)
        thread.start()
        threads.append(thread)
        time.sleep(0.1)  # Small delay between thread starts
    
    # Start console display thread
    display_thread = threading.Thread(target=display_console, daemon=True)
    display_thread.start()
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down monitoring system...")
        sys.exit(0)


# ===== RUN =====
if __name__ == "__main__":
    main()
