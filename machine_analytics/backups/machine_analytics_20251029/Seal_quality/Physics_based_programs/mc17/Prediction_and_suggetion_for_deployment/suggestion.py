import sys
import json
import psycopg2
import pandas as pd
import numpy as np
import time
import warnings
import uuid
import datetime
import pytz

from suggestion_module import get_suggestions
from logger_module import log_event

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# ===== CONFIG LOADING =====
def load_config():
    with open("config.json", "r") as f:
        return json.load(f)

# ===== DB FETCH =====
def fetch_data_from_postgres():
    # --- Connect to short_data_hul (for mc22_short_data) ---
    conn_short = psycopg2.connect(
        dbname="short_data_hul",
        user="postgres",
        password="ai4m2024",
        host="localhost",
        port="5432"
    )
    query_short = """
    SELECT cam_position,
           spare1,
           status,
           timestamp,
           hor_pressure
    FROM public.mc22_short_data
    ORDER BY timestamp DESC
    LIMIT 1000;
    """
    df_short = pd.read_sql_query(query_short, conn_short)
    conn_short.close()

    # --- Connect to hul (for mc22_mid) ---
    conn_mid = psycopg2.connect(
        dbname="hul",
        user="postgres",
        password="ai4m2024",
        host="localhost",
        port="5432"
    )
    query_mid = """
    SELECT spare1,
           timestamp,
           hor_sealer_rear_1_temp,
           hor_sealer_front_1_temp
    FROM public.mc22_mid
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

# ===== INSERT SUGGESTION INTO DATABASE =====
def insert_suggestion(alert_details, suggestion_details, tp):
    try:
        kolkata_tz = pytz.timezone('Asia/Kolkata')
        current_time = datetime.datetime.now(kolkata_tz).strftime("%Y-%m-%d %H:%M:%S")
        conn = psycopg2.connect(
            dbname="hul",
            user="postgres",
            password="ai4m2024",
            host="100.64.84.30",
            port="5432"
        )
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.suggestions (timestamp, machine_number, alert_details, suggestion_details, acknowledge)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (current_time, 'mc22', json.dumps(alert_details), json.dumps(suggestion_details), 1)
        )
        conn.commit()
        cur.close()
        conn.close()
        log_event(f"Inserted suggestion into database: {suggestion_details}", current_time)

        # Update mc22_tp_status with current Asia/Kolkata timestamp
        update_tp_status(tp, 1, current_time.replace(" ", "T") + ".000000+05:30")
    except Exception as e:
        kolkata_tz = pytz.timezone('Asia/Kolkata')
        log_event(f"Failed to insert suggestion into database: {e}", datetime.datetime.now(kolkata_tz).strftime("%Y-%m-%d %H:%M:%S"))

# ===== UPDATE TP STATUS =====
def update_tp_status(tp, active, timestamp=None):
    try:
        kolkata_tz = pytz.timezone('Asia/Kolkata')
        current_time = datetime.datetime.now(kolkata_tz).isoformat()
        conn = psycopg2.connect(
            dbname="hul",
            user="postgres",
            password="ai4m2024",
            host="100.64.84.30",
            port="5432"
        )
        cur = conn.cursor()

        tp_column = f"tp{tp}"

        if active == 1:
            if timestamp is None:
                timestamp = current_time
            uuid_str = str(uuid.uuid4())
            json_data = {
                "uuid": uuid_str,
                "active": 1,
                "filepath": "",
                "timestamp": timestamp,
                "color_code": 1,
                "machine_part": "plc",
                "updated_timestamp": current_time
            }
            # Update the tpXX column if a row exists
            cur.execute(
                f"UPDATE public.mc22_tp_status SET {tp_column} = %s::jsonb",
                (json.dumps(json_data),)
            )
            if cur.rowcount == 0:
                # Insert a new row if no row exists
                cur.execute(
                    f"INSERT INTO public.mc22_tp_status ({tp_column}) VALUES (%s::jsonb)",
                    (json.dumps(json_data),)
                )
        else:
            # Set active to 0 if data exists
            cur.execute(
                f"SELECT {tp_column} FROM public.mc22_tp_status"
            )
            row = cur.fetchone()
            if row and row[0]:
                current_json = row[0]
                current_json['active'] = 0
                current_json['updated_timestamp'] = current_time
                cur.execute(
                    f"UPDATE public.mc22_tp_status SET {tp_column} = %s::jsonb",
                    (json.dumps(current_json),)
                )

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        kolkata_tz = pytz.timezone('Asia/Kolkata')
        log_event(f"Failed to update mc22_tp_status: {e}", datetime.datetime.now(kolkata_tz).strftime("%Y-%m-%d %H:%M:%S"))

# ===== MAIN MONITORING LOOP =====
def main_loop(refresh_sec=2):
    last_valid_pressure = None
    last_status = None
    last_suggestion = None
    previous_config = None

    heat_issue_logged = None  # None, "low", "high"
    pressure_issue_logged = None  # None, "low", "high"

    while True:
        cfg = load_config()

        # Check for config changes in d1, d2, d3
        config_changed = check_config_changes(cfg, previous_config)

        try:
            df = fetch_data_from_postgres()
        except Exception as e:
            kolkata_tz = pytz.timezone('Asia/Kolkata')
            log_event(f"DB fetch failed: {e}", datetime.datetime.now(kolkata_tz).strftime("%Y-%m-%d %H:%M:%S"))
            time.sleep(refresh_sec)
            continue

        if df.empty:
            print("No data")
            time.sleep(refresh_sec)
            continue

        latest_machine_ts = df.iloc[0]['timestamp'].strftime("%Y-%m-%d %H:%M:%S")

        current_status = df.iloc[0]['status']
        machine_running = (current_status == 1)

        # Log start/stop
        if last_status is None:
            last_status = current_status
        elif last_status != current_status:
            if current_status == 1:
                log_event("Machine started", latest_machine_ts)
            else:
                log_event("Machine stopped", latest_machine_ts)
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
        suggestions = get_suggestions()

        # === CONFIG CHANGE SUGGESTIONS (Works regardless of machine status) ===
        if config_changed and previous_config is not None:
            changed_params = []
            for param in ['d1', 'd2', 'd3']:
                if cfg.get(param) != previous_config.get(param):
                    changed_params.append(f"{param}: {previous_config.get(param)} → {cfg.get(param)}")

            kolkata_tz = pytz.timezone('Asia/Kolkata')
            current_time = datetime.datetime.now(kolkata_tz).strftime("%Y-%m-%d %H:%M:%S")
            log_event(
                f"Config parameter(s) changed: {', '.join(changed_params)}. "
                f"New suggestions - Temp: {suggestions['suggested_temp']:.2f}°C, "
                f"Stroke1: {suggestions['suggested_s1']:.3f}, "
                f"Stroke2: {suggestions['suggested_s2']:.3f}",
                current_time
            )
            last_suggestion = suggestions  # Update to prevent duplicate logging

        # === MACHINE RUNNING SUGGESTIONS ===
        # Seal quality (only when machine is running)
        if machine_running and not (np.isnan(latest_heat) or np.isnan(latest_pressure)):
            heat_ok = (heat_lo <= latest_heat <= heat_hi)
            pressure_ok = (pres_lo <= latest_pressure <= pres_hi)
            seal_quality = "GOOD SEAL" if (heat_ok and pressure_ok) else "BAD SEAL"

            # --- Combined heat and pressure suggestion ---
            if not heat_ok and not pressure_ok:
                # Both heat and pressure are out of range
                if ((latest_heat < heat_lo and pressure_issue_logged != "low" and heat_issue_logged != "low") or
                    (latest_heat > heat_hi and pressure_issue_logged != "high" and heat_issue_logged != "high")):
                    kolkata_tz = pytz.timezone('Asia/Kolkata')
                    current_time = datetime.datetime.now(kolkata_tz).strftime("%Y-%m-%d %H:%M:%S")
                    log_event(
                        f"Combined issue: Heat energy {'low' if latest_heat < heat_lo else 'high'}, "
                        f"Pressure {'low' if latest_pressure < pres_lo else 'high'}, "
                        f"change temperature to {suggestions['suggested_temp']:.2f}, "
                        f"stroke1 to {suggestions['suggested_s1']:.3f}, stroke2 to {suggestions['suggested_s2']:.3f}",
                        current_time
                    )
                    insert_suggestion(
                        {"tp": 69},
                        {
                            "suggestion": {
                                "HMI_Hor_Seal_Front_27": suggestions['suggested_temp'],
                                "HMI_Hor_Seal_Front_28": suggestions['suggested_temp'],
                                "HMI_Hor_Sealer_Strk_1": suggestions['suggested_s1'],
                                "HMI_Hor_Sealer_Strk_2": suggestions['suggested_s2']
                            }
                        },
                        69
                    )
                    heat_issue_logged = "low" if latest_heat < heat_lo else "high"
                    pressure_issue_logged = "low" if latest_pressure < pres_lo else "high"

            # --- Individual heat energy log ---
            elif not heat_ok:
                if latest_heat < heat_lo and heat_issue_logged != "low":
                    kolkata_tz = pytz.timezone('Asia/Kolkata')
                    current_time = datetime.datetime.now(kolkata_tz).strftime("%Y-%m-%d %H:%M:%S")
                    log_event(f"Low heat energy, change the temperature to {suggestions['suggested_temp']:.2f}", current_time)
                    insert_suggestion(
                        {"tp": 62},
                        {"suggestion": {
                            "HMI_Hor_Seal_Front_27": suggestions['suggested_temp'],
                            "HMI_Hor_Seal_Front_28": suggestions['suggested_temp']
                        }},
                        62
                    )
                    heat_issue_logged = "low"
                elif latest_heat > heat_hi and heat_issue_logged != "high":
                    kolkata_tz = pytz.timezone('Asia/Kolkata')
                    current_time = datetime.datetime.now(kolkata_tz).strftime("%Y-%m-%d %H:%M:%S")
                    log_event(f"High heat energy, change the temperature to {suggestions['suggested_temp']:.2f}", current_time)
                    insert_suggestion(
                        {"tp": 62},
                        {"suggestion": {
                            "HMI_Hor_Seal_Front_27": suggestions['suggested_temp'],
                            "HMI_Hor_Seal_Front_28": suggestions['suggested_temp']
                        }},
                        62
                    )
                    heat_issue_logged = "high"

            # --- Individual pressure log ---
            elif not pressure_ok:
                if latest_pressure < pres_lo and pressure_issue_logged != "low":
                    kolkata_tz = pytz.timezone('Asia/Kolkata')
                    current_time = datetime.datetime.now(kolkata_tz).strftime("%Y-%m-%d %H:%M:%S")
                    log_event(
                        f"Low pressure, change stroke1 to {suggestions['suggested_s1']:.3f}, stroke2 to {suggestions['suggested_s2']:.3f}",
                        current_time
                    )
                    insert_suggestion(
                        {"tp": 61},
                        {"suggestion": {
                            "HMI_Hor_Sealer_Strk_1": suggestions['suggested_s1'],
                            "HMI_Hor_Sealer_Strk_2": suggestions['suggested_s2']
                        }},
                        61
                    )
                    pressure_issue_logged = "low"
                elif latest_pressure > pres_hi and pressure_issue_logged != "high":
                    kolkata_tz = pytz.timezone('Asia/Kolkata')
                    current_time = datetime.datetime.now(kolkata_tz).strftime("%Y-%m-%d %H:%M:%S")
                    log_event(
                        f"High pressure, change stroke1 to {suggestions['suggested_s1']:.3f}, stroke2 to {suggestions['suggested_s2']:.3f}",
                        current_time
                    )
                    insert_suggestion(
                        {"tp": 61},
                        {"suggestion": {
                            "HMI_Hor_Sealer_Strk_1": suggestions['suggested_s1'],
                            "HMI_Hor_Sealer_Strk_2": suggestions['suggested_s2']
                        }},
                        61
                    )
                    pressure_issue_logged = "high"

            # --- Clear logged issues if conditions are met ---
            else:
                if heat_issue_logged is not None:
                    update_tp_status(62, 0)
                    update_tp_status(69, 0)
                heat_issue_logged = None
                if pressure_issue_logged is not None:
                    update_tp_status(61, 0)
                    update_tp_status(69, 0)
                pressure_issue_logged = None

            # --- Suggestion log (only for non-config changes) ---
            if last_suggestion != suggestions and not config_changed:
                kolkata_tz = pytz.timezone('Asia/Kolkata')
                current_time = datetime.datetime.now(kolkata_tz).strftime("%Y-%m-%d %H:%M:%S")
                log_event(
                    "New suggestion given - "
                    f"Temp: {suggestions['suggested_temp']:.2f}°C, "
                    f"Stroke1: {suggestions['suggested_s1']:.3f}, "
                    f"Stroke2: {suggestions['suggested_s2']:.3f}",
                    current_time
                )
                last_suggestion = suggestions
        else:
            seal_quality = "N/A"

        # Console print
        print(f"[{latest_machine_ts}] [Cycle {latest_cycle_id}] Status: {'RUNNING' if machine_running else 'STOPPED'}, "
              f"Heat: {latest_heat:.2f} J, Pressure: {latest_pressure:.2f} bar, Temp: {latest_temp:.2f}°C, "
              f"Seal: {seal_quality}")

        # Update previous config for next iteration
        previous_config = cfg.copy()

        time.sleep(refresh_sec)

# ===== RUN =====
if __name__ == "__main__":
    main_loop(refresh_sec=2)
