#!/usr/bin/env python3
import sys
import json
import psycopg2
import pandas as pd
import numpy as np
import time
import warnings
import datetime
from suggestion_module import get_suggestions
from logger_module import log_event
from uuid import uuid4
import multiprocessing

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')


# ===== CONFIG LOADING =====
def load_config():
    with open("config.json", "r") as f:
        return json.load(f)


# ===== DB FETCH =====
def fetch_data_from_postgres(machine_id):
    # --- short_data_hul ---
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
    FROM public.mc{machine_id}_short_data
    ORDER BY timestamp DESC
    LIMIT 1000;
    """
    df_short = pd.read_sql_query(query_short, conn_short)
    conn_short.close()

    # --- hul (mid) ---
    conn_mid = psycopg2.connect(
        dbname="hul",
        user="postgres",
        password="ai4m2024",
        host="localhost",
        port="5432"
    )
    query_mid = f"""
    SELECT spare1,
           timestamp,
           hor_sealer_rear_1_temp,
           hor_sealer_front_1_temp
    FROM public.mc{machine_id}_mid
    ORDER BY timestamp DESC
    LIMIT 1000;
    """
    df_mid = pd.read_sql_query(query_mid, conn_mid)
    conn_mid.close()

    # Convert + merge
    df_short['timestamp'] = pd.to_datetime(df_short['timestamp'])
    df_mid['timestamp'] = pd.to_datetime(df_mid['timestamp'])
    df = pd.merge(df_short, df_mid, on='spare1', how='left', suffixes=('', '_mid'))

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
    if previous_config is None:
        return False
    for param in ['d1', 'd2', 'd3']:
        if current_config.get(param) != previous_config.get(param):
            return True
    return False


def format_event(is_state):
    return {
        "timestamp": datetime.datetime.now().isoformat(),
        "uuid": str(uuid4()),
        "active": 1 if is_state else 0,
        "filepath": "",
        "color_code": 1,
        "machine_part": "plc"
    }


# ===== MAIN MONITORING LOOP =====
def main_loop(machine_id, refresh_sec=2):
    last_valid_pressure = None
    last_status = None
    last_suggestion = None
    previous_config = None

    heat_issue_logged = None
    pressure_issue_logged = None

    while True:
        cfg = load_config()
        config_changed = check_config_changes(cfg, previous_config)

        try:
            df = fetch_data_from_postgres(machine_id)
        except Exception as e:
            log_event(f"[MC{machine_id}] DB fetch failed: {e}", time.strftime("%Y-%m-%d %H:%M:%S"))
            time.sleep(refresh_sec)
            continue

        if df.empty:
            time.sleep(refresh_sec)
            continue

        latest_machine_ts = df.iloc[0]['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
        current_status = df.iloc[0]['status']
        machine_running = (current_status == 1)

        # Machine start/stop logging
        if last_status is None:
            last_status = current_status
        elif last_status != current_status:
            if current_status == 1:
                log_event(f"[MC{machine_id}] Machine started", latest_machine_ts)
            else:
                log_event(f"[MC{machine_id}] Machine stopped", latest_machine_ts)
            last_status = current_status

        # Process cycles
        cycle_df = process_data(df, cfg)

        # Fill NaN pressures
        pressures = cycle_df['pressure_avg'].copy()
        for i in range(len(pressures)):
            if pd.isna(pressures.iloc[i]):
                if last_valid_pressure is not None:
                    pressures.iloc[i] = last_valid_pressure
            else:
                last_valid_pressure = pressures.iloc[i]
        cycle_df['pressure_avg'] = pressures

        # Thresholds
        heat_lo = cfg["GOOD_HEAT_TARGET"] * (1 - cfg["GOOD_HEAT_TOLERANCE"])
        heat_hi = cfg["GOOD_HEAT_TARGET"] * (1 + cfg["GOOD_HEAT_TOLERANCE"])
        pres_lo, pres_hi = cfg["GOOD_PRESSURE_MIN"], cfg["GOOD_PRESSURE_MAX"]

        # Latest values
        latest_heat = float(cycle_df['heat_energy'].iloc[-1]) if len(cycle_df) else np.nan
        latest_pressure = float(pressures.iloc[-1]) if len(cycle_df) else np.nan
        latest_temp = float(cycle_df['temperature_C'].iloc[-1]) if len(cycle_df) else np.nan
        latest_cycle_id = df.iloc[0]['spare1']

        suggestions = get_suggestions()

        # Config change logging
        if config_changed and previous_config is not None:
            changed_params = [f"{p}: {previous_config.get(p)} → {cfg.get(p)}"
                              for p in ['d1', 'd2', 'd3'] if cfg.get(p) != previous_config.get(p)]
            log_event(
                f"[MC{machine_id}] Config changed: {', '.join(changed_params)}. "
                f"New suggestions - Temp: {suggestions['suggested_temp']:.2f}°C, "
                f"Stroke1: {suggestions['suggested_s1']:.3f}, "
                f"Stroke2: {suggestions['suggested_s2']:.3f}",
                latest_machine_ts
            )
            last_suggestion = suggestions

        # Machine running checks
        if machine_running and not (np.isnan(latest_heat) or np.isnan(latest_pressure)):
            heat_ok = (heat_lo <= latest_heat <= heat_hi)
            pressure_ok = (pres_lo <= latest_pressure <= pres_hi)
            seal_quality = "GOOD SEAL" if (heat_ok and pressure_ok) else "BAD SEAL"

            # Heat issues
            if not heat_ok:
                if latest_heat < heat_lo and heat_issue_logged != "low":
                    try:
                        tpdata = json.dumps(format_event(True))
                        event_conn = psycopg2.connect(
                            dbname="hul",
                            user="postgres",
                            password="ai4m2024",
                            host="192.168.1.168",
                            port="5432"
                        )
                        event_cursor = event_conn.cursor()
                        query = f"""UPDATE mc{machine_id}_tp_status SET tp61 = %s"""
                        event_cursor.execute(query, (tpdata,))
                        event_conn.commit()

                        s1 = round(suggestions['suggested_s1'], 2)
                        s2 = round(suggestions['suggested_s2'], 2)
                        temp_s = round(suggestions['suggested_temp'], 1)
                        message = {
                            "suggestion": {
                                "HMI_Hor_Sealer_Strk_1": s1,
                                "HMI_Hor_Sealer_Strk_2": s2,
                                "text": f"Change Stroke 1 to {s1}, Stroke 2 to {s2}, Temp to {temp_s}",
                                "HMI_Hor_Seal_Front_28": temp_s,
                                "HMI_Hor_Seal_Front_27": temp_s
                            }
                        }
                        print(machine_id,message)
                        insert_q = """INSERT INTO suggestions("timestamp", machine_number, alert_details, suggestion_details, acknowledge) VALUES (%s, %s, %s, %s, 1);"""
                        params = (datetime.datetime.now(), f'mc{machine_id}', '{"tp":61}', json.dumps(message))
                        event_cursor.execute(insert_q, params)
                        event_conn.commit()
                    except Exception as e:
                        log_event(f"[MC{machine_id}] Event DB update failed: {e}", latest_machine_ts)
                    finally:
                        if 'event_cursor' in locals():
                            event_cursor.close()
                        if 'event_conn' in locals():
                            event_conn.close()
                    log_event(f"[MC{machine_id}] Low heat energy, adjust temp to {suggestions['suggested_temp']:.2f}", latest_machine_ts)
                    heat_issue_logged = "low"

                elif latest_heat > heat_hi and heat_issue_logged != "high":
                    log_event(f"[MC{machine_id}] High heat energy, adjust temp to {suggestions['suggested_temp']:.2f}", latest_machine_ts)
                    heat_issue_logged = "high"
            else:
                heat_issue_logged = None

            # Pressure issues
            if not pressure_ok:
                if latest_pressure < pres_lo and pressure_issue_logged != "low":
                    try:
                        tpdata = json.dumps(format_event(True))
                        event_conn = psycopg2.connect(
                            dbname="hul",
                            user="postgres",
                            password="ai4m2024",
                            host="192.168.1.168",
                            port="5432"
                        )
                        event_cursor = event_conn.cursor()
                        query = f"""UPDATE mc{machine_id}_tp_status SET tp62 = %s"""
                        event_cursor.execute(query, (tpdata,))
                        event_conn.commit()

                        s1 = round(suggestions['suggested_s1'], 2)
                        s2 = round(suggestions['suggested_s2'], 2)
                        temp_s = round(suggestions['suggested_temp'], 1)
                        message = {
                            "suggestion": {
                                "HMI_Hor_Sealer_Strk_1": s1,
                                "HMI_Hor_Sealer_Strk_2": s2,
                                "text": f"Change Stroke 1 to {s1}, Stroke 2 to {s2}, Temp to {temp_s}",
                                "HMI_Hor_Seal_Front_28": temp_s,
                                "HMI_Hor_Seal_Front_27": temp_s
                            }
                        }
                        print(machine_id,message)
                        insert_q = """INSERT INTO suggestions("timestamp", machine_number, alert_details, suggestion_details, acknowledge) VALUES (%s, %s, %s, %s, 1);"""
                        params = (datetime.datetime.now(), f'mc{machine_id}', '{"tp":62}', json.dumps(message))
                        event_cursor.execute(insert_q, params)
                        event_conn.commit()
                    except Exception as e:
                        log_event(f"[MC{machine_id}] Event DB update failed: {e}", latest_machine_ts)
                    finally:
                        if 'event_cursor' in locals():
                            event_cursor.close()
                        if 'event_conn' in locals():
                            event_conn.close()
                    log_event(f"[MC{machine_id}] Low pressure, adjust strokes", latest_machine_ts)
                    pressure_issue_logged = "low"
                elif latest_pressure > pres_hi and pressure_issue_logged != "high":
                    log_event(f"[MC{machine_id}] High pressure, adjust strokes", latest_machine_ts)
                    pressure_issue_logged = "high"
            else:
                pressure_issue_logged = None

            # Suggestion logging
            if last_suggestion != suggestions and not config_changed:
                log_event(
                    f"[MC{machine_id}] New suggestion - Temp: {suggestions['suggested_temp']:.2f}°C, "
                    f"S1: {suggestions['suggested_s1']:.3f}, S2: {suggestions['suggested_s2']:.3f}",
                    latest_machine_ts
                )
                last_suggestion = suggestions
        else:
            seal_quality = "N/A"

        # Console output
        print(f"[MC{machine_id}] [{latest_machine_ts}] Cycle {latest_cycle_id}, "
              f"Status: {'RUNNING' if machine_running else 'STOPPED'}, "
              f"Heat: {latest_heat:.2f}, Pressure: {latest_pressure:.2f}, "
              f"Temp: {latest_temp:.2f}, Seal: {seal_quality}")

        previous_config = cfg.copy()
        time.sleep(refresh_sec)


# ===== RUN ALL MACHINES =====
if __name__ == "__main__":
    machine_ids = [17, 18, 19, 20, 21, 22]
    processes = []
    for mid in machine_ids:
        p = multiprocessing.Process(target=main_loop, args=(mid, 2))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
