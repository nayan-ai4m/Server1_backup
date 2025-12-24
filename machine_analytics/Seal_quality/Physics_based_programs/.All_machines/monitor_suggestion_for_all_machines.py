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
import traceback
import logging
from uuid import uuid4
import multiprocessing
import os
# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

def setup_logger():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    log_dir = os.path.join("logs", today)
    os.makedirs(log_dir, exist_ok=True)

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    log_file = os.path.join(log_dir, f"{script_name}.log")

    logger = logging.getLogger("machine_logger")
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        fh = logging.FileHandler(log_file, mode="a")
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


logger = setup_logger()


def log_event(message: str, timestamp: str, machine: str):
    logger.info(f"[{machine}] [{timestamp}] {message}")


def log_error(message: str, exc: Exception = None, machine: str = ""):
    if exc:
        logger.error(
            f"[{machine}] {message} | Exception: {str(exc)}\n{traceback.format_exc()}"
        )
    else:
        logger.error(f"[{machine}] {message}")

# ===== CONFIG LOADING =====
def load_config(machine_no: str):
    filename = "config_mc"+str(machine_no)+".json"
    with open(filename, "r") as f:
        print("opened",filename)
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
    # Compare all keys, not just a few
    return current_config != previous_config


def format_event(is_state):
    return {
        "timestamp": datetime.datetime.now().isoformat(),
        "uuid": str(uuid4()),
        "active": 1 if is_state else 0,
        "filepath": "",
        "color_code": 1,
        "machine_part": "plc"
    }

# ===== DB SENDER HELPERS =====
def send_heat_event(machine_id, ts, suggestions, level):
    try:
        conn = psycopg2.connect(
            dbname="hul", user="postgres", password="ai4m2024",
            host="192.168.1.168", port="5432"
        )
        cur = conn.cursor()

        if level in ["low", "high"]:
            tp = 61
            tpdata = json.dumps(format_event(True))
            cur.execute(f"UPDATE mc{machine_id}_tp_status SET tp{tp} = %s", (tpdata,))
            conn.commit()

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
            insert_q = """INSERT INTO suggestions(
                              "timestamp", machine_number, alert_details, suggestion_details, acknowledge
                          ) VALUES (%s, %s, %s, %s, 1);"""
            params = (datetime.datetime.now(), f'mc{machine_id}', f'{{"tp":{tp}}}', json.dumps(message))
            cur.execute(insert_q, params)
            conn.commit()

            log_event(f"[MC{machine_id}] Heat issue ({level}), suggestion applied", ts, f"mc{machine_id}")

        elif level == "config_change":
            # Insert config change suggestions
            insert_q = """INSERT INTO suggestions(
                              "timestamp", machine_number, alert_details, gsm_suggestions, acknowledge
                          ) VALUES (%s, %s, %s, %s, 1);"""
            params = (
                datetime.datetime.now(),
                f"mc{machine_id}",
                json.dumps({"alert": "config changed"}),
                json.dumps(suggestions)
            )
            cur.execute(insert_q, params)
            conn.commit()
            log_event(f"[MC{machine_id}] Config change suggestions applied", ts, f"mc{machine_id}")

    except Exception as e:
        log_event(f"[MC{machine_id}] Event DB update failed: {e}", ts, f"mc{machine_id}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

def send_pressure_event(machine_id, ts, suggestions, level):
    try:
        conn = psycopg2.connect(
            dbname="hul", user="postgres", password="ai4m2024",
            host="192.168.1.168", port="5432"
        )
        cur = conn.cursor()

        if level in ["low", "high"]:
            tp = 62
            tpdata = json.dumps(format_event(True))
            cur.execute(f"UPDATE mc{machine_id}_tp_status SET tp{tp} = %s", (tpdata,))
            conn.commit()

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
            insert_q = """INSERT INTO suggestions(
                              "timestamp", machine_number, alert_details, suggestion_details, acknowledge
                          ) VALUES (%s, %s, %s, %s, 1);"""
            params = (datetime.datetime.now(), f'mc{machine_id}', f'{{"tp":{tp}}}', json.dumps(message))
            cur.execute(insert_q, params)
            conn.commit()

            log_event(f"[MC{machine_id}] Pressure issue ({level}), suggestion applied", ts, f"mc{machine_id}")

        elif level == "config_change":
            # Insert config change suggestions
            insert_q = """INSERT INTO suggestions(
                              "timestamp", machine_number, alert_details, gsm_suggestions, acknowledge
                          ) VALUES (%s, %s, %s, %s, 1);"""
            params = (
                datetime.datetime.now(),
                f"mc{machine_id}",
                json.dumps({"alert": "config changed"}),
                json.dumps(suggestions)
            )
            cur.execute(insert_q, params)
            conn.commit()
            log_event(f"[MC{machine_id}] Config change suggestions applied", ts, f"mc{machine_id}")

    except Exception as e:
        log_event(f"[MC{machine_id}] Event DB update failed: {e}", ts, f"mc{machine_id}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()



# ===== MAIN MONITORING LOOP =====
def main_loop(machine_id, refresh_sec=2):
    last_valid_pressure = None
    last_status = None
    last_suggestion = None
    previous_config = None
    
    # NEW: 2-minute wait timer variables
    wait_start_time = None
    is_waiting = False
    WAIT_DURATION = 120  # 2 minutes in seconds

    # event counters
    event_counters = {
        "low_heat": 0, "high_heat": 0,
        "low_pressure": 0, "high_pressure": 0
    }

    while True:
        print(machine_id)
        cfg = load_config(machine_id)
        config_changed = check_config_changes(cfg, previous_config)

        if config_changed:
            log_event(f"[MC{machine_id}] Config file updated", 
              datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), f"mc{machine_id}")
            cfg = load_config(machine_id)
        try:
            df = fetch_data_from_postgres(machine_id)
        except Exception as e:
            log_event(f"[MC{machine_id}] DB fetch failed: {e}", time.strftime("%Y-%m-%d %H:%M:%S"),f"mc{machine_id}")
            time.sleep(refresh_sec)
            continue

        if df.empty:
            time.sleep(refresh_sec)
            continue

        latest_machine_ts = df.iloc[0]['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
        current_status = df.iloc[0]['status']
        machine_running = (current_status == 1)

        # Machine start/stop detection and logging
        if last_status is None:
            # First iteration - just set status, no wait timer
            last_status = current_status
        elif last_status != current_status:
            if current_status == 1:
                # STOPPED → RUNNING transition
                log_event(f"[MC{machine_id}] Machine started", latest_machine_ts, f"mc{machine_id}")
                # Start 2-minute wait timer
                wait_start_time = time.time()
                is_waiting = True
                log_event(f"[MC{machine_id}] Starting 2-minute stabilization wait", latest_machine_ts, f"mc{machine_id}")
            else:
                # RUNNING → STOPPED transition
                log_event(f"[MC{machine_id}] Machine stopped", latest_machine_ts, f"mc{machine_id}")
                # Reset wait timer if it was active
                if is_waiting:
                    is_waiting = False
                    wait_start_time = None
                    log_event(f"[MC{machine_id}] Wait timer reset due to machine stop", latest_machine_ts, f"mc{machine_id}")
                # Reset all counters when machine stops
                event_counters = {
                    "low_heat": 0, "high_heat": 0,
                    "low_pressure": 0, "high_pressure": 0
                }
            last_status = current_status

        # Check if we're still in the 2-minute wait period
        if is_waiting and wait_start_time is not None:
            elapsed = time.time() - wait_start_time
            if elapsed < WAIT_DURATION:
                remaining = int(WAIT_DURATION - elapsed)
                print(f"[MC{machine_id}] [{latest_machine_ts}] WAITING: {remaining}s remaining before processing starts")
                time.sleep(refresh_sec)
                continue
            else:
                # Wait period completed
                is_waiting = False
                wait_start_time = None
                log_event(f"[MC{machine_id}] 2-minute wait completed, starting normal processing", latest_machine_ts, f"mc{machine_id}")

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
        heat_hi = cfg["GOOD_HEAT_TARGET"] * (0 + cfg["GOOD_HEAT_TOLERANCE"])
        pres_lo, pres_hi = cfg["GOOD_PRESSURE_MIN"], cfg["GOOD_PRESSURE_MAX"]

        # Latest values
        latest_heat = float(cycle_df['heat_energy'].iloc[-1]) if len(cycle_df) else np.nan
        latest_pressure = float(pressures.iloc[-1]) if len(cycle_df) else np.nan
        latest_temp = float(cycle_df['temperature_C'].iloc[-1]) if len(cycle_df) else np.nan
        latest_cycle_id = df.iloc[0]['spare1']

        suggestions = get_suggestions('mc'+str(machine_id))

        # Config change logging
        if config_changed and previous_config is not None:
            suggestions = get_suggestions('mc'+str(machine_id))
            changed_params = [f"{p}: {previous_config.get(p)} → {cfg.get(p)}"
                              for p in ['d1', 'd2', 'd3'] if cfg.get(p) != previous_config.get(p)]
            log_event(
                f"[MC{machine_id}] Config changed: {', '.join(changed_params)}. "
                f"New suggestions - Temp: {suggestions['suggested_temp']:.2f}°C, "
                f"Stroke1: {suggestions['suggested_s1']:.3f}, "
                f"Stroke2: {suggestions['suggested_s2']:.3f}",
                latest_machine_ts,f"mc{machine_id}"
            )
            send_heat_event(machine_id, latest_machine_ts, suggestions, "config_change")
           # send_pressure_event(machine_id, latest_machine_ts, suggestions, "config_change")
            last_suggestion = suggestions
        # Machine running checks
        if machine_running and not (np.isnan(latest_heat) or np.isnan(latest_pressure)):
            heat_ok = (heat_lo <= latest_heat <= heat_hi)
            pressure_ok = (pres_lo <= latest_pressure <= pres_hi)
            seal_quality = "GOOD SEAL" if (heat_ok and pressure_ok) else "BAD SEAL"

            # Heat issues
            if not heat_ok:
                if latest_heat < heat_lo:
                    event_counters["low_heat"] += 1
                    event_counters["high_heat"] = 0
                    if event_counters["low_heat"] >= 5:
                        send_heat_event(machine_id, latest_machine_ts, suggestions, "low")
                        # Reset counter after sending alert
                        event_counters["low_heat"] = 0
                elif latest_heat > heat_hi:
                    print("High heat")
                    #event_counters["high_heat"] += 1
                    #event_counters["low_heat"] = 0
                    #if event_counters["high_heat"] >= 5:
                        #send_heat_event(machine_id, latest_machine_ts, suggestions, "high")
                        #event_counters["high_heat"] = 0
            else:
                event_counters["low_heat"] = event_counters["high_heat"] = 0

            # Pressure issues
            if not pressure_ok:
                if latest_pressure < pres_lo:
                    event_counters["low_pressure"] += 1
                    event_counters["high_pressure"] = 0
                    if event_counters["low_pressure"] >= 5:
                        send_pressure_event(machine_id, latest_machine_ts, suggestions, "low")
                        # Reset counter after sending alert
                        event_counters["low_pressure"] = 0
                elif latest_pressure > pres_hi:
                    print("High Pressure")
                    #event_counters["high_pressure"] += 1
                    #event_counters["low_pressure"] = 0
                    #if event_counters["high_pressure"] >= 5:
                        #send_pressure_event(machine_id, latest_machine_ts, suggestions, "high")
                        #event_counters["high_pressure"] = 0
            else:
                event_counters["low_pressure"] = event_counters["high_pressure"] = 0

            # Suggestion logging
            if last_suggestion != suggestions and not config_changed:
                log_event(
                    f"[MC{machine_id}] New suggestion - Temp: {suggestions['suggested_temp']:.2f}°C, "
                    f"S1: {suggestions['suggested_s1']:.3f}, S2: {suggestions['suggested_s2']:.3f}",
                    latest_machine_ts,f"mc{machine_id}"
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
    machine_ids = [17,18,19,20,21,22,25,26,27,28,29,30]
    processes = []
    for mid in machine_ids:
        p = multiprocessing.Process(target=main_loop, args=(mid, 2))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
