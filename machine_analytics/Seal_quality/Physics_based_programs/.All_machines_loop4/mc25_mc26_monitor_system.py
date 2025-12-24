#!/usr/bin/env python3
"""
S2 Quality Monitor for MC25 & MC26
Monitors seal quality (S2) and generates alerts based on torque and heat energy
"""

import sys
import json
import psycopg2
import pandas as pd
import numpy as np
import time
import warnings
import datetime
from suggestion_module_v2 import get_suggestions
import traceback
import logging
from uuid import uuid4
import multiprocessing
import os
import math
from statistics import mean

# Suppress pandas SQLAlchemy warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

def setup_logger():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    log_dir = os.path.join("logs", today)
    os.makedirs(log_dir, exist_ok=True)

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    log_file = os.path.join(log_dir, f"{script_name}.log")

    logger = logging.getLogger("s2_monitor_logger")
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
    filename = f"config_mc{machine_no}.json"
    with open(filename, "r") as f:
        print(f"Opened {filename}")
        return json.load(f)

# ===== DB FETCH =====
def fetch_data_from_postgres(machine_id):
    """Fetch torque and temperature data from postgres"""
    
    # Database configurations
    READ_DB_CONFIG = {
        "dbname": "hul",
        "user": "postgres",
        "password": "ai4m2024",
        "host": "100.96.244.68",
        "port": "5432"
    }
    
    # --- Fetch torque data from fast table ---
    conn_fast = psycopg2.connect(**READ_DB_CONFIG)
    query_fast = f"""
    SELECT timestamp, 
           horizantal_motor_tq as torque,
           cam_position,
           status,
           cycle_id
    FROM mc{machine_id}_fast
    ORDER BY timestamp DESC
    LIMIT 1000;
    """
    df_fast = pd.read_sql_query(query_fast, conn_fast)
    conn_fast.close()

    # --- Fetch temperature data from mid table ---
    conn_mid = psycopg2.connect(**READ_DB_CONFIG)
    query_mid = f"""
    SELECT timestamp,
           hor_temp_27_pv_front,
           hor_temp_28_pv_rear
    FROM mc{machine_id}_mid
    ORDER BY timestamp DESC
    LIMIT 100;
    """
    df_mid = pd.read_sql_query(query_mid, conn_mid)
    conn_mid.close()

    # Convert timestamps
    df_fast['timestamp'] = pd.to_datetime(df_fast['timestamp'])
    df_mid['timestamp'] = pd.to_datetime(df_mid['timestamp'])
    
    # Calculate average temperature
    df_mid['avg_temp'] = (df_mid['hor_temp_27_pv_front'] + df_mid['hor_temp_28_pv_rear']) / 2.0
    
    # Merge on closest timestamp
    df_fast = df_fast.sort_values('timestamp')
    df_mid = df_mid.sort_values('timestamp')
    
    # Use merge_asof for time-based join
    df = pd.merge_asof(df_fast, df_mid[['timestamp', 'avg_temp']], 
                       on='timestamp', 
                       direction='nearest',
                       tolerance=pd.Timedelta('5s'))
    
    df = df.sort_values('timestamp', ascending=False).reset_index(drop=True)
    
    return df

# ===== HEAT ENERGY CALCULATION =====
def calculate_heat_energy(T_C, cfg):
    """Calculate heat energy based on temperature"""
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

# ===== S2 CALCULATION =====
def calculate_s2(heat_energy, torque, cfg):
    """Calculate S2 seal quality metric"""
    if heat_energy is None or torque is None:
        return None
    
    # S2 coefficients
    a = cfg.get("seal_a", 0.35)  # Coefficient for heat
    b = cfg.get("seal_b", 0.4)   # Coefficient for torque
    c = cfg.get("seal_c", 0.25)  # Coefficient for sealing time
    
    # Targets
    heat_target = cfg.get("HEAT_TARGET", 315.0)
    torque_target = cfg.get("TORQUE_TARGET", 2.5)
    sealing_time_target = cfg.get("SEALING_TIME_TARGET", 0.2)
    sealing_time = cfg.get("sealing_time", 0.2)  # Current sealing time
    
    S2 = (a * (heat_energy / heat_target) +
          b * (torque / torque_target) +
          c * (sealing_time / sealing_time_target))
    
    return S2

# ===== PROCESS DATA =====
def process_data(df, cfg):
    """Process data to calculate metrics using timestamp-based grouping"""
    df = df.copy()
    
    # Group by 1-second time windows for averaging
    df['time_window'] = df['timestamp'].dt.floor('1s')
    
    # Calculate average torque for cam positions 160-240 (similar to pressure in Code A)
    torque_per_window = (
        df[(df['cam_position'] >= 160) & (df['cam_position'] <= 240)]
        .groupby('time_window')['torque']
        .mean()
        .reset_index(name='torque_avg')
    )
    
    # Per-window temperature and last timestamp
    window_data = (
        df.groupby('time_window', dropna=True)
          .agg({
              'avg_temp': 'mean',
              'timestamp': 'max',
              'status': 'last'
          })
          .reset_index()
          .rename(columns={'timestamp': 'last_ts'})
    )
    
    window_data = window_data.merge(torque_per_window, on='time_window', how='left')
    
    # Calculate heat energy
    window_data['heat_energy'] = window_data['avg_temp'].apply(
        lambda temp: calculate_heat_energy(temp, cfg) if pd.notna(temp) else None
    )
    
    # Calculate S2
    window_data['s2'] = window_data.apply(
        lambda row: calculate_s2(row['heat_energy'], row['torque_avg'], cfg), axis=1
    )
    
    window_data = window_data.sort_values('last_ts', ascending=True).reset_index(drop=True)
    return window_data

# ===== CONFIG CHANGE DETECTION =====
def check_config_changes(current_config, previous_config):
    if previous_config is None:
        return False
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
def send_low_heat_event(machine_id, ts, suggestions):
    """Send alert for low heat energy"""
    try:
        conn = psycopg2.connect(
            dbname="hul", user="postgres", password="ai4m2024",
            host="192.168.1.168", port="5432"
        )
        cur = conn.cursor()

        tp = 61  # TP for heat issues
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
                "text": f"Low Heat: Change Stroke 1 to {s1}, Stroke 2 to {s2}, Temp to {temp_s}",
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

        log_event(f"Low heat energy alert sent, suggestion applied", ts, f"mc{machine_id}")

    except Exception as e:
        log_error(f"Event DB update failed", e, f"mc{machine_id}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

def send_low_torque_event(machine_id, ts, suggestions):
    """Send alert for low torque"""
    try:
        conn = psycopg2.connect(
            dbname="hul", user="postgres", password="ai4m2024",
            host="192.168.1.168", port="5432"
        )
        cur = conn.cursor()

        tp = 62  # TP for torque issues (same as pressure)
        tpdata = json.dumps(format_event(True))
        cur.execute(f"UPDATE mc{machine_id}_tp_status SET tp{tp} = %s", (tpdata,))
        conn.commit()

        s1 = round(suggestions['suggested_s1'], 2)
        s2 = round(suggestions['suggested_s2'], 2)
        temp_s = round(suggestions["suggested_temp"], 1)

        message = {
            "suggestion": {
                "HMI_Hor_Sealer_Strk_1": s1,
                "HMI_Hor_Sealer_Strk_2": s2,
                "text": f"Low Torque: Change Stroke 1 to {s1}, Stroke 2 to {s2}, Temp to {temp_s}",
                "HMI_Hor_Seal_Front_28": temp_s,
                "HMI_Hor_Seal_Front_27": temp_s,
            }
        }
        
        insert_q = """INSERT INTO suggestions(
                          "timestamp", machine_number, alert_details, suggestion_details, acknowledge
                      ) VALUES (%s, %s, %s, %s, 1);"""
        params = (datetime.datetime.now(), f'mc{machine_id}', f'{{"tp":{tp}}}', json.dumps(message))
        cur.execute(insert_q, params)
        conn.commit()

        log_event(f"Low torque alert sent, suggestion applied", ts, f"mc{machine_id}")

    except Exception as e:
        log_error(f"Event DB update failed", e, f"mc{machine_id}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

def send_config_change_event(machine_id, ts, suggestions):
    """Send config change suggestions"""
    try:
        conn = psycopg2.connect(
            dbname="hul", user="postgres", password="ai4m2024",
            host="192.168.1.168", port="5432"
        )
        cur = conn.cursor()

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
        
        log_event(f"Config change suggestions applied", ts, f"mc{machine_id}")

    except Exception as e:
        log_error(f"Config change event DB update failed", e, f"mc{machine_id}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

# ===== MAIN MONITORING LOOP =====
def main_loop(machine_id, refresh_sec=2):
    """Main monitoring loop for a single machine"""
    
    # State variables
    last_status = None
    last_suggestion = None
    previous_config = None
    machine_start_time = None
    startup_delay_complete = False
    
    # Event counters
    event_counters = {
        "low_s2": 0,
        "low_heat": 0,
        "low_torque": 0
    }
    
    # Thresholds
    S2_THRESHOLD = 0.75
    MIN_TORQUE = 1.5
    MIN_HEAT = 600  # Minimum temperature translates to heat energy
    
    while True:
        try:
            # Load configuration
            cfg = load_config(machine_id)
            config_changed = check_config_changes(cfg, previous_config)
            
            if config_changed and previous_config is not None:
                log_event(f"Config file updated", 
                         datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                         f"mc{machine_id}")
                
                # Get new suggestions based on updated config
                suggestions = get_suggestions(f'mc{machine_id}')
                send_config_change_event(machine_id, 
                                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                        suggestions)
                last_suggestion = suggestions
            
            # Fetch data
            try:
                df = fetch_data_from_postgres(machine_id)
            except Exception as e:
                log_error(f"DB fetch failed", e, f"mc{machine_id}")
                time.sleep(refresh_sec)
                continue
            
            if df.empty:
                time.sleep(refresh_sec)
                continue
            
            # Get machine status
            latest_machine_ts = df.iloc[0]['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
            current_status = df.iloc[0]['status']
            machine_running = (current_status == 1)
            
            # Machine start/stop detection with 2-minute startup delay
            if last_status is None:
                last_status = current_status
                if current_status == 1:
                    machine_start_time = time.time()
            elif last_status != current_status:
                if current_status == 1:
                    log_event(f"Machine started, waiting 2 minutes for stabilization...", 
                             latest_machine_ts, f"mc{machine_id}")
                    machine_start_time = time.time()
                    startup_delay_complete = False
                else:
                    log_event(f"Machine stopped", latest_machine_ts, f"mc{machine_id}")
                    startup_delay_complete = False
                    # Reset counters when machine stops
                    for key in event_counters:
                        event_counters[key] = 0
                last_status = current_status
            
            # Check if startup delay is complete
            if machine_running and machine_start_time is not None:
                if not startup_delay_complete:
                    elapsed = time.time() - machine_start_time
                    if elapsed >= 120:  # 2 minutes
                        startup_delay_complete = True
                        log_event(f"Startup delay complete, beginning monitoring", 
                                 latest_machine_ts, f"mc{machine_id}")
                    else:
                        remaining = 120 - elapsed
                        print(f"[MC{machine_id}] Waiting for startup: {remaining:.0f} seconds remaining...")
                        time.sleep(refresh_sec)
                        continue
            
            # Process data if machine is running and startup delay is complete
            if machine_running and startup_delay_complete:
                # Process window-based data
                window_df = process_data(df, cfg)
                
                if not window_df.empty:
                    # Get latest values
                    latest_row = window_df.iloc[-1]
                    latest_s2 = latest_row['s2']
                    latest_heat_energy = latest_row['heat_energy']
                    latest_torque = latest_row['torque_avg']
                    latest_temp = latest_row['avg_temp']
                    
                    # Get suggestions
                    suggestions = get_suggestions(f'mc{machine_id}')
                    
                    # Check S2 threshold
                    if pd.notna(latest_s2):
                        print(f"[MC{machine_id}] [{latest_machine_ts}] "
                              f"S2: {latest_s2:.3f}, "
                              f"Heat: {latest_heat_energy:.2f}, "
                              f"Torque: {latest_torque:.2f}, "
                              f"Temp: {latest_temp:.2f}")
                        
                        if latest_s2 < S2_THRESHOLD:
                            event_counters["low_s2"] += 1
                            
                            # Determine which component is lower relative to target
                            heat_ratio = latest_heat_energy / cfg.get("HEAT_TARGET", 315.0)
                            torque_ratio = latest_torque / cfg.get("TORQUE_TARGET", 2.5)
                            
                            # Find which is further from target
                            if heat_ratio < torque_ratio:
                                # Heat is the limiting factor
                                event_counters["low_heat"] += 1
                                event_counters["low_torque"] = 0  # Reset torque counter
                                
                                print(f"[MC{machine_id}] Low S2 due to heat: {latest_heat_energy:.2f} "
                                      f"(ratio: {heat_ratio:.3f})")
                                
                                if event_counters["low_heat"] >= 5:
                                    log_event(f"Low heat energy detected (5 consecutive), sending alert",
                                             latest_machine_ts, f"mc{machine_id}")
                                    send_low_heat_event(machine_id, latest_machine_ts, suggestions)
                                    event_counters["low_heat"] = 0  # Reset after alert
                                    
                            else:
                                # Torque is the limiting factor
                                event_counters["low_torque"] += 1
                                event_counters["low_heat"] = 0  # Reset heat counter
                                
                                print(f"[MC{machine_id}] Low S2 due to torque: {latest_torque:.2f} "
                                      f"(ratio: {torque_ratio:.3f})")
                                
                                if event_counters["low_torque"] >= 5:
                                    log_event(f"Low torque detected (5 consecutive), sending alert",
                                             latest_machine_ts, f"mc{machine_id}")
                                    send_low_torque_event(machine_id, latest_machine_ts, suggestions)
                                    event_counters["low_torque"] = 0  # Reset after alert
                        else:
                            # S2 is good, reset all counters
                            for key in event_counters:
                                event_counters[key] = 0
                            
                            print(f"[MC{machine_id}] S2 GOOD: {latest_s2:.3f}")
                    
                    # Log suggestion changes
                    if last_suggestion != suggestions:
                        log_event(
                            f"New suggestion - Temp: {suggestions['suggested_temp']:.2f}°C, "
                            f"S1: {suggestions['suggested_s1']:.3f}, S2: {suggestions['suggested_s2']:.3f}",
                            latest_machine_ts, f"mc{machine_id}"
                        )
                        last_suggestion = suggestions
            
            else:
                if not machine_running:
                    print(f"[MC{machine_id}] Machine stopped")
                elif not startup_delay_complete:
                    print(f"[MC{machine_id}] Waiting for startup delay...")
            
            # Update previous config
            previous_config = cfg.copy()
            
            # Wait before next iteration
            time.sleep(refresh_sec)
            
        except Exception as e:
            log_error(f"Error in main loop", e, f"mc{machine_id}")
            time.sleep(refresh_sec)

# ===== RUN ALL MACHINES =====
if __name__ == "__main__":
    machine_ids = [25, 26]  # Monitor MC25 and MC26
    processes = []
    
    print("Starting S2 Quality Monitor for MC25 and MC26...")
    print("Press Ctrl+C to stop")
    
    for mid in machine_ids:
        p = multiprocessing.Process(target=main_loop, args=(mid, 2))
        p.start()
        processes.append(p)
        print(f"Started monitor for MC{mid}")
    
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\nShutting down monitors...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join()
        print("Monitors stopped.")
