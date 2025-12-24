import sys
import psycopg2
import pandas as pd
import numpy as np
from collections import deque
from scipy import signal
from scipy.stats import pearsonr
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import time
import json
import logging
from logging.handlers import RotatingFileHandler
import threading
import argparse
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ===============================================================================
# CONFIGURATION AND LOGGING SETUP
# ===============================================================================
def load_db_config():
    """Load database configuration from config.json."""
    try:
        config_path = Path(__file__).parent / 'config.json'
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config.get('database', {})
    except FileNotFoundError:
        logging.error("config.json not found. Creating default configuration...")
        default_config = {
            "database": {
                "dbname": "short_data_hul",
                "user": "postgres",
                "password": "ai4m2024",
                "host": "192.168.1.149",
                "port": "5432"
            }
        }
        config_path = Path(__file__).parent / 'config.json'
        with open(config_path, 'w') as f:
            json.dump(default_config, f, indent=4)
        return default_config['database']
    except Exception as e:
        logging.error(f"Error loading config.json: {e}")
        return None

def setup_logger(machine_id=None):
    """Setup logging configuration with rotation."""
    log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    if machine_id:
        log_file = log_dir / f'sync_{machine_id.lower()}.log'
        logger_name = f'sync.{machine_id}'
    else:
        log_file = log_dir / 'sync_unified.log'
        logger_name = 'sync.main'
    
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

DB_CONFIG = load_db_config()

# ===============================================================================
# DATABASE SETUP AND CONFIGURATION
# ===============================================================================
def setup_configuration_table():
    """Create machine synchronization configuration table."""
    logger = logging.getLogger('sync.main')
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS machine_sync_config (
            machine_id VARCHAR(10) PRIMARY KEY,
            source_table VARCHAR(50) NOT NULL,
            sync_table VARCHAR(50) NOT NULL,
            cycle_column VARCHAR(50) NOT NULL,
            status_column VARCHAR(50) NOT NULL,
            timestamp_column VARCHAR(50) DEFAULT 'timestamp',
            
            -- Column mappings for different machine types
            hor_sealer_col VARCHAR(50),
            ver_sealer_col VARCHAR(50),
            rot_valve_col VARCHAR(50),
            fill_piston_col VARCHAR(50),
            web_puller_col VARCHAR(50),
            cam_position_col VARCHAR(50),
            web_puller_current_col VARCHAR(50),
            
            -- For MC25/MC26 machines
            horizontal_sealer_col VARCHAR(50),
            vertical_sealer_col VARCHAR(50),
            piston_right_col VARCHAR(50),
            piston_left_col VARCHAR(50),
            rotary_valve_col VARCHAR(50),
            puller_tq_col VARCHAR(50),
            
            -- Scale factors
            current_scale_factor FLOAT DEFAULT 10.0,
            torque_invert BOOLEAN DEFAULT FALSE,
            
            -- Analysis parameters
            noise_threshold_percentile FLOAT DEFAULT 10.0,
            sliding_window_size INTEGER DEFAULT 10,
            max_lag_samples INTEGER DEFAULT 20,
            avg_buffer_size INTEGER DEFAULT 100,
            cycle_delay INTEGER DEFAULT 3,
            update_interval FLOAT DEFAULT 0.1,
            
            -- Thresholds for alerts
            vs_hs_threshold FLOAT DEFAULT 70.0,
            vs_hs_alert_above BOOLEAN DEFAULT FALSE,
            fp_rv_threshold FLOAT DEFAULT 40.0,
            fp_rv_alert_above BOOLEAN DEFAULT TRUE,
            ps_hs_threshold FLOAT DEFAULT 40.0,
            ps_hs_alert_above BOOLEAN DEFAULT TRUE,
            rv_vs_threshold FLOAT DEFAULT 70.0,
            rv_vs_alert_above BOOLEAN DEFAULT FALSE,
            
            -- Plot settings
            enable_plots BOOLEAN DEFAULT FALSE,
            plot_output_dir VARCHAR(100) DEFAULT 'sync_analysis_plots',
            
            -- Machine type (standard or advanced)
            machine_type VARCHAR(20) DEFAULT 'standard',
            
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        conn.commit()
        logger.info("Sync configuration table created/verified successfully!")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error setting up configuration table: {e}", exc_info=True)
        return False

def insert_default_sync_configurations():
    """Insert default configurations for all machines."""
    logger = logging.getLogger('sync.main')
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Standard machines (MC17-MC22)
        standard_configs = [
            ('MC17', 'mc17_short_data', 'sync_data', 'spare1', 'status'),
            ('MC18', 'mc18_short_data', 'sync_data_18', 'spare1', 'status'),
            ('MC19', 'mc19_short_data', 'sync_data_19', 'spare1', 'status'),
            ('MC20', 'mc20_short_data', 'sync_data_20', 'spare1', 'status'),
            ('MC21', 'mc21_short_data', 'sync_data_21', 'spare1', 'status'),
            ('MC22', 'mc22_short_data', 'sync_data_22', 'spare1', 'status'),
        ]
        
        for machine_id, source_table, sync_table, cycle_col, status_col in standard_configs:
            cursor.execute("""
            INSERT INTO machine_sync_config 
            (machine_id, source_table, sync_table, cycle_column, status_column,
             hor_sealer_col, ver_sealer_col, rot_valve_col, fill_piston_col,
             web_puller_col, cam_position_col, web_puller_current_col,
             current_scale_factor, machine_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (machine_id) DO NOTHING;
            """, (
                machine_id, source_table, sync_table, cycle_col, status_col,
                'hor_sealer_position', 'ver_sealer_position', 'rot_valve_1_position',
                'fill_piston_1_position', 'web_puller_position', 'cam_position',
                'web_puller_current', 10.0, 'standard'
            ))
        
        # Advanced machines (MC25, MC26)
        advanced_configs = [
            ('MC25', 'mc25_fast', 'sync_data_25', 'cycle_id', 'status'),
            ('MC26', 'mc26_fast', 'sync_data_26', 'cycle_id', 'status'),
        ]
        
        for machine_id, source_table, sync_table, cycle_col, status_col in advanced_configs:
            cursor.execute("""
            INSERT INTO machine_sync_config 
            (machine_id, source_table, sync_table, cycle_column, status_column,
             horizontal_sealer_col, vertical_sealer_col, piston_right_col,
             rotary_valve_col, puller_tq_col, torque_invert, machine_type,
             enable_plots)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (machine_id) DO NOTHING;
            """, (
                machine_id, source_table, sync_table, cycle_col, status_col,
                'horizontal_sealer_position', 'vertical_sealer_position',
                'piston_position_right', 'rotary_valve_position', 'puller_tq',
                True, 'advanced', True
            ))
        
        conn.commit()
        logger.info(f"Default sync configurations processed for {len(standard_configs) + len(advanced_configs)} machines")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error inserting default configurations: {e}", exc_info=True)
        return False

def load_machine_config(machine_id):
    """Load configuration for a specific machine."""
    logger = logging.getLogger(f'sync.{machine_id}')
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT * FROM machine_sync_config 
        WHERE machine_id = %s AND active = TRUE;
        """, (machine_id,))
        
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if row:
            return dict(zip(columns, row))
        else:
            logger.warning(f"No active configuration found for machine {machine_id}")
            return None
    except Exception as e:
        logger.error(f"Error loading machine configuration: {e}", exc_info=True)
        return None

def create_sync_table(table_name, logger):
    """Create sync data table for a machine."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            spare1 INTEGER,
            cycle_id INTEGER,
            vertical_horizontal_sync FLOAT,
            vertical_horizontal_sync_avg FLOAT,
            vertical_rotational_sync FLOAT,
            vertical_rotational_sync_avg FLOAT,
            filling_rotational_sync FLOAT,
            filling_rotational_sync_avg FLOAT,
            pulling_horizontal_sync FLOAT,
            pulling_horizontal_sync_avg FLOAT,
            timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Sync table {table_name} created/verified successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating sync table {table_name}: {e}", exc_info=True)
        return False

# ===============================================================================
# DATA FETCHING AND PROCESSING
# ===============================================================================
def get_latest_cycle(config, logger):
    """Fetch the latest cycle from the source table."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        query = f"SELECT {config['cycle_column']} FROM {config['source_table']} ORDER BY timestamp DESC LIMIT 1;"
        cursor.execute(query)
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error fetching latest cycle: {e}", exc_info=True)
        return None

def fetch_cycle_data(config, cycle_id, logger):
    """Fetch data for a specific cycle."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        if config['machine_type'] == 'standard':
            # Standard machine columns
            query = f"""
            SELECT {config['hor_sealer_col']},
                   {config['ver_sealer_col']},
                   {config['rot_valve_col']},
                   {config['fill_piston_col']},
                   {config['web_puller_col']},
                   {config['cam_position_col']},
                   {config['web_puller_current_col']}/{config['current_scale_factor']} as web_puller_current,
                   {config['timestamp_column']},
                   {config['cycle_column']},
                   {config['status_column']}
            FROM {config['source_table']}
            WHERE {config['cycle_column']} = %s
            ORDER BY {config['timestamp_column']} DESC;
            """
            columns = ['hor_sealer_position', 'ver_sealer_position', 'rot_valve_1_position',
                      'fill_piston_1_position', 'web_puller_position', 'cam_position',
                      'web_puller_current', 'timestamp', 'cycle_id', 'status']
        else:
            # Advanced machine columns (MC25/MC26)
            torque_mult = '-1.0' if config['torque_invert'] else '1.0'
            query = f"""
            SELECT {config['horizontal_sealer_col']},
                   {config['vertical_sealer_col']},
                   {config['piston_right_col']},
                   {config['rotary_valve_col']},
                   {config['puller_tq_col']}*{torque_mult} as puller_tq,
                   {config['timestamp_column']},
                   {config['cycle_column']},
                   {config['status_column']}
            FROM {config['source_table']}
            WHERE {config['cycle_column']} = %s
            ORDER BY {config['timestamp_column']} DESC;
            """
            columns = ['horizontal_sealer_position', 'vertical_sealer_position',
                      'piston_position_right', 'rotary_valve_position', 'puller_tq',
                      'timestamp', 'cycle_id', 'status']
        
        cursor.execute(query, (cycle_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        logger.error(f"Error fetching cycle data: {e}", exc_info=True)
        return pd.DataFrame()

def insert_sync_data(config, cycle_id, vs_hs_sync, vs_hs_avg, vs_rv_sync, vs_rv_avg,
                    fp_rv_sync, fp_rv_avg, ps_hs_sync, ps_hs_avg, timestamp, logger):
    """Insert synchronization data into the database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Use appropriate column name based on source table
        cycle_col_name = 'spare1' if 'spare1' in config['cycle_column'] else 'cycle_id'
        
        insert_query = f"""
        INSERT INTO {config['sync_table']} 
        ({cycle_col_name}, vertical_horizontal_sync, vertical_horizontal_sync_avg,
         vertical_rotational_sync, vertical_rotational_sync_avg,
         filling_rotational_sync, filling_rotational_sync_avg,
         pulling_horizontal_sync, pulling_horizontal_sync_avg, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        cursor.execute(insert_query, (
            cycle_id, vs_hs_sync, vs_hs_avg, vs_rv_sync, vs_rv_avg,
            fp_rv_sync, fp_rv_avg, ps_hs_sync, ps_hs_avg, timestamp
        ))
        
        conn.commit()
        logger.info(f"Sync data inserted for cycle {cycle_id}: VS-HS={vs_hs_avg:.1f}%, "
                   f"VS-RV={vs_rv_avg:.1f}%, FP-RV={fp_rv_avg:.1f}%, PS-HS={ps_hs_avg:.1f}%")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error inserting sync data: {e}", exc_info=True)
        if 'conn' in locals():
            conn.rollback()
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# ===============================================================================
# SYNCHRONIZATION ANALYSIS FUNCTIONS
# ===============================================================================
def check_in_phase(series1, series2):
    """Calculate basic in-phase percentage."""
    diff1 = np.diff(series1)
    diff2 = np.diff(series2)
    non_zero_mask = (np.sign(diff1) != 0) & (np.sign(diff2) != 0)
    same_direction = np.sign(diff1[non_zero_mask]) == np.sign(diff2[non_zero_mask])
    return np.sum(same_direction) / len(same_direction) if len(same_direction) > 0 else 0

def apply_threshold_filter(series, percentile=10.0):
    """Apply threshold filtering to reduce noise impact."""
    series = np.array(series)
    diff = np.diff(series)
    threshold = np.percentile(np.abs(diff), percentile)
    diff[np.abs(diff) < threshold] = 0
    
    filtered = np.zeros_like(series)
    filtered[0] = series[0]
    for i in range(1, len(series)):
        filtered[i] = filtered[i-1] + diff[i-1]
    
    return filtered

def calculate_cross_correlation(series1, series2, max_lag=20):
    """Calculate cross-correlation to find optimal time lag."""
    s1 = (series1 - np.mean(series1)) / (np.std(series1) + 1e-10)
    s2 = (series2 - np.mean(series2)) / (np.std(series2) + 1e-10)
    
    correlation = signal.correlate(s2, s1, mode='full')
    lags = signal.correlation_lags(len(s2), len(s1), mode='full')
    
    lag_mask = (lags >= -max_lag) & (lags <= max_lag)
    valid_corr = correlation[lag_mask]
    valid_lags = lags[lag_mask]
    
    if len(valid_corr) > 0:
        max_corr_idx = np.argmax(np.abs(valid_corr))
        max_corr = valid_corr[max_corr_idx]
        optimal_lag = valid_lags[max_corr_idx]
        max_corr_normalized = max_corr / len(series1)
        
        # Calculate correlation with lag compensation
        if optimal_lag > 0:
            if optimal_lag < len(series1):
                corr_value, _ = pearsonr(series1[:-optimal_lag], series2[optimal_lag:])
            else:
                corr_value = 0
        elif optimal_lag < 0:
            lag = abs(optimal_lag)
            if lag < len(series1):
                corr_value, _ = pearsonr(series1[lag:], series2[:-lag])
            else:
                corr_value = 0
        else:
            corr_value, _ = pearsonr(series1, series2)
        
        return max_corr_normalized, optimal_lag, abs(corr_value) * 100
    
    return 0, 0, 0

def sliding_window_correlation(series1, series2, window_size=10):
    """Calculate correlation in sliding windows."""
    if len(series1) < window_size or len(series2) < window_size:
        return 0, []
    
    correlations = []
    for i in range(len(series1) - window_size + 1):
        window1 = series1[i:i + window_size]
        window2 = series2[i:i + window_size]
        
        if np.std(window1) > 1e-10 and np.std(window2) > 1e-10:
            corr, _ = pearsonr(window1, window2)
            correlations.append(abs(corr))
        else:
            correlations.append(0)
    
    avg_correlation = np.mean(correlations) * 100 if correlations else 0
    return avg_correlation, correlations

def comprehensive_sync_analysis(config, series1, series2):
    """Perform comprehensive synchronization analysis."""
    filtered1 = apply_threshold_filter(series1, config['noise_threshold_percentile'])
    filtered2 = apply_threshold_filter(series2, config['noise_threshold_percentile'])
    
    sliding_corr, window_corrs = sliding_window_correlation(
        filtered1, filtered2, config['sliding_window_size']
    )
    
    max_corr, optimal_lag, lag_compensated_corr = calculate_cross_correlation(
        filtered1, filtered2, config['max_lag_samples']
    )
    
    diff1 = np.diff(filtered1)
    diff2 = np.diff(filtered2)
    mask = (np.abs(diff1) > 0) & (np.abs(diff2) > 0)
    if np.sum(mask) > 0:
        same = np.sign(diff1[mask]) == np.sign(diff2[mask])
        in_phase_pct = np.sum(same) / len(same) * 100
    else:
        in_phase_pct = 0
    
    sync_score = (
        sliding_corr * 0.4 +
        lag_compensated_corr * 0.4 +
        in_phase_pct * 0.2
    )
    
    return {
        'sync_score': sync_score,
        'sliding_correlation': sliding_corr,
        'lag_compensated_correlation': lag_compensated_corr,
        'optimal_lag': optimal_lag,
        'in_phase_percentage': in_phase_pct,
        'window_correlations': window_corrs,
        'max_cross_correlation': max_corr
    }

# ===============================================================================
# PROCESSING FUNCTIONS
# ===============================================================================
def process_standard_machine(config, df, cycle_id, avg_buffers, logger):
    """Process synchronization for standard machines (MC17-MC22)."""
    status = df['status'].iloc[0]
    if not bool(status):
        logger.info(f"Machine stopped for cycle {cycle_id}")
        return
    
    vs_hs_sync = check_in_phase(df['ver_sealer_position'], df['hor_sealer_position'])
    fp_rv_sync = check_in_phase(df['fill_piston_1_position'], df['rot_valve_1_position'])
    ps_hs_sync = check_in_phase(df['web_puller_current'], df['hor_sealer_position'])
    rv_vs_sync = check_in_phase(df['rot_valve_1_position'], df['ver_sealer_position'])
    
    avg_buffers['vs_hs'].append(vs_hs_sync * 100)
    avg_buffers['fp_rv'].append(fp_rv_sync * 100)
    avg_buffers['ps_hs'].append(ps_hs_sync * 100)
    avg_buffers['rv_vs'].append(rv_vs_sync * 100)
    
    avg_vs_hs = np.mean(avg_buffers['vs_hs'])
    avg_fp_rv = np.mean(avg_buffers['fp_rv'])
    avg_ps_hs = np.mean(avg_buffers['ps_hs'])
    avg_rv_vs = np.mean(avg_buffers['rv_vs'])
    
    timestamp = df['timestamp'].iloc[-1]
    
    insert_sync_data(
        config, cycle_id, vs_hs_sync * 100, avg_vs_hs, rv_vs_sync * 100, avg_rv_vs,
        fp_rv_sync * 100, avg_fp_rv, ps_hs_sync * 100, avg_ps_hs, timestamp, logger
    )

def process_advanced_machine(config, df, cycle_id, avg_buffers, sync_metrics, logger):
    """Process synchronization for advanced machines (MC25/MC26)."""
    status = df['status'].iloc[0]
    if not bool(status):
        logger.info(f"Machine stopped for cycle {cycle_id}")
        return
    
    # Comprehensive analysis
    vs_hs_metrics = comprehensive_sync_analysis(
        config,
        df['vertical_sealer_position'].values,
        df['horizontal_sealer_position'].values
    )
    
    fp_rv_metrics = comprehensive_sync_analysis(
        config,
        df['piston_position_right'].values,
        df['rotary_valve_position'].values
    )
    
    ps_hs_metrics = comprehensive_sync_analysis(
        config,
        df['puller_tq'].values,
        df['horizontal_sealer_position'].values
    )
    
    rv_vs_metrics = comprehensive_sync_analysis(
        config,
        df['rotary_valve_position'].values,
        df['vertical_sealer_position'].values
    )
    
    # Store metrics
    sync_metrics['vs_hs'] = vs_hs_metrics
    sync_metrics['fp_rv'] = fp_rv_metrics
    sync_metrics['ps_hs'] = ps_hs_metrics
    sync_metrics['rv_vs'] = rv_vs_metrics
    
    # Update buffers
    avg_buffers['vs_hs'].append(vs_hs_metrics['sync_score'])
    avg_buffers['fp_rv'].append(fp_rv_metrics['sync_score'])
    avg_buffers['ps_hs'].append(ps_hs_metrics['sync_score'])
    avg_buffers['rv_vs'].append(rv_vs_metrics['sync_score'])
    
    avg_vs_hs = np.mean(avg_buffers['vs_hs'])
    avg_fp_rv = np.mean(avg_buffers['fp_rv'])
    avg_ps_hs = np.mean(avg_buffers['ps_hs'])
    avg_rv_vs = np.mean(avg_buffers['rv_vs'])
    
    timestamp = df['timestamp'].iloc[-1]
    
    insert_sync_data(
        config, cycle_id, vs_hs_metrics['sync_score'], avg_vs_hs,
        rv_vs_metrics['sync_score'], avg_rv_vs,
        fp_rv_metrics['sync_score'], avg_fp_rv,
        ps_hs_metrics['sync_score'], avg_ps_hs, timestamp, logger
    )

# ===============================================================================
# MAIN MONITORING LOOP
# ===============================================================================
def monitor_machine(machine_id):
    """Main monitoring function for a specific machine."""
    logger = setup_logger(machine_id)
    
    try:
        logger.info(f"Starting synchronization analysis for {machine_id}...")
        config = load_machine_config(machine_id)
        
        if not config:
            logger.error(f"Failed to load configuration for {machine_id}")
            return
        
        # Create sync table
        create_sync_table(config['sync_table'], logger)
        
        logger.info(f"{machine_id} Synchronization System Started")
        logger.info(f"Machine Type: {config['machine_type']}")
        logger.info(f"Source Table: {config['source_table']}")
        logger.info(f"Sync Table: {config['sync_table']}")
        
        cycle_buffer = deque(maxlen=config['cycle_delay'])
        avg_buffers = {
            'vs_hs': deque(maxlen=config['avg_buffer_size']),
            'fp_rv': deque(maxlen=config['avg_buffer_size']),
            'ps_hs': deque(maxlen=config['avg_buffer_size']),
            'rv_vs': deque(maxlen=config['avg_buffer_size'])
        }
        sync_metrics = {'vs_hs': {}, 'fp_rv': {}, 'ps_hs': {}, 'rv_vs': {}}
        
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while True:
            try:
                latest_cycle = get_latest_cycle(config, logger)
                if latest_cycle is None:
                    logger.warning("No cycle data found")
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error("Too many consecutive errors, stopping")
                        break
                    time.sleep(config['update_interval'])
                    continue
                
                consecutive_errors = 0
                
                if not cycle_buffer or cycle_buffer[-1] != latest_cycle:
                    cycle_buffer.append(latest_cycle)
                
                if len(cycle_buffer) == config['cycle_delay']:
                    cycle_to_process = cycle_buffer.popleft()
                    logger.debug(f"Processing cycle (delayed): {cycle_to_process}")
                    
                    df = fetch_cycle_data(config, cycle_to_process, logger)
                    if not df.empty:
                        if config['machine_type'] == 'standard':
                            process_standard_machine(config, df, cycle_to_process, avg_buffers, logger)
                        else:
                            process_advanced_machine(config, df, cycle_to_process, avg_buffers, sync_metrics, logger)
                
                time.sleep(config['update_interval'])
                
            except KeyboardInterrupt:
                logger.info(f"[{machine_id}] Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}", exc_info=True)
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive errors ({consecutive_errors}), stopping")
                    break
                time.sleep(config['update_interval'])
                continue
                
    except Exception as e:
        logger.critical(f"Critical error in monitor_machine for {machine_id}: {e}", exc_info=True)
    finally:
        logger.info(f"Synchronization monitoring stopped for {machine_id}")

def monitor_all_machines():
    """Monitor all active machines in separate threads."""
    main_logger = setup_logger()
    main_logger.info("Starting synchronization monitoring for all active machines...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT machine_id FROM machine_sync_config WHERE active = TRUE ORDER BY machine_id;")
        machines = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        if not machines:
            main_logger.warning("No active machines found in configuration")
            return
        
        main_logger.info(f"Found {len(machines)} active machines: {', '.join(machines)}")
        
        threads = []
        for machine_id in machines:
            thread = threading.Thread(
                target=monitor_machine,
                args=(machine_id,),
                name=f"Sync-{machine_id}",
                daemon=False
            )
            thread.start()
            threads.append(thread)
            main_logger.info(f"Started sync monitoring thread for {machine_id}")
            time.sleep(1)
        
        # Wait for all threads
        for thread in threads:
            thread.join()
            
    except KeyboardInterrupt:
        main_logger.info("Monitoring stopped by user (Ctrl+C)")
    except Exception as e:
        main_logger.error(f"Error in monitor_all_machines: {e}", exc_info=True)
    finally:
        main_logger.info("All synchronization monitoring threads stopped")

# ===============================================================================
# COMMAND LINE INTERFACE
# ===============================================================================
def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Unified Synchronization Analysis System')
    parser.add_argument('--setup', action='store_true', help='Setup configuration table and insert defaults')
    parser.add_argument('--machine', type=str, help='Machine ID to monitor (e.g., MC17, MC18)')
    parser.add_argument('--all', action='store_true', help='Monitor all active machines')
    parser.add_argument('--list', action='store_true', help='List all configured machines')
    
    args = parser.parse_args()
    
    main_logger = setup_logger()
    
    if not DB_CONFIG:
        main_logger.error("Failed to load database configuration. Exiting.")
        return
    
    if args.setup:
        main_logger.info("Setting up synchronization configuration table...")
        if setup_configuration_table():
            main_logger.info("Inserting default configurations...")
            insert_default_sync_configurations()
        return
    
    if args.list:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT machine_id, source_table, sync_table, machine_type, active 
                FROM machine_sync_config 
                ORDER BY machine_id;
            """)
            machines = cursor.fetchall()
            print("\nConfigured Machines:")
            print("-" * 70)
            print(f"{'Machine':<10} {'Source Table':<25} {'Type':<12} {'Status':<10}")
            print("-" * 70)
            for machine_id, source_table, sync_table, machine_type, active in machines:
                status = "ACTIVE" if active else "INACTIVE"
                print(f"{machine_id:<10} {source_table:<25} {machine_type:<12} {status:<10}")
            print("-" * 70)
            cursor.close()
            conn.close()
        except Exception as e:
            main_logger.error(f"Error listing machines: {e}", exc_info=True)
        return
    
    if args.all:
        monitor_all_machines()
    elif args.machine:
        monitor_machine(args.machine.upper())
    else:
        parser.print_help()
        print("\nExample usage:")
        print("  python sync_unified.py --setup              # Setup database tables")
        print("  python sync_unified.py --list               # List all machines")
        print("  python sync_unified.py --machine MC17       # Monitor MC17")
        print("  python sync_unified.py --all                # Monitor all active machines")

if __name__ == "__main__":
    main()
