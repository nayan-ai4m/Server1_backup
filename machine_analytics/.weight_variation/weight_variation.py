import sys
import psycopg2
import pandas as pd
import numpy as np
from scipy.signal import savgol_filter, find_peaks
from datetime import datetime, timedelta
import time
import math
import json
import uuid
import traceback
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from sqlalchemy import create_engine

# Register a converter to return float instead of decimal.Decimal for NUMERIC/DECIMAL types
def dec_to_float(value, cursor):
    return float(value) if value is not None else None

dec2float = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    dec_to_float
)
psycopg2.extensions.register_type(dec2float)

class ConfigurableMachineAnalyzer:
    """Unified analyzer that reads machine configurations from database"""

    def __init__(self, config_file='config.json'):
        """Initialize analyzer with configuration from file and database"""
        try:
            # Load configuration from JSON
            self.load_config(config_file)

            # Setup logging
            self.setup_logging()

            self.logger.info("="*80)
            self.logger.info("Initializing Configurable Machine Analyzer")
            self.logger.info("="*80)

            # Initialize database connections
            self.init_database_connections()

            # Load machine configurations from database
            self.load_machine_configs()

            # Initialize machine analyzers
            self.machine_analyzers = {}
            for machine_id in self.machine_configs.keys():
                self.machine_analyzers[machine_id] = MachineAnalyzer(
                    machine_id=machine_id,
                    config=self.machine_configs[machine_id],
                    connections={
                        'short_data': self.short_data_conn,
                        'short_data_engine': self.short_data_engine,
                        'event': self.event_conn,
                        'tp_status': self.tp_status_conn
                    },
                    logger=self.logger
                )

            self.logger.info(f"Successfully initialized {len(self.machine_analyzers)} machine analyzers")

        except Exception as e:
            print(f"Error in __init__: {str(e)}")
            traceback.print_exc()
            raise

    def load_config(self, config_file):
        """Load database configuration from JSON file"""
        try:
            config_path = Path(config_file)
            if not config_path.exists():
                # Create default config if not exists
                default_config = {
                    "databases": {
                        "short_data": {
                            "dbname": "short_data_hul",
                            "user": "postgres",
                            "password": "ai4m2024",
                            "host": "localhost",
                            "port": "5432"
                        },
                        "event": {
                            "dbname": "hul",
                            "user": "postgres",
                            "password": "ai4m2024",
                            "host": "localhost",
                            "port": "5432"
                        },
                        "tp_status": {
                            "dbname": "hul",
                            "user": "postgres",
                            "password": "ai4m2024",
                            "host": "192.168.1.168",
                            "port": "5432"
                        }
                    },
                    "logging": {
                        "log_dir": "logs",
                        "log_level": "INFO",
                        "max_bytes": 10485760,
                        "backup_count": 5
                    },
                    "analysis": {
                        "update_interval": 0.6,
                        "restart_wait_time": 120
                    }
                }
                with open(config_file, 'w') as f:
                    json.dump(default_config, f, indent=4)
                print(f"Created default config file: {config_file}")

            with open(config_file, 'r') as f:
                self.config = json.load(f)

        except Exception as e:
            print(f"Error loading config: {str(e)}")
            raise

    def setup_logging(self):
        """Setup comprehensive logging system"""
        try:
            # Create logs directory
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_dir', 'logs'))
            log_dir.mkdir(exist_ok=True)

            # Setup main logger
            self.logger = logging.getLogger('MachineAnalyzer')
            self.logger.setLevel(getattr(logging, log_config.get('log_level', 'INFO')))

            # Clear existing handlers
            self.logger.handlers.clear()

            # File handler with rotation
            log_file = log_dir / f'analyzer_{datetime.now().strftime("%Y%m%d")}.log'
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=log_config.get('max_bytes', 10485760),  # 10MB
                backupCount=log_config.get('backup_count', 5)
            )
            file_handler.setLevel(logging.DEBUG)

            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)

            # Formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

            self.logger.info(f"Logging initialized. Log file: {log_file}")

        except Exception as e:
            print(f"Error setting up logging: {str(e)}")
            raise

    def init_database_connections(self):
        """Initialize all database connections"""
        try:
            db_config = self.config['databases']

            self.logger.info("Connecting to databases...")

            # Short data connection (PostgreSQL)
            short_data_uri = f"postgresql+psycopg2://{db_config['short_data']['user']}:{db_config['short_data']['password']}@{db_config['short_data']['host']}:{db_config['short_data']['port']}/{db_config['short_data']['dbname']}"
            self.short_data_engine = create_engine(short_data_uri)
            self.short_data_conn = psycopg2.connect(**db_config['short_data'])
            self.logger.info(f"Connected to short_data DB: {db_config['short_data']['host']}")

            # Event connection (PostgreSQL)
            self.event_conn = psycopg2.connect(**db_config['event'])
            self.logger.info(f"Connected to event DB: {db_config['event']['host']}")

            # TP Status connection (PostgreSQL)
            self.tp_status_conn = psycopg2.connect(**db_config['tp_status'])
            self.logger.info(f"Connected to tp_status DB: {db_config['tp_status']['host']}")

        except Exception as e:
            self.logger.error(f"Error connecting to databases: {str(e)}")
            raise

    def load_machine_configs(self):
        """Load machine configurations from database"""
        try:
            self.logger.info("Loading machine configurations from database...")

            cursor = self.short_data_conn.cursor()

            # Check if machine_config table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'machine_configs'
                );
            """)

            table_exists = cursor.fetchone()[0]

            if not table_exists:
                self.logger.warning("machine_configs table not found. Creating table...")
                self.create_config_table()
                self.insert_default_configs()

            # Load configurations
            cursor.execute("""
                SELECT
                    machine_id,
                    machine_name,
                    short_data_table,
                    loop3_checkpoint_field,
                    tp_status_table,
                    tp_overweight,
                    tp_underweight,
                    fill_piston_column,
                    cycle_column,
                    hopper_level_column,
                    cam_min,
                    cam_max,
                    threshold_value,
                    const_threshold_1,
                    const_threshold_2,
                    approach_buffer,
                    angle_threshold_low,
                    angle_threshold_high,
                    detection_history_size,
                    is_active
                FROM machine_configs
                WHERE is_active = true
                ORDER BY machine_id;
            """)

            rows = cursor.fetchall()
            cursor.close()

            self.machine_configs = {}
            for row in rows:
                machine_id = row[0]
                self.machine_configs[machine_id] = {
                    'machine_id': row[0],
                    'machine_name': row[1],
                    'short_data_table': row[2],
                    'loop3_checkpoint_field': row[3],
                    'tp_status_table': row[4],
                    'tp_overweight': row[5],
                    'tp_underweight': row[6],
                    'fill_piston_column': row[7],
                    'cycle_column': row[8],
                    'hopper_level_column': row[9],
                    'cam_min': row[10],
                    'cam_max': row[11],
                    'threshold_value': row[12],
                    'const_threshold_1': row[13],
                    'const_threshold_2': row[14],
                    'approach_buffer': row[15],
                    'angle_threshold_low': row[16],
                    'angle_threshold_high': row[17],
                    'detection_history_size': row[18],
                    'is_active': row[19]
                }
                self.logger.info(f"Loaded config for {row[1]} (ID: {machine_id}) | "
                                 f"Fill: {row[7]}, Cycle: {row[8]}, Hopper: {row[9]}")

            if not self.machine_configs:
                self.logger.warning("No active machine configurations found!")

        except Exception as e:
            self.logger.error(f"Error loading machine configs: {str(e)}")
            traceback.print_exc()
            raise

    def create_config_table(self):
        """Create machine_configs table if it doesn't exist"""
        try:
            cursor = self.short_data_conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS machine_configs (
                    machine_id VARCHAR(10) PRIMARY KEY,
                    machine_name VARCHAR(50) NOT NULL,
                    short_data_table VARCHAR(50) NOT NULL,
                    loop3_checkpoint_field VARCHAR(50) NOT NULL,
                    tp_status_table VARCHAR(50) NOT NULL,
                    tp_overweight VARCHAR(10) NOT NULL,
                    tp_underweight VARCHAR(10) NOT NULL,
                    fill_piston_column VARCHAR(50) DEFAULT 'fill_piston_1_current',
                    cycle_column VARCHAR(50) DEFAULT 'spare1',
                    hopper_level_column VARCHAR(50) DEFAULT 'hopper_1_level',
                    cam_min INTEGER DEFAULT 120,
                    cam_max INTEGER DEFAULT 190,
                    threshold_value DECIMAL(5,2) DEFAULT 0.3,
                    const_threshold_1 DECIMAL(5,2) DEFAULT -3.0,
                    const_threshold_2 DECIMAL(5,2) DEFAULT -4.5,
                    approach_buffer DECIMAL(5,2) DEFAULT 0.2,
                    angle_threshold_low INTEGER DEFAULT 27,
                    angle_threshold_high INTEGER DEFAULT 67,
                    detection_history_size INTEGER DEFAULT 16,
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            self.short_data_conn.commit()
            cursor.close()

            self.logger.info("Created machine_configs table")

        except Exception as e:
            self.logger.error(f"Error creating config table: {str(e)}")
            self.short_data_conn.rollback()
            raise

    def insert_default_configs(self):
        """Insert default configurations for existing machines"""
        try:
            cursor = self.short_data_conn.cursor()

            # Default configurations
            default_configs = [
                # MC17 to MC22 (original)
                ('mc17', 'Machine 17', 'mc17_short_data', 'mc17', 'mc17_tp_status', 'tp66', 'tp67',
                 'fill_piston_1_current', 'spare1', 'hopper_1_level', 120, 190, 0.3, -3.0, -4.5, 0.2, 27, 67, 16),
                ('mc18', 'Machine 18', 'mc18_short_data', 'mc18', 'mc18_tp_status', 'tp66', 'tp67',
                 'fill_piston_1_current', 'spare1', 'hopper_1_level', 120, 190, 0.3, -3.0, -4.5, 0.2, 27, 67, 16),
                ('mc19', 'Machine 19', 'mc19_short_data', 'mc19', 'mc19_tp_status', 'tp66', 'tp67',
                 'fill_piston_1_current', 'spare1', 'hopper_1_level', 120, 190, 0.3, -3.0, -4.5, 0.2, 27, 67, 16),
                ('mc20', 'Machine 20', 'mc20_short_data', 'mc20', 'mc20_tp_status', 'tp66', 'tp67',
                 'fill_piston_1_current', 'spare1', 'hopper_1_level', 120, 190, 0.3, -3.0, -4.5, 0.2, 27, 67, 16),
                ('mc21', 'Machine 21', 'mc21_short_data', 'mc21', 'mc21_tp_status', 'tp66', 'tp67',
                 'fill_piston_1_current', 'spare1', 'hopper_1_level', 120, 190, 0.3, -3.0, -4.5, 0.2, 19, 67, 16),
                ('mc22', 'Machine 22', 'mc22_short_data', 'mc22', 'mc22_tp_status', 'tp66', 'tp67',
                 'fill_piston_1_current', 'spare1', 'hopper_1_level', 120, 190, 0.3, -3.0, -4.5, 0.2, 19, 67, 16),

                # MC25 & MC26
                ('mc25', 'Machine 25', 'mc25_short_data', 'mc25', 'mc25_tp_status', 'tp66', 'tp67',
                 'piston_left_tq', 'cycle_id', 'hopper_left_level', 120, 190, 0.3, -3.0, -4.5, 0.2, 27, 67, 16),
                ('mc26', 'Machine 26', 'mc26_short_data', 'mc26', 'mc26_tp_status', 'tp66', 'tp67',
                 'piston_left_tq', 'cycle_id', 'hopper_left_level', 120, 190, 0.3, -3.0, -4.5, 0.2, 27, 67, 16),

                # MC27 & MC30
                ('mc27', 'Machine 27', 'mc27_short_data', 'mc27', 'mc27_tp_status', 'tp66', 'tp67',
                 'filling_instant_load', 'cycle_id', 'hopper_level', 120, 190, 0.3, -3.0, -4.5, 0.2, 27, 67, 16),
                ('mc30', 'Machine 30', 'mc30_short_data', 'mc30', 'mc30_tp_status', 'tp66', 'tp67',
                 'filling_instant_load', 'cycle_id', 'hopper_level', 120, 190, 0.3, -3.0, -4.5, 0.2, 27, 67, 16),
            ]

            for config in default_configs:
                cursor.execute("""
                    INSERT INTO machine_configs (
                        machine_id, machine_name, short_data_table, loop3_checkpoint_field,
                        tp_status_table, tp_overweight, tp_underweight,
                        fill_piston_column, cycle_column, hopper_level_column,
                        cam_min, cam_max, threshold_value, const_threshold_1, const_threshold_2, approach_buffer,
                        angle_threshold_low, angle_threshold_high, detection_history_size
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (machine_id) DO NOTHING;
                """, config)

            self.short_data_conn.commit()
            cursor.close()

            self.logger.info(f"Inserted {len(default_configs)} default machine configurations")

        except Exception as e:
            self.logger.error(f"Error inserting default configs: {str(e)}")
            self.short_data_conn.rollback()
            raise

    def run(self):
        """Main execution loop"""
        try:
            update_interval = self.config.get('analysis', {}).get('update_interval', 0.6)

            self.logger.info(f"Starting analysis loop (interval: {update_interval}s)")

            while True:
                for machine_id, analyzer in self.machine_analyzers.items():
                    try:
                        analyzer.update_analysis()
                    except Exception as e:
                        self.logger.error(f"Error analyzing {machine_id}: {str(e)}")
                        traceback.print_exc()

                time.sleep(update_interval)

        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal. Shutting down...")
        except Exception as e:
            self.logger.error(f"Error in main loop: {str(e)}")
            traceback.print_exc()
        finally:
            self.cleanup()

    def cleanup(self):
        """Cleanup resources"""
        try:
            self.logger.info("Cleaning up connections...")

            if hasattr(self, 'short_data_conn') and self.short_data_conn:
                self.short_data_conn.close()
                self.logger.info("Closed short_data connection")

            if hasattr(self, 'short_data_engine') and self.short_data_engine:
                self.short_data_engine.dispose()
                self.logger.info("Closed short_data engine")

            if hasattr(self, 'event_conn') and self.event_conn:
                self.event_conn.close()
                self.logger.info("Closed event connection")

            if hasattr(self, 'tp_status_conn') and self.tp_status_conn:
                self.tp_status_conn.close()
                self.logger.info("Closed tp_status connection")

            self.logger.info("Cleanup completed")

        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")


class MachineAnalyzer:
    """Individual machine analyzer"""

    def __init__(self, machine_id, config, connections, logger):
        """Initialize machine-specific analyzer"""
        self.machine_id = machine_id
        self.config = config
        self.short_data_conn = connections['short_data']
        self.short_data_engine = connections['short_data_engine']
        self.event_conn = connections['event']
        self.tp_status_conn = connections['tp_status']
        self.logger = logger

        # Machine state
        self.previous_machine_status = None
        self.waiting_for_restart = False
        self.latest_timestamp = None
        self.latest_filling_stroke = "N/A"

        # Hopper variables
        self.recent_slopes = []
        self.processed_peaks = set()

        # Weight analysis variables
        self.current_viscosity_status = "Normal"
        self.current_average_angle = None

        # Bucketing system
        self.detection_history = []
        self.previous_consensus = "NORMAL WEIGHT"

        self.logger.info(f"Initialized analyzer for {config['machine_name']}")

    def fetch_latest_data(self, limit=1000):
        """Fetch latest data from machine-specific table"""
        try:
            cursor = self.short_data_conn.cursor()
            fill_col = self.config['fill_piston_column']
            cycle_col = self.config['cycle_column']

            query = f"""
                SELECT
                    {fill_col}/10.0 as fill_piston_1_current,
                    cam_position,
                    {cycle_col} as spare1,
                    timestamp,
                    status
                FROM {self.config['short_data_table']}
                ORDER BY timestamp DESC LIMIT {limit};
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()

            columns = ['fill_piston_1_current', 'cam_position', 'spare1', 'timestamp', 'machine_status']
            df = pd.DataFrame(rows, columns=columns)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df.sort_values('timestamp')

        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error fetching data: {str(e)}")
            return pd.DataFrame()

    def fetch_latest_filling_stroke(self):
        """Fetch latest filling stroke from loop3_checkpoints"""
        try:
            cursor = self.event_conn.cursor()
            query = f"""
                WITH filtered_data AS (
                    SELECT
                        timestamp,
                        {self.config['loop3_checkpoint_field']}->>'HMI_Filling_Stroke_Deg' as filling_stroke_deg
                    FROM loop3_checkpoints
                    WHERE
                        timestamp >= NOW() - INTERVAL '7 days'
                        AND {self.config['loop3_checkpoint_field']}->>'HMI_Filling_Stroke_Deg' IS NOT NULL
                ), ranked_data AS (
                    SELECT
                        timestamp,
                        filling_stroke_deg,
                        LAG(filling_stroke_deg) OVER (ORDER BY timestamp) as prev_filling_stroke
                    FROM filtered_data
                )
                SELECT timestamp, filling_stroke_deg
                FROM ranked_data
                WHERE
                    filling_stroke_deg != prev_filling_stroke
                    OR prev_filling_stroke IS NULL
                ORDER BY timestamp DESC
                LIMIT 1;
            """
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()

            if result and len(result) >= 2 and result[1] is not None:
                return result[1]
            return "N/A"

        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error fetching filling stroke: {str(e)}")
            return "N/A"

    def calculate_baseline(self, df):
        """Calculate baseline for current analysis"""
        try:
            mask = (df['cam_position'] >= 0) & (df['cam_position'] <= 360)
            range_data = df[mask]
            baseline = range_data.groupby('cam_position')['fill_piston_1_current'].mean()
            return baseline, range_data
        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error calculating baseline: {str(e)}")
            return pd.Series(), pd.DataFrame()

    def adaptive_savgol_filter(self, data, window_length, polyorder):
        """Apply adaptive Savitzky-Golay filter"""
        try:
            n_points = len(data)
            if n_points < window_length:
                window_length = n_points if n_points % 2 != 0 else n_points - 1
            if window_length < polyorder + 1:
                polyorder = window_length - 1 if window_length > 1 else 0
            if window_length > 2 and polyorder >= 0:
                return savgol_filter(data, window_length, polyorder)
            return data
        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error in savgol filter: {str(e)}")
            return data

    def fetch_hopper_data(self):
        """Fetch hopper data for last 15 minutes"""
        try:
            cursor = self.short_data_conn.cursor()
            hopper_col = self.config['hopper_level_column']

            cursor.execute(f"SELECT MAX(timestamp) FROM {self.config['short_data_table']};")
            latest_timestamp = cursor.fetchone()[0]
            cursor.close()

            if latest_timestamp is None:
                raise ValueError("No data found")

            fifteen_minutes_before = latest_timestamp - timedelta(minutes=15)

            query = f"""
                SELECT
                    DATE_TRUNC('second', timestamp) as bucket_timestamp,
                    AVG({hopper_col}) as avg_hopper_level,
                    AVG(status) as avg_status
                FROM {self.config['short_data_table']}
                WHERE timestamp >= %s
                GROUP BY DATE_TRUNC('second', timestamp)
                ORDER BY bucket_timestamp ASC;
            """

            df = pd.read_sql_query(
                query,
                self.short_data_engine,
                params=[(fifteen_minutes_before,)],
                parse_dates=['bucket_timestamp']
            )
            return df

        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error fetching hopper data: {str(e)}")
            return pd.DataFrame()

    def detect_extrema(self, data, prominence=2, distance=10):
        """Detect peaks and valleys in data"""
        try:
            if len(data) < 3:
                return np.array([]), np.array([]), {}, {}

            peaks, peak_properties = find_peaks(data, prominence=prominence, distance=distance)
            valleys, valley_properties = find_peaks(-data, prominence=prominence, distance=distance)

            return peaks, valleys, peak_properties, valley_properties
        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error detecting extrema: {str(e)}")
            return np.array([]), np.array([]), {}, {}

    def calculate_and_track_slopes(self, peaks, valleys, hopper_data, timestamps):
        """Calculate and track slopes between peaks and valleys"""
        try:
            new_slopes_added = False

            for peak_idx in peaks:
                peak_time_key = f"{peak_idx}_{hopper_data[peak_idx]:.2f}"
                if peak_time_key in self.processed_peaks:
                    continue

                previous_valleys = valleys[valleys < peak_idx]

                if len(previous_valleys) > 0:
                    valley_idx = previous_valleys[-1]
                    level_change = hopper_data[peak_idx] - hopper_data[valley_idx]
                    time_diff = peak_idx - valley_idx

                    if time_diff > 0:
                        slope = level_change / time_diff
                        angle_degrees = math.degrees(math.atan(slope))

                        slope_data = {
                            'slope': slope,
                            'level_change': level_change,
                            'angle_degrees': angle_degrees,
                            'peak_time': timestamps[peak_idx]
                        }

                        self.recent_slopes.append(slope_data)
                        self.processed_peaks.add(peak_time_key)
                        new_slopes_added = True

                        if len(self.recent_slopes) > 6:
                            self.recent_slopes.pop(0)

            if new_slopes_added:
                self.update_slope_display()

        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error tracking slopes: {str(e)}")

    def update_slope_display(self):
        """Update average slope calculation"""
        try:
            if not self.recent_slopes:
                self.current_average_angle = None
                return

            avg_angle = sum(s['angle_degrees'] for s in self.recent_slopes) / len(self.recent_slopes)
            self.current_average_angle = avg_angle

        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error updating slope: {str(e)}")

    def handle_underweight_detection(self):
        """Handle underweight detection with logging and database updates"""
        try:
            self.logger.warning(f"[{self.machine_id}] CONSENSUS: POSSIBLE UNDERWEIGHT - Triggering alerts")

            cursor_event = self.event_conn.cursor()
            cursor_tp = self.tp_status_conn.cursor()

            event_data = (
                datetime.now().strftime("%Y%m%d_%H%M%S"),
                str(uuid.uuid4()),
                "PLC",
                self.machine_id,
                '',
                "POSSIBLE UNDERWEIGHT",
                "quality"
            )

            insert_query = """
                INSERT INTO event_table
                (timestamp, event_id, zone, camera_id, filename, event_type, alert_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor_event.execute(insert_query, event_data)
            self.event_conn.commit()

            tp_data = json.dumps({
                "timestamp": datetime.now().isoformat(),
                "uuid": str(uuid.uuid4()),
                "active": 1,
                "filepath": 'http://192.168.1.148:8015/',
                "color_code": 3
            })

            update_query = f"""
                UPDATE {self.config['tp_status_table']}
                SET {self.config['tp_underweight']} = '{tp_data}'
            """
            cursor_tp.execute(update_query)
            self.tp_status_conn.commit()

            cursor_event.close()
            cursor_tp.close()

            self.logger.info(f"[{self.machine_id}] Underweight event logged and status updated")

        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error handling underweight: {str(e)}")
            self.event_conn.rollback()
            self.tp_status_conn.rollback()

    def handle_overweight_detection(self):
        """Handle overweight detection with logging and database updates"""
        try:
            self.logger.warning(f"[{self.machine_id}] CONSENSUS: POSSIBLE OVERWEIGHT - Triggering alerts")

            cursor_event = self.event_conn.cursor()
            cursor_tp = self.tp_status_conn.cursor()

            event_data = (
                datetime.now().strftime("%Y%m%d_%H%M%S"),
                str(uuid.uuid4()),
                "PLC",
                self.machine_id,
                '',
                "POSSIBLE OVERWEIGHT",
                "quality"
            )

            insert_query = """
                INSERT INTO event_table
                (timestamp, event_id, zone, camera_id, filename, event_type, alert_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor_event.execute(insert_query, event_data)
            self.event_conn.commit()

            tp_data = json.dumps({
                "timestamp": datetime.now().isoformat(),
                "uuid": str(uuid.uuid4()),
                "active": 1,
                "filepath": 'http://192.168.1.168:8015/',
                "color_code": 3
            })

            update_query = f"""
                UPDATE {self.config['tp_status_table']}
                SET {self.config['tp_overweight']} = '{tp_data}'
            """
            cursor_tp.execute(update_query)
            self.tp_status_conn.commit()

            cursor_event.close()
            cursor_tp.close()

            self.logger.info(f"[{self.machine_id}] Overweight event logged and status updated")

        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error handling overweight: {str(e)}")
            self.event_conn.rollback()
            self.tp_status_conn.rollback()

    def analyze_weight_status(self):
        """Analyze weight status with consensus-based detection"""
        try:
            if self.current_average_angle is None:
                return

            avg_angle = self.current_average_angle
            viscosity = self.current_viscosity_status

            angle_low = self.config['angle_threshold_low']
            angle_high = self.config['angle_threshold_high']

            if avg_angle < angle_low and viscosity == "High":
                individual_detection = "POSSIBLE OVERWEIGHT"
            elif angle_low <= avg_angle <= angle_high and viscosity == "High":
                individual_detection = "POSSIBLE OVERWEIGHT"
            elif avg_angle > angle_high and viscosity == "High":
                individual_detection = "POSSIBLE OVERWEIGHT"
            elif avg_angle < angle_low and viscosity == "Low":
                individual_detection = "POSSIBLE UNDERWEIGHT"
            elif angle_low <= avg_angle <= angle_high and viscosity == "Low":
                individual_detection = "POSSIBLE UNDERWEIGHT"
            elif avg_angle > angle_high and viscosity == "Low":
                individual_detection = "POSSIBLE UNDERWEIGHT"
            elif avg_angle < angle_low and viscosity == "Normal":
                individual_detection = "POSSIBLE OVERWEIGHT"
            elif avg_angle > angle_high and viscosity == "Normal":
                individual_detection = "POSSIBLE UNDERWEIGHT"
            else:
                individual_detection = "NORMAL WEIGHT"

            self.detection_history.append(individual_detection)
            history_size = self.config['detection_history_size']
            if len(self.detection_history) > history_size:
                self.detection_history.pop(0)

            if len(self.detection_history) < history_size:
                consensus_result = "NORMAL WEIGHT"
            else:
                if all(d == "POSSIBLE UNDERWEIGHT" for d in self.detection_history):
                    consensus_result = "POSSIBLE UNDERWEIGHT"
                    self.logger.info(f"[{self.machine_id}] Consensus: UNDERWEIGHT (All {history_size} agree)")
                elif all(d == "POSSIBLE OVERWEIGHT" for d in self.detection_history):
                    consensus_result = "POSSIBLE OVERWEIGHT"
                    self.logger.info(f"[{self.machine_id}] Consensus: OVERWEIGHT (All {history_size} agree)")
                else:
                    consensus_result = "NORMAL WEIGHT"

            if consensus_result != self.previous_consensus:
                self.logger.info(f"[{self.machine_id}] Consensus changed: {self.previous_consensus} → {consensus_result}")
                if consensus_result == "POSSIBLE UNDERWEIGHT":
                    self.handle_underweight_detection()
                elif consensus_result == "POSSIBLE OVERWEIGHT":
                    self.handle_overweight_detection()
                self.previous_consensus = consensus_result

        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error analyzing weight status: {str(e)}")

    def plot_fill_piston_data(self, df, baseline_df):
        """Analyze fill piston data and detect viscosity changes"""
        try:
            last_10_cycles = df['spare1'].unique()[-10:]
            filtered_df = df[df['spare1'].isin(last_10_cycles)]

            baseline, baseline_points = self.calculate_baseline(baseline_df)

            best_values = filtered_df.groupby('cam_position')['fill_piston_1_current'].mean().reset_index()
            best_values = best_values.sort_values('cam_position')

            window_length = min(2, len(best_values) - 1)
            if window_length % 2 == 0:
                window_length -= 1
            polyorder = 3

            if len(best_values) > 3:
                smoothed_current = self.adaptive_savgol_filter(
                    best_values['fill_piston_1_current'],
                    window_length,
                    polyorder
                )

                smoothed_df = pd.DataFrame({
                    'cam_position': best_values['cam_position'],
                    'smoothed_current': smoothed_current
                })

                cam_min = self.config['cam_min']
                cam_max = self.config['cam_max']
                threshold = self.config['threshold_value']

                smoothed_in_range = smoothed_df[
                    (smoothed_df['cam_position'] >= cam_min) &
                    (smoothed_df['cam_position'] <= cam_max)
                ]

                if len(smoothed_in_range) > 0:
                    min_idx = smoothed_in_range['smoothed_current'].idxmin()
                    if not pd.isna(min_idx):
                        min_row = smoothed_in_range.loc[min_idx]
                        min_cam = min_row['cam_position']
                        min_val = min_row['smoothed_current']

                        if min_cam in baseline.index:
                            baseline_val = baseline[min_cam]
                            if min_val < baseline_val - threshold:
                                self.current_viscosity_status = "High"
                            elif min_val > baseline_val + threshold:
                                self.current_viscosity_status = "Low"
                            else:
                                self.current_viscosity_status = "Normal"

        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error in fill piston analysis: {str(e)}")

    def analyze_hopper_data(self, df):
        """Analyze hopper level data"""
        try:
            if df.empty:
                return

            df = df.sort_values('bucket_timestamp').reset_index(drop=True)

            hopper_levels = df['avg_hopper_level'].values
            timestamps = df['bucket_timestamp'].values

            peaks, valleys, peak_props, valley_props = self.detect_extrema(hopper_levels)

            self.calculate_and_track_slopes(peaks, valleys, hopper_levels, timestamps)

        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error analyzing hopper data: {str(e)}")

    def update_analysis(self):
        """Main update cycle for machine analysis"""
        try:
            display_data = self.fetch_latest_data(limit=2000)

            if display_data.empty:
                self.logger.warning(f"[{self.machine_id}] No data available")
                return

            #self.latest_filling_stroke = self.fetch_latest_filling_stroke()

            current_machine_status = display_data['machine_status'].iloc[-1]

            if self.previous_machine_status is None:
                self.previous_machine_status = current_machine_status

            if current_machine_status != self.previous_machine_status:
                if current_machine_status == 1:
                    self.logger.info(f"[{self.machine_id}] Machine restarted, waiting 120s...")
                    self.waiting_for_restart = True
                    time.sleep(120)
                    self.waiting_for_restart = False
                self.previous_machine_status = current_machine_status

            if current_machine_status == 1 and not self.waiting_for_restart:
                baseline_data = self.fetch_latest_data(limit=8000)
                last_cycles = baseline_data['spare1'].unique()[-500:]
                baseline_df = baseline_data[baseline_data['spare1'].isin(last_cycles)]

                self.plot_fill_piston_data(display_data, baseline_df)

                hopper_df = self.fetch_hopper_data()
                self.analyze_hopper_data(hopper_df)

                self.analyze_weight_status()

        except Exception as e:
            self.logger.error(f"[{self.machine_id}] Error in update_analysis: {str(e)}")
            traceback.print_exc()


def main():
    """Main entry point"""
    try:
        print("="*80)
        print("Unified Machine Analyzer System")
        print("="*80)

        analyzer = ConfigurableMachineAnalyzer(config_file='config.json')
        analyzer.run()

    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
