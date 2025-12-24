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

class CombinedAnalyzer:
    def __init__(self):
        try:
            # Database connection parameters for short_data_hul (localhost)
            self.conn_params = {
                'dbname': 'short_data_hul',
                'user': 'postgres',
                'password': 'ai4m2024',
                'host': 'localhost',
                'port': '5432'
            }

            # Database connection parameters for hul (100.96.244.68) - used for event_table and loop3_checkpoints
            self.event_conn_params = {
                'dbname': 'hul',
                'user': 'postgres',
                'password': 'ai4m2024',
                'host': 'localhost',
                'port': '5432'
            }

            # Database connection parameters for mc19_tp_status (192.168.1.168)
            self.tp_status_conn_params = {
                'dbname': 'hul',
                'user': 'postgres',
                'password': 'ai4m2024',
                'host': '192.168.1.168',
                'port': '5432'
            }

            # Initialize database connections
            self.conn = psycopg2.connect(**self.conn_params)
            self.event_conn = psycopg2.connect(**self.event_conn_params)
            self.tp_status_conn = psycopg2.connect(**self.tp_status_conn_params)

            # Timer for real-time updates - 0.5 seconds
            # Initialize Fill Piston variables
            self.previous_machine_status = None
            self.waiting_for_restart = False
            self.latest_timestamp = None
            self.latest_filling_stroke = "N/A"
            self.cam_min = 120
            self.cam_max = 190
            self.threshold_value = 0.3
            self.const_threshold_1 = -3.0
            self.const_threshold_2 = -4.5
            self.approach_buffer = 0.2
            self.current_column = 'fill_piston_1_current'

            # Initialize Hopper variables
            self.recent_slopes = []
            self.processed_peaks = set()

            # Weight analysis variables
            self.current_viscosity_status = "Normal"
            self.current_average_angle = None

            # Bucketing system for consensus detection
            self.detection_history = []  # Rolling window of last 5 detections
            self.previous_consensus = "NORMAL WEIGHT"  # Track previous consensus to avoid duplicate events

        except Exception as e:
            print(f"Error in __init__ at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()
            raise

    def __del__(self):
        """Destructor to close database connections"""
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
            if hasattr(self, 'event_conn') and self.event_conn:
                self.event_conn.close()
            if hasattr(self, 'tp_status_conn') and self.tp_status_conn:
                self.tp_status_conn.close()
        except Exception as e:
            print(f"Error in __del__ at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()

    def fetch_latest_data(self, limit=1000):
        try:
            cursor = self.conn.cursor()
            query = f"""
             SELECT fill_piston_1_current/10.0 as fill_piston_1_current,
             cam_position,
             spare1,
             timestamp,
             status
             FROM mc19_short_data
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
            print(f"Error in fetch_latest_data at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()

    def fetch_latest_filling_stroke(self):
        try:
            cursor = self.event_conn.cursor()
            query = """
            WITH filtered_data AS (
            SELECT
            timestamp,
            mc19->>'HMI_Filling_Stroke_Deg' as filling_stroke_deg
            FROM loop3_checkpoints
            WHERE
            timestamp >= NOW() - INTERVAL '7 days'
            AND mc19->>'HMI_Filling_Stroke_Deg' IS NOT NULL
            ), ranked_data AS (
            SELECT
            timestamp,
            filling_stroke_deg,
            LAG(filling_stroke_deg) OVER (ORDER BY timestamp) as prev_filling_stroke
            FROM filtered_data
            )
            SELECT
            timestamp,
            filling_stroke_deg
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
            print(f"Error in fetch_latest_filling_stroke at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()
            return "N/A"

    def calculate_baseline(self, df, current_column):
        try:
            mask = (df['cam_position'] >= 0) & (df['cam_position'] <= 360)
            range_data = df[mask]
            baseline = range_data.groupby('cam_position')[current_column].mean()
            return baseline, range_data
        except Exception as e:
            print(f"Error in calculate_baseline at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()
            return pd.Series(), pd.DataFrame()

    def adaptive_savgol_filter(self, data, window_length, polyorder):
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
            print(f"Error in adaptive_savgol_filter at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()
            return data

    def fetch_hopper_data(self):
        try:
            cursor = self.conn.cursor()
            recent_query = """
            SELECT MAX(timestamp) as latest_timestamp
            FROM mc19_short_data;
            """
            cursor.execute(recent_query)
            latest_timestamp = cursor.fetchone()[0]

            if latest_timestamp is None:
                raise ValueError("No data found in the database")

            fifteen_minutes_before = latest_timestamp - timedelta(minutes=15)

            query = """
            SELECT
                DATE_TRUNC('second', timestamp) as bucket_timestamp,
                AVG(hopper_1_level) as avg_hopper_1_level,
                AVG(status) as avg_status
            FROM mc19_short_data
            WHERE timestamp >= %s
            GROUP BY DATE_TRUNC('second', timestamp)
            ORDER BY bucket_timestamp ASC;
            """

            df = pd.read_sql_query(query, self.conn, params=[fifteen_minutes_before], parse_dates=['bucket_timestamp'])
            cursor.close()

            if df.empty:
                raise ValueError("No data found in the last 15 minutes")

            return df
        except Exception as e:
            print(f"Error in fetch_hopper_data at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()

    def detect_extrema(self, data, prominence=2, distance=10):
        try:
            if len(data) < 3:
                return np.array([]), np.array([]), {}, {}

            peaks, peak_properties = find_peaks(data, prominence=prominence, distance=distance)
            valleys, valley_properties = find_peaks(-data, prominence=prominence, distance=distance)

            return peaks, valleys, peak_properties, valley_properties
        except Exception as e:
            print(f"Error in detect_extrema at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()
            return np.array([]), np.array([]), {}, {}

    def calculate_and_track_slopes(self, peaks, valleys, hopper_data, timestamps):
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
            print(f"Error in calculate_and_track_slopes at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()

    def update_slope_display(self):
        try:
            if not self.recent_slopes:
                self.current_average_angle = None
                return

            avg_slope = sum(s['slope'] for s in self.recent_slopes) / len(self.recent_slopes)
            avg_level_change = sum(s['level_change'] for s in self.recent_slopes) / len(self.recent_slopes)
            avg_angle = sum(s['angle_degrees'] for s in self.recent_slopes) / len(self.recent_slopes)

            self.current_average_angle = avg_angle

        except Exception as e:
            print(f"Error in update_slope_display at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()

    def handle_underweight_detection(self):
        """Handle underweight detection with event logging and status update"""
        try:
            print("CONSENSUS: POSSIBLE UNDERWEIGHT - Triggering alerts")
            cursor_event = self.event_conn.cursor()
            cursor_tp = self.tp_status_conn.cursor()

            # Event Logging (INSERT into event_table)
            event_data = (
                datetime.now().strftime("%Y%m%d_%H%M%S"),
                str(uuid.uuid4()),
                "PLC",
                "mc19",
                '',
                "POSSIBLE UNDERWEIGHT",
                "quality"
            )
            insert_query = """INSERT INTO event_table (timestamp, event_id, zone, camera_id, filename, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            cursor_event.execute(insert_query, event_data)
            self.event_conn.commit()

            # Status Update (UPDATE mc19_tp_status)
            tp_data = json.dumps({
                "timestamp": datetime.now().isoformat(),
                "uuid": str(uuid.uuid4()),
                "active": 1,
                "filepath": 'http://192.168.1.148:8015/',
                "color_code": 3
            })
            update_query = """UPDATE mc19_tp_status SET tp67 = '{}' """.format(tp_data)
            cursor_tp.execute(update_query)
            self.tp_status_conn.commit()

            cursor_event.close()
            cursor_tp.close()

            print("✓ Underweight event logged and status updated")

        except Exception as e:
            print(f"Error in handle_underweight_detection at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()
            self.event_conn.rollback()
            self.tp_status_conn.rollback()

    def handle_overweight_detection(self):
        """Handle overweight detection with event logging and status update"""
        try:
            print("CONSENSUS: POSSIBLE OVERWEIGHT - Triggering alerts")
            cursor_event = self.event_conn.cursor()
            cursor_tp = self.tp_status_conn.cursor()

            # Event Logging (INSERT into event_table)
            event_data = (
                datetime.now().strftime("%Y%m%d_%H%M%S"),
                str(uuid.uuid4()),
                "PLC",
                "mc19",
                '',
                "POSSIBLE OVERWEIGHT",
                "quality"
            )
            insert_query = """INSERT INTO event_table (timestamp, event_id, zone, camera_id, filename, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            cursor_event.execute(insert_query, event_data)
            self.event_conn.commit()

            # Status Update (UPDATE mc19_tp_status)
            tp_data = json.dumps({
                "timestamp": datetime.now().isoformat(),
                "uuid": str(uuid.uuid4()),
                "active": 1,
                "filepath": 'http://192.168.1.148:8015/',
                "color_code": 3
            })
            update_query = """UPDATE mc19_tp_status SET tp66 = '{}' """.format(tp_data)
            cursor_tp.execute(update_query)
            self.tp_status_conn.commit()

            # Flag Management
            self.table_empty = False
            cursor_event.close()
            cursor_tp.close()

            print("✓ Overweight event logged and status updated")

        except Exception as e:
            print(f"Error in handle_overweight_detection at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()
            self.event_conn.rollback()
            self.tp_status_conn.rollback()

    def analyze_weight_status(self):
        try:
            if self.current_average_angle is None:
                individual_detection = "ANALYZING - Insufficient Data"
                print(f"\n=== WEIGHT ANALYSIS - {datetime.now().strftime('%H:%M:%S')} ===")
                print("Status: Insufficient data for analysis")
                print("=" * 50)
                return

            # Step 1: Individual Detection Logic (unchanged)
            avg_angle = self.current_average_angle
            viscosity = self.current_viscosity_status

            print(f"\n=== WEIGHT ANALYSIS - {datetime.now().strftime('%H:%M:%S')} ===")
            print(f"Average Angle: {avg_angle:.2f}°")
            print(f"Viscosity Status: {viscosity}")

            # Determine individual detection result
            if avg_angle < 27 and viscosity == "High":
                individual_detection = "POSSIBLE OVERWEIGHT"
            elif 27 <= avg_angle <= 67 and viscosity == "High":
                individual_detection = "POSSIBLE OVERWEIGHT"
            elif avg_angle > 67 and viscosity == "High":
                individual_detection = "POSSIBLE OVERWEIGHT"
            elif avg_angle < 27 and viscosity == "Low":
                individual_detection = "POSSIBLE UNDERWEIGHT"
            elif 27 <= avg_angle <= 67 and viscosity == "Low":
                individual_detection = "POSSIBLE UNDERWEIGHT"
            elif avg_angle > 67 and viscosity == "Low":
                individual_detection = "POSSIBLE UNDERWEIGHT"
            elif avg_angle < 27 and viscosity == "Normal":
                individual_detection = "POSSIBLE OVERWEIGHT"
            elif avg_angle > 67 and viscosity == "Normal":
                individual_detection = "POSSIBLE UNDERWEIGHT"
            else:
                individual_detection = "NORMAL WEIGHT"

            print(f"Individual Detection: {individual_detection}")

            # Step 2: Bucketing System - Add to rolling window
            self.detection_history.append(individual_detection)

            # Maintain rolling window of 5 detections
            if len(self.detection_history) > 16:
                self.detection_history.pop(0)

            print(f"Detection History ({len(self.detection_history)}/5): {self.detection_history}")

            # Step 3: Consensus Decision
            if len(self.detection_history) < 16:
                consensus_result = "NORMAL WEIGHT"  # Default to normal until we have 5 detections
                print(f"Consensus: {consensus_result} (Waiting for {16 - len(self.detection_history)} more detections)")
            else:
                # Check for unanimous consensus
                if all(detection == "POSSIBLE UNDERWEIGHT" for detection in self.detection_history):
                    consensus_result = "POSSIBLE UNDERWEIGHT"
                    print("Consensus: POSSIBLE UNDERWEIGHT (All 5 detections agree)")
                elif all(detection == "POSSIBLE OVERWEIGHT" for detection in self.detection_history):
                    consensus_result = "POSSIBLE OVERWEIGHT"
                    print("Consensus: POSSIBLE OVERWEIGHT (All 5 detections agree)")
                else:
                    consensus_result = "NORMAL WEIGHT"
                    print(f"Consensus: NORMAL WEIGHT (Mixed results: {self.detection_history})")

            print("=" * 50)

            # Step 4: Event Logging & Status Update (only if consensus changed)
            if consensus_result != self.previous_consensus:
                print(f"Consensus changed from {self.previous_consensus} to {consensus_result}")

                if consensus_result == "POSSIBLE UNDERWEIGHT":
                    self.handle_underweight_detection()
                elif consensus_result == "POSSIBLE OVERWEIGHT":
                    self.handle_overweight_detection()

                # Update previous consensus
                self.previous_consensus = consensus_result
            else:
                print(f"Consensus unchanged: {consensus_result}")

        except Exception as e:
            print(f"Error in analyze_weight_status at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()

    def update_analysis(self):
        try:
            display_data = self.fetch_latest_data(limit=2000)

            self.latest_filling_stroke = self.fetch_latest_filling_stroke()

            current_machine_status = display_data['machine_status'].iloc[-1]

            if self.previous_machine_status is None:
                self.previous_machine_status = current_machine_status

            if current_machine_status != self.previous_machine_status:
                if current_machine_status == 1:
                    self.waiting_for_restart = True
                    time.sleep(120)
                    self.waiting_for_restart = False
                self.previous_machine_status = current_machine_status

            if current_machine_status == 1 and not self.waiting_for_restart:
                baseline_data = self.fetch_latest_data(limit=8000)
                last_600_cycles = baseline_data['spare1'].unique()[-500:]
                baseline_df = baseline_data[baseline_data['spare1'].isin(last_600_cycles)]
                self.plot_fill_piston_data(display_data, baseline_df)
                hopper_df = self.fetch_hopper_data()
                self.analyze_hopper_data(hopper_df)

                self.analyze_weight_status()

        except Exception as e:
            print(f"Error in update_analysis at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()

    def plot_fill_piston_data(self, df, baseline_df):
        try:
            last_10_cycles = df['spare1'].unique()[-10:]
            filtered_df = df[df['spare1'].isin(last_10_cycles)]
            latest_cycle = filtered_df['spare1'].max()

            baseline, baseline_points = self.calculate_baseline(baseline_df, self.current_column)

            best_values = filtered_df.groupby('cam_position')[self.current_column].mean().reset_index()
            best_values = best_values.sort_values('cam_position')
            window_length = min(2, len(best_values) - 1)
            if window_length % 2 == 0:
                window_length -= 1
            polyorder = 3

            minimum_data = None
            deviation_alerts = []

            if len(best_values) > 3:
                smoothed_current = self.adaptive_savgol_filter(best_values[self.current_column], window_length, polyorder)

                smoothed_df = pd.DataFrame({
                    'cam_position': best_values['cam_position'],
                    'smoothed_current': smoothed_current
                })

                smoothed_in_range = smoothed_df[
                    (smoothed_df['cam_position'] >= self.cam_min) &
                    (smoothed_df['cam_position'] <= self.cam_max)
                ]

                if len(smoothed_in_range) > 0:
                    min_idx = smoothed_in_range['smoothed_current'].idxmin()
                    if not pd.isna(min_idx):
                        min_row = smoothed_in_range.loc[min_idx]
                        min_cam = min_row['cam_position']
                        min_val = min_row['smoothed_current']

                        if min_cam in baseline.index:
                            baseline_val = baseline[min_cam]
                            if min_val < baseline_val - self.threshold_value:
                                deviation_alerts.append(("WARNING", "Possible high viscosity"))
                                self.current_viscosity_status = "High"
                            elif min_val > baseline_val + self.threshold_value:
                                deviation_alerts.append(("WARNING", "Possible low viscosity"))
                                self.current_viscosity_status = "Low"
                            else:
                                self.current_viscosity_status = "Normal"

            baseline_series = baseline.copy()
            high_threshold = baseline_series + self.threshold_value
            low_threshold = baseline_series - self.threshold_value

            cam_positions_in_range = [p for p in baseline.index if self.cam_min <= p <= self.cam_max]

            if minimum_data:
                min_cam, min_val = minimum_data
                min_text = f'Minimum: {min_val:.2f}A at Cam {min_cam:.1f}'

                status = "OK"
                status_color = "black"
                alert_message = ""

                if min_val < self.const_threshold_2:
                    status = "CRITICAL"
                    status_color = "red"
                    alert_message = "High load on Filling Piston"
                elif self.const_threshold_2 <= min_val < self.const_threshold_1:
                    status = "OK"
                    status_color = "green"
                elif min_val > self.const_threshold_1:
                    status = "CRITICAL"
                    status_color = "red"
                    alert_message = "Possibility of Filling Piston Leakage/Foaming"

                if status == "OK" and (self.const_threshold_2 <= min_val < self.const_threshold_2 + self.approach_buffer):
                    status = "WARNING"
                    status_color = "orange"
                    alert_message = "High load on Filling Piston"
                elif status == "OK" and (self.const_threshold_1 - self.approach_buffer < min_val < self.const_threshold_1):
                    status = "WARNING"
                    status_color = "orange"
                    alert_message = "Possibility of Filling Piston Leakage/Foaming"

                if deviation_alerts:
                    for alert_status, alert_msg in deviation_alerts:
                        if status == "OK" or (status == "WARNING" and alert_status == "CRITICAL"):
                            status = alert_status
                            status_color = "orange" if alert_status == "WARNING" else "red"
                        if alert_message:
                            alert_message += f", {alert_msg}"
                        else:
                            alert_message = alert_msg

            info_text = (f'Time: {self.latest_timestamp.strftime("%H:%M:%S") if self.latest_timestamp else "N/A"}\n'
                        f'Cycle: {latest_cycle}\n'
                        f'Stroke: {self.latest_filling_stroke}')

        except Exception as e:
            print(f"Error in plot_fill_piston_data at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()

    def analyze_hopper_data(self, df):
        try:
            df = df.sort_values('bucket_timestamp').reset_index(drop=True)

            hopper_levels = df['avg_hopper_1_level'].values
            timestamps = df['bucket_timestamp'].values
            time_numeric = np.arange(len(timestamps))

            peaks, valleys, peak_props, valley_props = self.detect_extrema(hopper_levels)

            self.calculate_and_track_slopes(peaks, valleys, hopper_levels, timestamps)
            self.update_hopper_plot(time_numeric, hopper_levels, peaks, valleys, timestamps)

        except Exception as e:
            print(f"Error in analyze_hopper_data at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()

    def update_hopper_plot(self, time_data, hopper_data, peaks, valleys, timestamps):
        try:
            if len(peaks) > 0:
                for peak_idx in peaks:
                    if peak_idx < len(hopper_data):
                        level = hopper_data[peak_idx]

            if len(valleys) > 0:
                for valley_idx in valleys:
                    if valley_idx < len(hopper_data):
                        level = hopper_data[valley_idx]

            if len(timestamps) > 0:
                num_ticks = min(8, len(time_data))
                tick_indices = np.linspace(0, len(time_data)-1, num_ticks, dtype=int)
                tick_labels = []

                for idx in tick_indices:
                    if idx < len(timestamps):
                        if hasattr(timestamps[idx], 'strftime'):
                            tick_labels.append(timestamps[idx].strftime('%H:%M'))
                        else:
                            timestamp_str = str(timestamps[idx])
                            if len(timestamp_str) >= 16:
                                tick_labels.append(timestamp_str[11:16])
                            else:
                                tick_labels.append(timestamp_str)

        except Exception as e:
            print(f"Error in update_hopper_plot at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
            traceback.print_exc()

def main():
    try:
        analyzer = CombinedAnalyzer()
        while True:
            analyzer.update_analysis()
            time.sleep(0.6)
    except Exception as e:
        print(f"Error in main at line {sys.exc_info()[-1].tb_lineno}: {str(e)}")
        traceback.print_exc()
        if 'analyzer' in locals() and hasattr(analyzer, '__del__'):
            analyzer.__del__()

if __name__ == "__main__":
    main()
