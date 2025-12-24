import psycopg2
import time
import signal
import sys,os
import json
from uuid import uuid4
import datetime
#import plotext as plt
import numpy as np
class ThresholdMonitor:
    def __init__(self, db_params, table_name, query_interval=3):
        self.db_params = db_params
        self.event_db_params = {'host': 'localhost', 'database': 'hul', 'user': 'postgres', 'password': 'ai4m2024'}
        self.table_name = table_name
        self.query_interval = query_interval
        self.running = True
        self.conn = None
        self.cursor = None
        self.thresholds = self.load_thresholds()
        self.wrinkle_flag = False   
    def load_thresholds(self):
        """Load thresholds from JSON file."""
        try:
            with open('leakage_thresholds.json', 'r') as f:
                thresholds = json.load(f)
            # Validate required keys
            print(thresholds)
            required_keys = {'mae_seal_current', 'mae_sealer_position', 'mae_seal_pressure',
                            'mae_front_temp', 'mae_rear_temp'}
            if not all(key in thresholds for key in required_keys):
                raise ValueError("JSON file missing required threshold keys")
            # Validate numeric values
            for key, value in thresholds.items():
                if not isinstance(value, (int, float)):
                    raise ValueError(f"Threshold for {key} must be a number")
            print(f"Thresholds loaded from  {thresholds}")
            return thresholds
        except FileNotFoundError:
            print(f"Error: Threshold file not found")
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON format in ")
            sys.exit(1)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)
    def format_event(self,is_state):
        return {
        "timestamp": datetime.datetime.now().isoformat(),
        "uuid": str(uuid4()),
        "active": 1 if is_state else 0,
        "filepath": "http://192.168.0.158:8015/mc20_detection_1742302886.png",
        "color_code": 1,
        "machine_part": "plc"
        }   
    def connect(self):
        """Establish a single database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.event_conn = psycopg2.connect(**self.event_db_params)
            self.cursor = self.conn.cursor()

            self.event_cursor = self.event_conn.cursor()
            print("Database connection established.")
        except Exception as e:
            print(f"Failed to connect to database: {e}")
            sys.exit(1)

    def disconnect(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("Database connection closed.")
    def generate_event_id(self, machine: str, status: int, timestamp: datetime) -> str:
        time_str = timestamp.strftime("%Y%m%d_%H%M%S")
        return f"{machine.upper()}_{time_str}_{status}"

    def insert_event(self ) :

        event_id = self.generate_event_id("mc20",'leakage', datetime.datetime.now())
        machine_name = 'mc20'
        #fault_message = self.fault_manager.get_message(machine_name, int(fault))

        query = """
            INSERT INTO event_table
            (timestamp, event_id, zone, camera_id, event_type, alert_type)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            datetime.datetime.now(),
            event_id,
            "PLC",
            'MC20',
            "Possible Wrinkle Detected",
            "Quality"
        ) 
        self.event_cursor.execute(query,(params))
        self.event_conn.commit()

    def query_and_analyze(self):
        """Query the latest 10 rows and analyze thresholds."""
        try:
            # Query to get the latest 10 rows ordered by timestamp
            query = """SELECT timestamp,mae_seal_pressure
            FROM mc20_autoencoder
            ORDER BY timestamp DESC
            
            LIMIT 100;
            """
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            total_rows = len(rows)
            pressure = []
            threshold_rows = total_rows * 0.6
            for row in rows:
                pressure.append(row[1])
            # Check each column against its threshold
            results = {}
            return pressure
                            
        except Exception as e:
            print(f"An error occurred during query/analysis: {e}")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
    def run(self):
        """Run the monitoring loop."""
        # Establish single connection
        self.connect()
        # Register signal handler for Ctrl+C
        def signal_handler(sig, frame):
            self.running = False
            print("\nShutting down gracefully...")

        signal.signal(signal.SIGINT, signal_handler)
        #plt.title("Streaming Data")
        #plt.plotsize(100, 100)
        #plt.xlabel('Time')
        # Main loop
        while self.running:

            pressure_spread = self.query_and_analyze()
            pressure = np.array(pressure_spread)
            peak_to_peak = np.max(pressure) - np.min(pressure)
            amplitude = peak_to_peak / 2
            mean_signal = np.mean(pressure)
            mad = np.mean(np.abs(pressure - mean_signal))
            std_dev = np.std(pressure)
            print('mean:{} ,lower :{}, upper:{} ,spead = {}'.format(mean_signal,mean_signal-std_dev,mean_signal+std_dev,2*std_dev))
            #std_dev = np.full(100,std_dev)
            #mean_signal = np.full(100,mean_signal)
            #lower = np.full(100,mean_signal- std_dev)
            #upper = np.full(100,mean_signal + std_dev)
            if (2 * std_dev > 0.049) and self.wrinkle_flag == False:
                print('mean:{} ,lower :{}, upper:{} ,spead = {}'.format(mean_signal,mean_signal-std_dev,mean_signal+std_dev,2*std_dev))
                data = self.format_event(True)
                query = """UPDATE mc20_tp_status SET tp63 = '{}' """.format(json.dumps(data))
                print(query)
                self.event_cursor.execute(query)
                self.event_conn.commit()
                self.insert_event()
                self.wrinkle_flag = True
            if (2 * std_dev < 0.049) and self.wrinkle_flag == True:
                self.wrinkle_flag = False
            #plt.plot(mean_signal,label='mean')
            #plt.plot(upper,label='uuper')
            #plt.plot(lower,label = 'lower')
            #plt.show()
            #print(f"Amplitude (half peak-to-peak): {amplitude:.4f}")
            #print(f"Mean Absolute Deviation (MAD): {mad:.4f}")
            #print(f"Standard Deviation: {std_dev:.4f}")
            #print(f"\nWaiting {self.query_interval} seconds before next query...")
            time.sleep(self.query_interval)
            #plt.sleep(0.06)
        # Clean up
        self.disconnect()
        print("Program terminated.")

# Usage
if __name__ == "__main__":
    db_params = {
        'dbname': 'short_data_hul',
        'user': 'postgres',
        'password': 'ai4m2024',
        'host': 'localhost',
        'port': '5432'
    }
    monitor = ThresholdMonitor(db_params, table_name='mc20_short_data', query_interval=3)
    monitor.run()


