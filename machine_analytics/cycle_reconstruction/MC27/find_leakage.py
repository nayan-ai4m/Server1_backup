import psycopg2
import time
import signal
import sys,os
import json
from uuid import uuid4
from kafka import KafkaProducer
import datetime

class ThresholdMonitor:
    def __init__(self, db_params, table_name, query_interval=3):
        self.db_params = db_params
        self.event_db_params = {'host': '192.168.1.168', 'database': 'hul', 'user': 'postgres', 'password': 'ai4m2024'}
        self.raw_db_params = {'host': 'localhost', 'database': 'hul', 'user': 'postgres', 'password': 'ai4m2024'}
        self.table_name = table_name
        self.query_interval = query_interval
        self.running = True
        self.conn = None
        self.cursor = None
        self.event_cursor = None
        self.event_conn = None
        self.thresholds = self.load_thresholds()
        self.producer = KafkaProducer(bootstrap_servers=['192.168.1.149:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.topic = "suggestion"
        self.flag_status = [False,False,False,False,False]

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
        "filepath": "http://192.168.0.158:8015/mc27_detection_1942302886.png",
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
            self.raw_conn = psycopg2.connect(**self.raw_db_params)
            self.raw_cursor = self.raw_conn.cursor()
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
        if self.event_conn:
            self.event_conn.close()
            print("Database connection closed.")

    def generate_event_id(self, machine: str, status: int, timestamp: datetime) -> str:
        time_str = timestamp.strftime("%Y%m%d_%H%M%S")
        return f"{machine.upper()}_{time_str}_{status}"

    def insert_event(self):
        """Insert event into event_table, only if tp01 active is 0."""
        if not self.check_tp01_status():
            print("Skipping event insertion: tp01 active is not 0")
            return
        event_id = self.generate_event_id("mc27",'leakage', datetime.datetime.now())
        machine_name = 'mc27'
        query = """
            INSERT INTO event_table
            (timestamp, event_id, zone, camera_id, event_type, alert_type)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            datetime.datetime.now(),
            event_id,
            "PLC",
            'MC27',
            "Possible Low Sealing Quality",
            "Quality"
        ) 
        self.raw_cursor.execute(query,(params))
        self.raw_conn.commit()

    def check_tp01_status(self):
        """Check if tp01 column contains active: 0 in its JSON data."""
        try:
            query = """SELECT tp01 FROM mc27_tp_status;"""
            self.event_cursor.execute(query)
            result = self.event_cursor.fetchone()
            if result and result[0]:
                tp01_data = result[0]
                # Handle both JSON string and pre-parsed dict (from JSONB)
                if isinstance(tp01_data, dict):
                    return tp01_data.get('active', 1) == 0
                elif isinstance(tp01_data, str):
                    tp01_data = json.loads(tp01_data)
                    return tp01_data.get('active', 1) == 0
                else:
                    print(f"Unexpected tp01 data type: {type(tp01_data)}")
                    return False
            return False
        except Exception as e:
            print(f"Error checking tp01 status: {e}")
            return False

    def query_and_analyze(self):
        """Query the latest 10 rows and analyze thresholds."""
        try:
            # Query to get the latest 10 rows ordered by timestamp
            query = """SELECT timestamp, mae_seal_current, mae_sealer_position,
                   mae_seal_pressure, mae_front_temp, mae_rear_temp,seal_current_vector,0.0, seal_pressure_vector, rear_temp_vector, front_temp_vector
            FROM mc27_autoencoder
            ORDER BY timestamp DESC
            LIMIT 10;
            """
            current_query = """select timestamp,hor_sealer_front_1_temp as front_current_value,hor_sealer_rear_1_temp as rear_current_value from mc27 order by timestamp desc limit 1;"""
            setpoint_query = """ select timestamp,(mc27->'HMI_Ver_Seal_Rear_28'->>'SetValue')::float as front_value,(mc27->'HMI_Ver_Seal_Rear_27'->>'SetValue')::float as rear_setvalue ,(mc27->'HMI_Hor_Sealer_Strk_1')::float as stroke1 , (mc27->'HMI_Hor_Sealer_Strk_2')::float as stroke2 from loop3_checkpoints where mc27 != 'null' order by timestamp desc limit 1;"""
            # Execute query and fetch results
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            total_rows = len(rows)

            # Calculate the number of rows needed to meet 60% threshold
            threshold_rows = total_rows * 0.6

            # Check each column against its threshold
            results = {}
            
            for col_name, threshold in self.thresholds.items():
                col_index = list(self.thresholds.keys()).index(col_name) + 1  # +1 to skip timestamp
                exceeding_count = sum(1 for row in rows if row[col_index] > threshold)
                vector = sum(1 for row in rows if row[col_index+5] < 0)
                exceeds_60_percent = exceeding_count >= threshold_rows
                vector_60_percent = vector >= 5

                percentage = (exceeding_count / total_rows) * 100 if total_rows > 0 else 0
                results[col_name] = {
                    'exceeding_count': exceeding_count,
                    'exceeds_60_percent': exceeds_60_percent,
                    'percentage': percentage,
                    'vector': vector_60_percent,
                    'idx' :col_index - 1
                }

            # Print results
            results['mae_seal_pressure']['exceeds_60_percent'] = True
            results['mae_seal_pressure']['vector'] = True
            print("\nThreshold Analysis:")
            for col_name, result in results.items():
                print(f"\n{col_name}:")
                print(f"Threshold: {self.thresholds[col_name]}")
                print(f"Rows exceeding threshold: {result['exceeding_count']}/{total_rows}")
                print(f"Percentage: {result['percentage']:.2f}%")
                print(f"Exceeds 60% threshold: {result['exceeds_60_percent']}")
                print(f"Exceeds 60% vector: {result['vector']}")
                print("leakage flag :{}",self.flag_status[result['idx']])
                
                # Check tp01 status before performing inserts/updates
                if result['exceeds_60_percent'] and self.flag_status[result['idx']] == False and self.check_tp01_status():
                    if col_name == 'mae_seal_pressure':
                        query = """select AVG(hor_pressure) from (select hor_pressure from mc27_short_data order by timestamp desc limit 100) as latest_values;"""
                        self.cursor.execute(query)
                        rows = self.cursor.fetchone()
                        avg_pressure = rows[0]
                        self.raw_cursor.execute(current_query)
                        rows = self.raw_cursor.fetchone()
                        (front_temp,rear_temp) = rows[1:]
                        self.raw_cursor.execute(setpoint_query)
                        rows = self.raw_cursor.fetchone()
                        (setpoint_front_temp,setpoint_rear_temp,stroke1,stroke2) = rows[1:]

                        data = json.dumps(self.format_event(True))
                        
                        self.flag_status[result['idx']] = True
                        if col_name == 'mae_seal_pressure':
                            if result['vector']:
                                tpdata = json.dumps(self.format_event(True))
                                query = """UPDATE mc27_tp_status SET tp62 = '{}' """.format(tpdata)
                                self.event_cursor.execute(query)
                                self.event_conn.commit()
                                if self.check_tp01_status():  # Additional check before insert
                                    self.insert_event()
                                data = {"machine":27,"type":"pressure","setpoint":setpoint_front_temp,"current":front_temp,"timestamp":str(datetime.datetime.now())}
                                self.producer.send(self.topic,value=data)
                                data = {"machine":27,"type":"pressure","setpoints":{'s1':54.0,'s2':3,'front':154,'rear':154},"current":{'front':154,'rear':154,'pressure':1.83},"timestamp":str(datetime.datetime.now()),"alert":tpdata}
                                self.producer.send(self.topic,value=data)
                                print(data)
                    if col_name == 'mae_rear_temp' or col_name == 'mae_front_temp':
                        self.raw_cursor.execute(current_query)
                        rows = self.raw_cursor.fetchone()
                        (front_temp,rear_temp) = rows[1:]
                        self.raw_cursor.execute(setpoint_query)
                        rows = self.raw_cursor.fetchone()
                        (setpoint_front_temp,setpoint_rear_temp,stroke1,stroke2) = rows[1:]
                        self.flag_status[result['idx']] = True
                        if col_name == 'mae_front_temp':
                            if setpoint_front_temp > front_temp:
                                print("decreased")
                                tpdata = json.dumps(self.format_event(True))
                                query = """UPDATE mc27_tp_status SET tp61 = '{}' """.format(tpdata)
                                self.event_cursor.execute(query)
                                self.event_conn.commit()
                                if self.check_tp01_status():  # Additional check before insert
                                    self.insert_event()
                                data = {"machine":27,"type":"front_temp","setpoints":{'s1':stroke1,'s2':stroke2,'front':setpoint_front_temp,'rear':setpoint_rear_temp},"current":{'front':front_temp,'rear':rear_temp},"timestamp":str(datetime.datetime.now()),"alert":tpdata}
                                self.producer.send(self.topic,value=data)
                            else :
                                print("increased")
                        if col_name == 'mae_rear_temp':
                            print(setpoint_rear_temp,rear_temp)
                            if setpoint_rear_temp == rear_temp:
                                print("deccreased")
                                tpdata = json.dumps(self.format_event(True))
                                query = """UPDATE mc27_tp_status SET tp61 = '{}' """.format(tpdata)
                                self.event_cursor.execute(query)
                                self.event_conn.commit()
                                if self.check_tp01_status():  # Additional check before insert
                                    self.insert_event()
                                data = {"machine":27,"type":"rear_temp","setpoints":{'s1':stroke1,'s2':stroke2,'front':setpoint_front_temp,'rear':setpoint_rear_temp},"current":{'front':front_temp,'rear':rear_temp},"timestamp":str(datetime.datetime.now()),"alert":tpdata}
                                self.producer.send(self.topic,value=data)
                            else :
                                print("inreased")
                else:
                    if result['exceeds_60_percent'] == False and self.check_tp01_status():
                        if self.flag_status[result['idx']] == True:
                            self.flag_status[result['idx']] = False
                            query = """UPDATE mc27_tp_status SET tp61 = 'null' """
                            self.event_cursor.execute(query)
                            self.event_conn.commit()

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

        # Main loop
        while self.running:
            self.query_and_analyze()
            print(f"\nWaiting {self.query_interval} seconds before next query...")
            time.sleep(self.query_interval)

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
    monitor = ThresholdMonitor(db_params, table_name='mc27_short_data', query_interval=3)
    monitor.run()

