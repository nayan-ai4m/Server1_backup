import sys
import os
import random
import psycopg2
import json
import time
import datetime
import logging
import threading
from paho.mqtt import client as mqtt_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class MQTTDatabaseHandler:
    def __init__(self, broker, port, db_config, topic="iotgatewayloop4"):
        self.broker = broker
        self.port = port
        self.db_config = db_config
        self.topic = topic
        self.client_id = f'subscribe-{random.randint(0, 100)}'

        # Load JSON configurations
        try:
            with open('mc27_high_speed.json', 'r') as f:
                self.mc27_high_speed_tags = json.load(f)['tags']
            with open('mc27_low_speed.json', 'r') as f:
                self.mc27_low_speed_tags = json.load(f)['tags']
            with open('mc30_high_speed.json', 'r') as f:
                self.mc30_high_speed_tags = json.load(f)['tags']
            with open('mc30_low_speed.json', 'r') as f:
                self.mc30_low_speed_tags = json.load(f)['tags']
        except FileNotFoundError as e:
            logger.error(f"JSON configuration file not found: {e}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON configuration: {e}")
            sys.exit(1)

        # Generate dynamic SQL queries
        self.mc27_high_speed_query = self.generate_insert_query('mc27', self.mc27_high_speed_tags)
        self.mc27_low_speed_query = self.generate_insert_query('mc27_mid', self.mc27_low_speed_tags, spare_count=5)
        self.mc30_high_speed_query = self.generate_insert_query('mc30', self.mc30_high_speed_tags)
        self.mc30_low_speed_query = self.generate_insert_query('mc30_mid', self.mc30_low_speed_tags, spare_count=5)

        # Data storage
        self.mc27_data = {}
        self.mc30_data = {}
        self.mc27_low_speed_buffer = {}
        self.mc30_low_speed_buffer = {}
        self.last_low_speed_insert = 0
        self.low_speed_interval = 5  # 5 seconds for low-speed data
        self.high_speed_interval = 0.03  # 30ms for high-speed data
        self.data_lock = threading.Lock()

        # Status tracking
        self.last_message_time = time.time()
        self.timeout = 30  # Timeout in seconds for no data

        # Initialize connections
        self.conn = None
        self.cursor = None
        self.connect_db()
        self.client = self.connect_mqtt()

    def generate_insert_query(self, table_name, tags, spare_count=1):
        """Generate dynamic INSERT query based on tags"""
        columns = ['"timestamp"'] + [tag.lower().replace('mc_', '') for tag in tags]
        columns += [f'spare{i+1}' for i in range(spare_count)]
        values_placeholder = ', '.join(['%s'] * len(columns))
        return f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values_placeholder})"

    def connect_db(self):
        """Connect to PostgreSQL database with continuous retry"""
        while True:
            try:
                self.close_db_connection()
                self.conn = psycopg2.connect(**self.db_config)
                self.conn.autocommit = False
                self.cursor = self.conn.cursor()
                logger.info("Successfully connected to PostgreSQL database")
                return
            except Exception as e:
                logger.error(f"Database connection error: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def close_db_connection(self):
        """Safely close database connection"""
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
        except Exception as e:
            logger.error(f"Error closing cursor: {e}")
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
        self.cursor = None
        self.conn = None

    def ensure_db_connection(self):
        """Ensure we have a valid database connection"""
        try:
            if self.conn is None or self.conn.closed != 0 or self.cursor is None or self.cursor.closed:
                logger.warning("Database connection lost. Reconnecting...")
                self.connect_db()
            return True
        except Exception as e:
            logger.error(f"Error checking database connection: {e}")
            return False

    def connect_mqtt(self):
        """Connect to MQTT broker"""
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logger.info("Connected to MQTT Broker!")
                self.subscribe()
            else:
                logger.error(f"Failed to connect to MQTT, return code {rc}")

        def on_disconnect(client, userdata, rc):
            logger.warning(f"Disconnected with return code {rc}")
            if rc != 0:
                logger.info("Unexpected disconnection. Attempting to reconnect...")

        client = mqtt_client.Client(self.client_id)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = self.on_message
        client.reconnect_delay_set(min_delay=1, max_delay=60)

        try:
            client.connect(self.broker, self.port, keepalive=60)
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            self.reconnect()
        return client

    def reconnect(self):
        """Reconnect to MQTT broker"""
        logger.info("Attempting to reconnect to MQTT broker...")
        while True:
            try:
                self.client.reconnect()
                break
            except Exception as e:
                logger.error(f"Reconnect failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def process_item(self, item):
        """Process MQTT item to extract machine, tag, and value"""
        split_id = item['id'].split('.')
        return split_id[1], split_id[2], {'value': item['value'], 'quality': item['quality'], 'ts': item['ts']}

    def insert_data_into_table(self, table_name, data, tags, spare_count=1):
        """Insert data into the specified table"""
        try:
            with self.conn.cursor() as cur:
                values = [data['timestamp']['value']]
                for tag in tags:
                    val = data.get(tag, {}).get('value', None)
                    if val == '':
                        values.append(None)
                    else:
                        try:
                            values.append(float(val))
                        except (ValueError, TypeError):
                            values.append(val)
                values += [None] * spare_count  # Add None for spare columns
                query = self.generate_insert_query(table_name, tags, spare_count)
                cur.execute(query, values)
                self.conn.commit()
                logger.info(f"Successfully inserted data into {table_name}")
        except psycopg2.OperationalError as e:
            logger.error(f"Database operational error: {e}. Will reconnect.")
            self.conn.rollback()
            self.connect_db()
        except psycopg2.InterfaceError as e:
            logger.error(f"Database interface error: {e}. Will reconnect.")
            self.conn.rollback()
            self.connect_db()
        except Exception as e:
            logger.error(f"Error inserting data into {table_name}: {e}")
            self.conn.rollback()

    def on_message(self, client, userdata, msg):
        """Process incoming MQTT messages"""
        self.last_message_time = time.time()
        data = json.loads(msg.payload.decode())

        with self.data_lock:
            # Reset data dictionaries for new message
            self.mc27_data = {}
            self.mc30_data = {}

            for item in data["values"]:
                machine, tag, processed_value = self.process_item(item)
                if machine == 'mc27':
                    self.mc27_data[tag] = processed_value
                elif machine == 'mc30':
                    self.mc30_data[tag] = processed_value

            # Update buffers with timestamp
            timestamp = {'value': str(datetime.datetime.now())}
            if self.mc27_data:
                self.mc27_data["timestamp"] = timestamp
                self.mc27_low_speed_buffer = self.mc27_data.copy()
            if self.mc30_data:
                self.mc30_data["timestamp"] = timestamp
                self.mc30_low_speed_buffer = self.mc30_data.copy()

    def process_high_speed_data(self):
        """Insert high-speed data into mc27 and mc30 every 30ms"""
        while True:
            try:
                if not self.ensure_db_connection():
                    logger.error("Failed to establish database connection. Will retry.")
                    time.sleep(1)
                    continue

                with self.data_lock:
                    timestamp = {'value': str(datetime.datetime.now())}
                    if self.mc27_data:
                        self.mc27_data["timestamp"] = timestamp
                        self.insert_data_into_table("mc27", self.mc27_data, self.mc27_high_speed_tags)
                    if self.mc30_data:
                        self.mc30_data["timestamp"] = timestamp
                        self.insert_data_into_table("mc30", self.mc30_data, self.mc30_high_speed_tags)

                time.sleep(self.high_speed_interval)  # 30ms
            except Exception as e:
                logger.error(f"Error processing high-speed data: {e}")
                time.sleep(1)

    def process_low_speed_data(self):
        """Insert low-speed data into mc27_mid and mc30_mid every 5 seconds"""
        while True:
            try:
                if not self.ensure_db_connection():
                    logger.error("Failed to establish database connection. Will retry.")
                    time.sleep(1)
                    continue

                current_time = time.time()
                if current_time - self.last_low_speed_insert >= self.low_speed_interval:
                    with self.data_lock:
                        timestamp = {'value': str(datetime.datetime.now())}
                        if self.mc27_low_speed_buffer:
                            self.mc27_low_speed_buffer["timestamp"] = timestamp
                            self.insert_data_into_table("mc27_mid", self.mc27_low_speed_buffer, self.mc27_low_speed_tags, spare_count=5)
                        if self.mc30_low_speed_buffer:
                            self.mc30_low_speed_buffer["timestamp"] = timestamp
                            self.insert_data_into_table("mc30_mid", self.mc30_low_speed_buffer, self.mc30_low_speed_tags, spare_count=5)
                        self.last_low_speed_insert = current_time

                time.sleep(0.1)  # Check every 100ms to avoid busy-waiting
            except Exception as e:
                logger.error(f"Error processing low-speed data: {e}")
                time.sleep(1)

    def subscribe(self):
        """Subscribe to MQTT topic"""
        self.client.subscribe(self.topic)
        logger.info(f"Subscribed to topic: {self.topic}")

    def check_data_flow(self):
        """Check if data is received within the timeout period"""
        while True:
            if time.time() - self.last_message_time > self.timeout:
                logger.warning(f"No data received for {self.timeout} seconds. Reconnecting...")
                self.reconnect()
            time.sleep(5)

    def run(self):
        """Start all background threads and MQTT loop"""
        logger.info("Starting MQTTDatabaseHandler threads")
        threads = [
            threading.Thread(target=self.check_data_flow, daemon=True, name="DataFlowThread"),
            threading.Thread(target=self.process_high_speed_data, daemon=True, name="HighSpeedDataThread"),
            threading.Thread(target=self.process_low_speed_data, daemon=True, name="LowSpeedDataThread")
        ]

        for thread in threads:
            thread.start()

        # Start MQTT loop in main thread
        self.client.loop_forever()

if __name__ == '__main__':
    db_config = {
        'host': '100.64.84.30',
        'dbname': 'hul',
        'user': 'postgres',
        'password': 'ai4m2024'
    }
    mqtt_handler = MQTTDatabaseHandler('192.168.1.149', 1883, db_config, topic="iotgatewayloop4")
    mqtt_handler.run()
