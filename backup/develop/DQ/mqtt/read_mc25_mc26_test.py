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
from kafka import KafkaProducer
from kafka.errors import KafkaError

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
    def __init__(self):
        # MQTT configuration
        self.broker = '192.168.1.149'
        self.port = 1883
        self.topic = "loop4"
        self.client_id = f'subscribe-{random.randint(0, 100)}'

        # Database configuration
        self.db_settings = {
            'host': '100.64.84.30',
            'dbname': 'hul',
            'user': 'postgres',
            'password': 'ai4m2024'
        }

        # Load JSON configurations
        try:
            with open('mc25_high_speed.json', 'r') as f:
                self.mc25_high_speed_tags = json.load(f)['tags']
            with open('mc25_low_speed.json', 'r') as f:
                self.mc25_low_speed_tags = json.load(f)['tags']
            with open('mc26_high_speed.json', 'r') as f:
                self.mc26_high_speed_tags = json.load(f)['tags']
            with open('mc26_low_speed.json', 'r') as f:
                self.mc26_low_speed_tags = json.load(f)['tags']
        except FileNotFoundError as e:
            logger.error(f"JSON configuration file not found: {e}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON configuration: {e}")
            sys.exit(1)

        # Generate dynamic SQL queries
        self.mc25_high_speed_query = self.generate_insert_query('mc25', self.mc25_high_speed_tags)
        self.mc25_low_speed_query = self.generate_insert_query('mc25_mid', self.mc25_low_speed_tags, spare_count=5)
        self.mc26_high_speed_query = self.generate_insert_query('mc26', self.mc26_high_speed_tags)
        self.mc26_low_speed_query = self.generate_insert_query('mc26_mid', self.mc26_low_speed_tags, spare_count=5)

        # Kafka configuration
        self.kafka_topic = 'l4_stoppage_code'
        self.kafka_bootstrap_servers = '192.168.1.149:9092'
        self.producer = None

        # Data storage
        self.mc25_data = {}
        self.mc26_data = {}
        self.mc25_low_speed_buffer = {}
        self.mc26_low_speed_buffer = {}
        self.last_low_speed_insert = 0
        self.low_speed_interval = 5  # 5 seconds for low-speed data

        # Status tracking
        self.last_message_time = time.time()
        self.timeout = 30
        self.mc25_status_queue = []
        self.mc26_status_queue = []
        self.queue_lock = threading.Lock()

        # Initialize connections
        self.conn = None
        self.cursor = None
        self.connect_db()
        self.client = self.connect_mqtt()
        self.connect_kafka()

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
                self.conn = psycopg2.connect(**self.db_settings)
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

    def connect_kafka(self):
        """Connect to Kafka with continuous retry"""
        while True:
            try:
                if self.producer:
                    self.producer.close()
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5
                )
                logger.info(f"Successfully connected to Kafka at {self.kafka_bootstrap_servers}")
                return
            except Exception as e:
                logger.error(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def ensure_kafka_connection(self):
        """Ensure we have a valid Kafka connection"""
        try:
            if self.producer is None:
                logger.warning("Kafka connection lost. Reconnecting...")
                self.connect_kafka()
            return True
        except Exception as e:
            logger.error(f"Error checking Kafka connection: {e}")
            return False

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

    def process_item_as_key(self, item):
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

        # Reset data dictionaries for new message
        self.mc25_data = {}
        self.mc26_data = {}

        for item in data["values"]:
            machine, tag, processed_value = self.process_item_as_key(item)

            # Store status for Kafka
            if 'status' in tag.lower():
                with self.queue_lock:
                    if machine == 'mc25':
                        self.mc25_status_queue.append(processed_value['value'])
                    elif machine == 'mc26':
                        self.mc26_status_queue.append(processed_value['value'])

            # Store data for database insertion
            if machine == 'mc25':
                self.mc25_data[tag] = processed_value
            elif machine == 'mc26':
                self.mc26_data[tag] = processed_value

        # Add timestamp and insert high-speed data
        timestamp = {'value': str(datetime.datetime.now())}
        if self.mc25_data:
            self.mc25_data["timestamp"] = timestamp
            self.mc25_low_speed_buffer = self.mc25_data.copy()
            self.insert_data_into_table("mc25", self.mc25_data, self.mc25_high_speed_tags)
        if self.mc26_data:
            self.mc26_data["timestamp"] = timestamp
            self.mc26_low_speed_buffer = self.mc26_data.copy()
            self.insert_data_into_table("mc26", self.mc26_data, self.mc26_high_speed_tags)

        # Insert low-speed data every 5 seconds
        current_time = time.time()
        if current_time - self.last_low_speed_insert >= self.low_speed_interval:
            if self.mc25_low_speed_buffer:
                self.mc25_low_speed_buffer["timestamp"] = timestamp
                self.insert_data_into_table("mc25_mid", self.mc25_low_speed_buffer, self.mc25_low_speed_tags, spare_count=5)
            if self.mc26_low_speed_buffer:
                self.mc26_low_speed_buffer["timestamp"] = timestamp
                self.insert_data_into_table("mc26_mid", self.mc26_low_speed_buffer, self.mc26_low_speed_tags, spare_count=5)
            self.last_low_speed_insert = current_time

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

    def produce_kafka_messages(self):
        """Continuously process status codes from the queues and send to Kafka"""
        while True:
            try:
                if not self.ensure_kafka_connection():
                    continue

                mc25_message = None
                with self.queue_lock:
                    if self.mc25_status_queue:
                        mc25_message = {"mc25": self.mc25_status_queue.pop(0)}
                if mc25_message:
                    try:
                        self.producer.send(
                            self.kafka_topic,
                            value=mc25_message,
                        ).add_errback(lambda e: self._handle_kafka_error(e, mc25_message, 'mc25'))
                        logger.info(f"Successfully sent mc25 message to Kafka topic {self.kafka_topic}")
                    except Exception as e:
                        self._handle_kafka_error(e, mc25_message, 'mc25')

                mc26_message = None
                with self.queue_lock:
                    if self.mc26_status_queue:
                        mc26_message = {"mc26": self.mc26_status_queue.pop(0)}
                if mc26_message:
                    try:
                        self.producer.send(
                            self.kafka_topic,
                            value=mc26_message,
                        ).add_errback(lambda e: self._handle_kafka_error(e, mc26_message, 'mc26'))
                        logger.info(f"Successfully sent mc26 message to Kafka topic {self.kafka_topic}")
                    except Exception as e:
                        self._handle_kafka_error(e, mc26_message, 'mc26')

                time.sleep(0.001)
            except Exception as e:
                logger.error(f"Unexpected error in Kafka producer: {e}")
                time.sleep(1)

    def _handle_kafka_error(self, error, message, machine):
        """Handle Kafka errors by requeueing the message"""
        with self.queue_lock:
            if machine == 'mc25':
                self.mc25_status_queue.insert(0, message['mc25'])
            else:
                self.mc26_status_queue.insert(0, message['mc26'])
            logger.error(f"Kafka message failed for {machine}: {error}")
            self.connect_kafka()

    def run(self):
        """Start all background threads and MQTT loop"""
        logger.info("Starting MQTTDatabaseHandler threads")
        threads = [
            threading.Thread(target=self.check_data_flow, daemon=True, name="DataFlowThread"),
            threading.Thread(target=self.produce_kafka_messages, daemon=True, name="KafkaProducerThread")
        ]

        for thread in threads:
            thread.start()

        # Start MQTT loop in main thread
        self.client.loop_forever()

if __name__ == '__main__':
    MQTTDatabaseHandler().run()
