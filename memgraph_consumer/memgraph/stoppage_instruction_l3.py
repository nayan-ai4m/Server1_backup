import json
import psycopg2
import psycopg2.extras
from datetime import datetime
import uuid
import time
import os
import sys
import logging
import threading
from kafka import KafkaConsumer
from kafka.errors import KafkaError

class StoppageConsumer:
    def __init__(self, config_file: str = "config.json"):

        self.logger = logging.getLogger(__name__)

        # Load configuration from config.json
        config = self.load_config(config_file)
        if not config:
            self.logger.error("Failed to load configuration. Exiting...")
            sys.exit(1)

        self.source_db_config = config.get("source_database", {})
        self.target_db_config = config.get("target_database", {})
        kafka_config = config.get("kafka", {})

        if not self.source_db_config or not self.target_db_config or not kafka_config:
            self.logger.error("Missing source_database, target_database, or kafka configuration in config.json. Exiting...")
            sys.exit(1)

        self.kafka_broker = kafka_config.get('broker', '192.168.1.149:9092')
        self.kafka_topic = kafka_config.get('topic', 'l3_stoppage_code')

        # Add machines
        self.machine_status = {
            **{f'mc{mc}': None for mc in range(17, 23)},
            "l3_case_erector": None,
            "l3_taping": None,
            "l3_check_weigher": None
        }

        # Map machines to their respective table names
        self.machine_table_map = {
            **{f'mc{mc}': f"mc{mc}_tp_status" for mc in range(17, 23)},
            "l3_case_erector": "case_erector_tp_status",
            "l3_taping": "tpmc_tp_status",
            "l3_check_weigher": "check_weigher_tp_status"
        }

        self.source_db = None
        self.target_db = None
        self.consumer = None
        self.connect_dbs()
        self.connect_kafka()

        self.select_query = "SELECT tp01 FROM {} LIMIT 1"
        self.insert_query = "INSERT INTO {} (tp01) VALUES (%s)"
        self.update_query = "UPDATE {} SET tp01 = %s"

    def load_config(self, config_file: str) -> dict:
        """Load configuration from a JSON file."""
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as file:
                    config = json.load(file)
                    print(f"Successfully loaded configuration from {config_file}")
                    return config
            else:
                self.logger.error(f"Configuration file {config_file} not found!")
                return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing configuration file {config_file}: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Error loading configuration file {config_file}: {e}")
            return {}

    def connect_dbs(self):
        """Establish connections to both source and target databases."""
        # Connect to source database (localhost)
        while True:
            try:
                if self.source_db and not self.source_db.closed:
                    self.source_db.close()
                self.source_db = psycopg2.connect(**self.source_db_config, connect_timeout=5)
                print(f"Successfully connected to source PostgreSQL database ({self.source_db_config['host']})")
                break
            except Exception as e:
                self.logger.error(f"Source database connection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)

        # Connect to target database (100.103.195.124)
        while True:
            try:
                if self.target_db and not self.target_db.closed:
                    self.target_db.close()
                self.target_db = psycopg2.connect(**self.target_db_config, connect_timeout=5)
                print(f"Successfully connected to target PostgreSQL database ({self.target_db_config['host']})")
                break
            except Exception as e:
                self.logger.error(f"Target database connection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def connect_kafka(self):
        """Establish connection to Kafka broker."""
        while True:
            try:
                self.consumer = KafkaConsumer(
                    self.kafka_topic,
                    bootstrap_servers=self.kafka_broker,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    reconnect_backoff_ms=1000,       # Start retry after 1s
                    reconnect_backoff_max_ms=10000,  # Max retry delay 10s
                    request_timeout_ms=30000,        # 30s timeout for requests
                    session_timeout_ms=10000         # Detect heartbeat failures in 10s
                )
                print(f"Connected to Kafka topic {self.kafka_topic}")
                return
            except Exception as e:
                self.logger.error(f"Kafka connection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def ensure_db_connection(self):
        """Check and ensure both database connections are active and usable."""
        try:
            if self.source_db is None or self.source_db.closed:
                self.logger.warning("Source database connection lost. Reconnecting...")
                self.connect_dbs()
                return False
            # Ping source DB
            with self.source_db.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()

            if self.target_db is None or self.target_db.closed:
                self.logger.warning("Target database connection lost. Reconnecting...")
                self.connect_dbs()
                return False
            # Ping target DB
            with self.target_db.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()

            return True
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            self.logger.error(f"Database ping failed: {e}. Reconnecting...")
            self.connect_dbs()
            return False
        except Exception as e:
            self.logger.error(f"Error checking database connections: {e}")
            return False

    def monitor_connections(self):
        """Periodically check connections to Kafka and databases."""
        while True:
            print(f"Monitoring connections...")
            self.ensure_db_connection()
            try:
                self.consumer.poll(timeout_ms=1000)
            except KafkaError as ke:
                self.logger.error(f"Kafka monitor error: {ke}. Reconnecting...")
                self.connect_kafka()
            time.sleep(300)  # Check every 5 minutes

    def fetch_machine_status(self, machine):
        """Fetch status from the hul database (source_db) for all machines."""
        if not self.ensure_db_connection():
            self.logger.error("Cannot fetch status without database connections")
            return None

        table = machine  # Table name is the machine name, e.g., 'mc17'
        query = f"SELECT status FROM {table} ORDER BY timestamp DESC LIMIT 1"

        cursor = None
        try:
            cursor = self.source_db.cursor()
            cursor.execute(query)
            record = cursor.fetchone()
            if record:
                status = record[0]
                if status is None:
                    return -1  # Sentinel for None/NULL
                else:
                    return int(status)
            else:
                return -1  # No record
        except Exception as e:
            self.logger.error(f"Error fetching status for {machine}: {e}")
            return None
        finally:
            if cursor:
                cursor.close()

    def poll_regular_machines(self):
        """Periodically poll the hul database for status of regular machines (mc17-mc22)."""
        while True:
            for mc in [f'mc{i}' for i in range(17, 23)]:
                status_code = self.fetch_machine_status(mc)
                if status_code is not None and self.machine_status[mc] != status_code:
                    self.handle_status_change(mc, status_code)
                    self.machine_status[mc] = status_code
            time.sleep(5)  # Poll every 5 seconds

    def handle_status_change(self, machine, status_code, retry_count=0, max_retries=3):
        """Process status change for a machine and update target database."""
        if not self.ensure_db_connection():
            self.logger.error("Cannot proceed without database connections")
            return

        source_cursor = None
        target_cursor = None
        try:
            table_name = self.machine_table_map.get(machine)
            if not table_name:
                self.logger.error(f"No table mapping found for {machine}")
                return

            # Query the source database
            target_cursor = self.target_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
            target_cursor.execute(self.select_query.format(table_name))
            record = target_cursor.fetchone()

            current_json = {}
            if record and record[0]:
                if isinstance(record[0], str):
                    try:
                        current_json = json.loads(record[0])
                    except json.JSONDecodeError:
                        current_json = {}
                elif isinstance(record[0], dict):
                    current_json = record[0]

            # Apply logic for active value
            is_special = machine in ["l3_case_erector", "l3_taping", "l3_check_weigher"]
            if machine in [f'mc{i}' for i in range(17, 23)]:
                active_value = 0 if status_code == 1 else 1  # mc17-mc22: active=0 if status=1, else active=1
            else:
                # Existing logic for special machines
                active_value = 1 if (is_special and status_code == 0) else 0 if is_special else 1 if status_code != 1 else 0

            new_data = {
                "uuid": current_json.get("uuid", str(uuid.uuid4())),
                "active": active_value,
                "timestamp": datetime.now().isoformat(),
                "color_code": 1,
                "stoppages_code": 0 if is_special else str(status_code) if status_code != 0 else current_json.get("stoppages_code", "0")
            }

            print(new_data)
            # Insert or update in the target database
            target_cursor = self.target_db.cursor(cursor_factory=psycopg2.extras.DictCursor)
            target_cursor.execute(self.select_query.format(table_name))
            target_record = target_cursor.fetchone()

            if target_record:
                target_cursor.execute(self.update_query.format(table_name), (json.dumps(new_data),))
                print(f"Updated tp01 for {machine} with status {status_code} in target database")
            else:
                target_cursor.execute(self.insert_query.format(table_name), (json.dumps(new_data),))
                print(f"Inserted row with tp01 for {machine} with status {status_code} in target database")

            self.target_db.commit()

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
            self.logger.error(f"Database error for {machine}: {db_err}. Reconnecting and retrying...")
            if retry_count < max_retries:
                self.connect_dbs()
                self.handle_status_change(machine, status_code, retry_count + 1, max_retries)
            else:
                self.logger.error(f"Max retries ({max_retries}) reached for {machine}. Skipping...")
        except Exception as e:
            self.logger.error(f"Error processing {machine}: {e}")
            if target_cursor and not self.target_db.closed:
                self.target_db.rollback()
            self.connect_dbs()
        finally:
            if source_cursor:
                source_cursor.close()
            if target_cursor:
                target_cursor.close()

    def process_messages(self):
        """Process messages from Kafka topic."""
        print(f"Listening for messages on topic '{self.kafka_topic}'...")
        while True:
            try:
                message_batch = self.consumer.poll(timeout_ms=1000)
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            data = message.value
                            print(data)
                            if not isinstance(data, dict):
                                self.logger.warning(f"Unexpected message format: {data}")
                                continue
                            for machine, status_code in data.items():
                                status_code = int(status_code)
                                if machine.startswith('mc'):
                                    self.logger.info(f"Ignoring Kafka message for regular machine {machine}")
                                    continue
                                if machine in self.machine_status and self.machine_status[machine] != status_code:
                                    self.handle_status_change(machine, status_code)
                                    self.machine_status[machine] = status_code
                        except Exception as e:
                            self.logger.error(f"Error processing message: {e}")
            except KafkaError as ke:
                self.logger.error(f"Kafka error: {ke}. Reconnecting...")
                self.connect_kafka()
            except Exception as e:
                self.logger.error(f"Unexpected error in consumer loop: {e}. Reconnecting in 5 seconds...")
                time.sleep(5)
                self.connect_kafka()

    def run(self):
        """Run the consumer with a monitoring thread."""
        monitor_thread = threading.Thread(target=self.monitor_connections, daemon=True)
        monitor_thread.start()
        poll_thread = threading.Thread(target=self.poll_regular_machines, daemon=True)
        poll_thread.start()
        try:
            self.process_messages()
        except KeyboardInterrupt:
            self.logger.info("Shutting down gracefully...")
            if self.source_db and not self.source_db.closed:
                self.source_db.close()
                self.logger.info("Source database connection closed")
            if self.target_db and not self.target_db.closed:
                self.target_db.close()
                self.logger.info("Target database connection closed")
            if self.consumer:
                self.consumer.close()
                self.logger.info("Kafka consumer closed")

if __name__ == '__main__':
    consumer = StoppageConsumer()
    consumer.run()
