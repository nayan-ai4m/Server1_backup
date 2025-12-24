import sys
import json
import time
import datetime
import logging
import threading
from pycomm3 import LogixDriver
import psycopg2

# Database configuration
DB_SETTINGS = {
    'host': '100.64.84.30',  # Updated from 'localhost'; adjust if necessary
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def get_shift_column():
    """Determine the shift column based on the current hour."""
    current_hour = datetime.datetime.now().hour
    if 7 <= current_hour < 15:
        return 'cld_a'
    elif 15 <= current_hour < 23:
        return 'cld_b'
    else:
        return 'cld_c'

class PLCDatabaseHandler:
    def __init__(self, plc_ip, db_config):
        """Initialize PLC and database handler."""
        self.plc_ip = plc_ip
        self.db_config = db_config

        # Load high-speed tag configurations from JSON
        try:
            with open('mc28_high_speed.json', 'r') as f:
                self.mc28_high_speed_tags = json.load(f)['tags']
            with open('mc29_high_speed.json', 'r') as f:
                self.mc29_high_speed_tags = json.load(f)['tags']
        except Exception as e:
            logger.error(f"Error loading JSON configuration: {e}")
            sys.exit(1)

        # Generate SQL insert queries
        self.mc28_high_speed_query = self.generate_insert_query('mc28', self.mc28_high_speed_tags)
        self.mc29_high_speed_query = self.generate_insert_query('mc29', self.mc29_high_speed_tags)

        # Data storage with thread-safe access
        self.mc28_data = {}
        self.mc29_data = {}
        self.data_lock = threading.Lock()

        # Initialize database connection
        self.conn = None
        self.cursor = None
        self.connect_db()

    def generate_insert_query(self, table_name, tags):
        """Generate a dynamic INSERT query based on tag list."""
        columns = ['"timestamp"'] + [tag.lower() for tag in tags]
        values_placeholder = ', '.join(['%s'] * len(columns))
        return f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values_placeholder})"

    def connect_db(self):
        """Establish connection to PostgreSQL database with retry logic."""
        while True:
            try:
                self.conn = psycopg2.connect(**self.db_config)
                self.cursor = self.conn.cursor()
                logger.info("Connected to database")
                return
            except Exception as e:
                logger.error(f"Database connection error: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def read_plc_tags(self):
        """Read tags from PLC every 100ms and update shared data."""
        while True:
            try:
                with LogixDriver(self.plc_ip) as plc:
                    while True:
                        with self.data_lock:
                            # Read MC28 tags
                            tag_values = plc.read("MC28_STATUS", "MC28_CLD")
                            self.mc28_data = {
                                'state': {'value': str(tag_values[0].value)},
                                'cld': {'value': str(tag_values[1].value)}
                            }
                            # Read MC29 tags
                            tag_values = plc.read("MC29_STATUS", "MC29_CLD")
                            self.mc29_data = {
                                'state': {'value': str(tag_values[0].value)},
                                'cld': {'value': str(tag_values[1].value)}
                            }
                        time.sleep(0.1)  # Read every 100ms
            except Exception as e:
                logger.error(f"PLC connection error: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def insert_data_into_table(self, table_name, data, tags, query):
        """Insert data into the specified table."""
        try:
            with self.conn.cursor() as cur:
                values = [str(datetime.datetime.now())]
                shift_column = get_shift_column()
                for tag in tags:
                    if tag == 'state':
                        val = data.get('state', {}).get('value', None)
                    elif tag in ('cld_a', 'cld_b', "cld_c"):
                        val = data.get('cld', {}).get('value', None) if tag == shift_column else None
                    else:
                        val = None
                    values.append(val)
                cur.execute(query, values)
                self.conn.commit()
                logger.info(f"Inserted data into {table_name}")
        except Exception as e:
            logger.error(f"Error inserting into {table_name}: {e}")
            self.conn.rollback()

    def process_high_speed_data(self):
        """Insert high-speed data into the database every 30ms."""
        while True:
            with self.data_lock:
                if self.mc28_data:
                    self.insert_data_into_table(
                        'mc28', self.mc28_data, self.mc28_high_speed_tags, self.mc28_high_speed_query
                    )
                if self.mc29_data:
                    self.insert_data_into_table(
                        'mc29', self.mc29_data, self.mc29_high_speed_tags, self.mc29_high_speed_query
                    )
            time.sleep(0.03)  # Insert every 30ms

    def run(self):
        """Start PLC reading and high-speed data processing threads."""
        threads = [
            threading.Thread(target=self.read_plc_tags, name="PLCReadThread"),
            threading.Thread(target=self.process_high_speed_data, name="HighSpeedDataThread")
        ]
        for thread in threads:
            thread.daemon = True  # Allow program to exit even if threads are running
            thread.start()
        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Program terminated by user")
            sys.exit(0)

if __name__ == '__main__':
    plc_ip = '141.141.143.20'
    handler = PLCDatabaseHandler(plc_ip, DB_SETTINGS)
    handler.run()
