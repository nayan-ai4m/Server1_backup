import psycopg2
from flask import Flask, request
import json
import time
import logging
import os
from datetime import datetime
from kafka import KafkaProducer

# ===================================================================
# 1. DYNAMIC LOGGING: Auto-switch log file at midnight
# ===================================================================
BASE_LOG_DIR = "/home/ai4m/develop/ui_backend/logs"
_error_handler = None
_current_date = None

def get_error_log_handler():
    """Ensure error logs go to today's dated folder"""
    global _error_handler, _current_date
    today = datetime.now().date()

    if _current_date != today:
        # Remove old handler
        if _error_handler:
            _error_handler.close()
            logging.getLogger().removeHandler(_error_handler)

        # Create new dated directory and file
        dated_dir = os.path.join(BASE_LOG_DIR, today.strftime("%Y-%m-%d"))
        os.makedirs(dated_dir, exist_ok=True)
        log_file = os.path.join(dated_dir, "checkweigher_status.log")

        # New handler
        _error_handler = logging.FileHandler(log_file)
        _error_handler.setLevel(logging.ERROR)
        _error_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        logging.getLogger().addHandler(_error_handler)
        _current_date = today

    return _error_handler

def log_error(msg, *args, **kwargs):
    """Log error to today's file + console"""
    get_error_log_handler()
    logging.error(msg, *args, **kwargs)

# Console: Show WARNING+ (for dev/debug)
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# ===================================================================
# 2. Flask App & Config Load
# ===================================================================
app = Flask(__name__)

try:
    with open('config.json') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    log_error("Error: config.json not found")
    print("Error: config.json not found")
    exit(1)
except json.JSONDecodeError:
    log_error("Error: Invalid JSON in config.json")
    print("Error: Invalid JSON in config.json")
    exit(1)

# ===================================================================
# 3. KafkaManager Class
# ===================================================================
class KafkaManager:
    def __init__(self):
        self.l3_producer = None
        self.l4_producer = None
        try:
            self.config = CONFIG['kafka']
        except KeyError:
            log_error("'kafka' section not found in config.json")
            print("Error: 'kafka' section not found in config.json")
            exit(1)
        self.connect_kafka()

    def connect_kafka(self):
        while True:
            try:
                self.l3_producer = KafkaProducer(
                    bootstrap_servers=self.config['l3']['bootstrap_servers'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    request_timeout_ms=30000,
                    retries=5
                )
                self.l4_producer = KafkaProducer(
                    bootstrap_servers=self.config['l4']['bootstrap_servers'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    request_timeout_ms=30000,
                    retries=5
                )
                print("Connected to Kafka brokers.")
                return
            except Exception as e:
                log_error("Kafka connection failed: %s. Retrying in 5 seconds...", e)
                print(f"Kafka connection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def send_message(self, loop, value):
        try:
            if loop == 'l3':
                self.l3_producer.send(self.config['l3']['topic'], value=value)
                self.l3_producer.flush()
            elif loop == 'l4':
                self.l4_producer.send(self.config['l4']['topic'], value=value)
                self.l4_producer.flush()
            return True
        except Exception as e:
            log_error("Error sending to Kafka (loop=%s): %s", loop, e)
            print(f"Error sending to Kafka: {e}")
            self.connect_kafka()
            return False

kafka_manager = KafkaManager()

# ===================================================================
# 4. Database Config
# ===================================================================
DB_CONFIG = {
    'database': CONFIG['database']['dbname'],
    'user': CONFIG['database']['user'],
    'password': CONFIG['database']['password'],
    'host': CONFIG['database']['host'],
    'port': CONFIG['database']['port']
}

# ===================================================================
# 5. Flask Route: /update_status
# ===================================================================
@app.route('/update_status', methods=['POST'])
def update_status():
    try:
        data = request.get_json()
        if not data:
            log_error("No JSON data received in request")
            return 'Error: No JSON data', 400

        loop3_status = data.get('loop3_status', 'Offline')
        loop4_status = data.get('loop4_status', 'Offline')
        loop3_status_int = 1 if loop3_status == 'Running' else 0
        loop4_status_int = 1 if loop4_status == 'Running' else 0

        # Send to Kafka
        kafka_success = True
        if not kafka_manager.send_message('l3', {'l3_check_weigher': loop3_status_int}):
            kafka_success = False
        if not kafka_manager.send_message('l4', {'l4_check_weigher': loop4_status_int}):
            kafka_success = False

        # Update PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS checkweigher_status (
                singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
                loop3 VARCHAR(50) NOT NULL,
                loop4 VARCHAR(50) NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Upsert logic
        query = """
            INSERT INTO checkweigher_status (singleton, loop3, loop4, timestamp)
            VALUES (TRUE, %s, %s, NOW())
            ON CONFLICT (singleton)
            DO UPDATE SET
                loop3 = EXCLUDED.loop3,
                loop4 = EXCLUDED.loop4,
                timestamp = NOW()
        """
        cursor.execute(query, (loop3_status, loop4_status))
        conn.commit()
        cursor.close()
        conn.close()

        return 'Status updated', 200

    except psycopg2.Error as e:
        log_error("Database error: %s", e)
        return f'Database error: {str(e)}', 500
    except Exception as e:
        log_error("Unexpected error in /update_status: %s", e)
        return f'Error: {str(e)}', 500

# ===================================================================
# 6. Run App
# ===================================================================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
