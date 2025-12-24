import psycopg2
from datetime import datetime, timedelta
from uuid import uuid4
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RollEndSensor_TP:
    def __init__(self, config_file):
        print("🚀 MC17 Roll End Sensor System Starting...")
        
        try:
            with open(config_file) as f:
                content = f.read()
                logging.info(f"Config file content: {content}")
                self.config = json.loads(content)
                print("✅ Config loaded successfully")
        except FileNotFoundError:
            print(f"❌ Config file {config_file} not found")
            logging.error(f"Config file {config_file} not found")
            raise
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in config: {e}")
            logging.error(f"Invalid JSON in {config_file}: {e}")
            raise

        # Initialize read database connection (localhost)
        try:
            self.read_conn = psycopg2.connect(**self.config["read_db"])
            self.read_cursor = self.read_conn.cursor()
            print("✅ Read database connected")
            logging.info("Successfully connected to read database (localhost)")
        except psycopg2.Error as e:
            print(f"❌ Read database connection failed: {e}")
            logging.error(f"Failed to connect to read database: {e}")
            raise

        # Initialize write database connection (remote)
        try:
            self.write_conn = psycopg2.connect(**self.config["write_db"])
            self.write_cursor = self.write_conn.cursor()
            print("✅ Write database connected")
            logging.info("Successfully connected to write database (141.141.141.128)")
        except psycopg2.Error as e:
            print(f"❌ Write database connection failed: {e}")
            logging.error(f"Failed to connect to write database: {e}")
            raise

        # Database queries
        self.fetch_mq_query = "SELECT timestamp, status FROM mc17 ORDER BY timestamp DESC LIMIT 1;"
        self.fetch_rq_query = '''
            SELECT timestamp, mc17 ->> 'ROLL_END_SENSOR' AS roll_end
            FROM loop3_checkpoints
            WHERE mc17 ->> 'ROLL_END_SENSOR' IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1;
        '''
        
        self.insert_work_instruction_query = """
            INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type, operator_type) 
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        
        self.insert_event_query = """
            INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type) 
            VALUES (%s, %s, %s, %s, %s, %s);
        """

        # State tracking variables - Updated flag system
        self.flag = 0  # 0 initially, 1 when roll end detected
        self.new_roll_timer_start = None  # Timer for MQ≠1, RQ=1 condition
        
        print(f"✅ System initialized - Flag: {self.flag}")

    @staticmethod
    def format_event_data(is_problem):
        """Format event data for TP status updates"""
        timestamp = datetime.now().isoformat()
        return {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": timestamp,
            "color_code": 3 if is_problem else 1
        }

    def insert_or_update_tp(self, column, event_data):
        """Insert or update TP status table"""
        check_query = f"SELECT {column} FROM mc17_tp_status;"
        update_query = f"UPDATE mc17_tp_status SET {column} = %s;"
        insert_query = f"INSERT INTO mc17_tp_status ({column}) VALUES (%s);"

        try:
            self.write_cursor.execute(check_query)
            exists = self.write_cursor.fetchone()
            if exists:
                logging.info(f"Updating {column} with event data: {event_data}")
                self.write_cursor.execute(update_query, (json.dumps(event_data),))
            else:
                logging.info(f"Inserting new {column} with event data: {event_data}")
                self.write_cursor.execute(insert_query, (json.dumps(event_data),))
            self.write_conn.commit()
        except psycopg2.Error as e:
            logging.error(f"Error updating/inserting {column} in mc17_tp_status: {e}")
            self.write_conn.rollback()

    def send_work_instruction(self, message, alert_type, operator_type=None):
        """Send work instruction to specific operator(s)"""
        timestamp = datetime.now()
        event_id = str(uuid4())
        
        try:
            if operator_type:
                data = (timestamp, event_id, "PLC", "MC17", message, alert_type, operator_type)
                self.read_cursor.execute(self.insert_work_instruction_query, data)
            else:
                data = (timestamp, event_id, "PLC", "MC17", message, alert_type)
                self.read_cursor.execute(self.insert_event_query, data)
            
            self.read_conn.commit()
            logging.info(f"Work instruction sent - Operator: {operator_type}, Message: {message}")
            
        except psycopg2.Error as e:
            logging.error(f"Error sending work instruction to {operator_type}: {e}")
            self.read_conn.rollback()

    def handle_roll_end_detection(self):
        """Handle roll end detection - MQ=1, RQ=0"""
        print("🚨 ROLL END TRIGGERED - Activating tp50 and setting flag=1")
        
        # Set flag to 1
        self.flag = 1
        
        # Work instruction for Cockpit Operator
        self.send_work_instruction(
            "Laminate roll about to end - Prepare new roll and coordinate with field operator",
            "work_instruction",
            "cockpit_operator"
        )
        
        # Work instruction for Field Operator
        self.send_work_instruction(
            "Laminate roll about to end - Prepare for roll changeover process",
            "work_instruction", 
            "field_operator"
        )
        
        # Activate tp50 (Roll End Alert)
        event_data = self.format_event_data(is_problem=True)
        self.insert_or_update_tp("tp50", event_data)
        
        self.write_conn.commit()
        logging.info("Roll end detected - tp50 activated, flag set to 1")

    def handle_new_roll_installation(self):
        """Handle new roll installation - MQ≠1, RQ=1 for 1 minute"""
        print("🎉 NEW ROLL TRIGGERED - Activating tp72 and setting flag=0")
        
        # Set flag back to 0
        self.flag = 0
        
        # Work instruction ONLY for Cockpit Operator (GSM update)
        self.send_work_instruction(
            "New laminate roll installed - Please update GSM (Grams per Square Meter) value",
            "work_instruction",
            "cockpit_operator"
        )
        
        # Activate tp72 (New Roll Added)
        event_data = self.format_event_data(is_problem=True)
        self.insert_or_update_tp("tp72", event_data)
        
        # Deactivate tp50 (Roll End Resolved)
        tp50_event_data = self.format_event_data(is_problem=False)
        self.insert_or_update_tp("tp50", tp50_event_data)
        
        self.write_conn.commit()
        
        # Reset timer
        self.new_roll_timer_start = None
        
        logging.info("New roll installation processed - tp72 activated, tp50 deactivated, flag reset to 0")

    def run(self):
        """Main execution loop"""
        print("📊 Monitoring started with updated flag logic")
        print("Logic: MQ=1,RQ=1 → No Alert | MQ=1,RQ=0 → Roll End | MQ≠1,RQ=1(1min) → New Roll")
        print("=" * 70)
        
        logging.info("Starting MC17 Roll End Sensor monitoring system")
        iteration_count = 0

        while True:
            try:
                iteration_count += 1
                
                # Fetch machine status (MQ) from read database
                self.read_cursor.execute(self.fetch_mq_query)
                mq_row = self.read_cursor.fetchone()
                mq = int(mq_row[1]) if mq_row and mq_row[1] is not None else None

                # Fetch roll end sensor data (RQ) from read database
                self.read_cursor.execute(self.fetch_rq_query)
                rq_row = self.read_cursor.fetchone()
                rq = None
                if rq_row and rq_row[1] is not None:
                    rq = 1 if rq_row[1].lower() == 'true' else 0

                # Print status every iteration
                timer_status = ""
                if self.new_roll_timer_start:
                    elapsed = (datetime.now() - self.new_roll_timer_start).total_seconds()
                    timer_status = f" | Timer: {elapsed:.1f}s"
                
                print(f"[{iteration_count:04d}] MQ={mq} RQ={rq} | Flag={self.flag}{timer_status}")

                # Skip processing if essential data is missing
                if mq is None or rq is None:
                    print("⚠️  MQ or RQ is None, skipping")
                    time.sleep(1)
                    continue

                # RULE 1: MQ=1 and RQ=1 → No Alert (Normal operation)
                if mq == 1 and rq == 1:
                    # Reset new roll timer if it was running
                    if self.new_roll_timer_start is not None:
                        self.new_roll_timer_start = None
                        print("   ℹ️  New roll timer reset (back to normal operation)")

                # RULE 2: MQ=1 and RQ=0 → Roll End Detection
                elif mq == 1 and rq == 0:
                    if self.flag == 0:  # Only trigger if flag is 0
                        self.handle_roll_end_detection()
                    
                    # Reset new roll timer if it was running
                    if self.new_roll_timer_start is not None:
                        self.new_roll_timer_start = None

                # RULE 3: MQ≠1 and RQ=1 → New Roll Detection (after 1 minute)
                elif mq != 1 and rq == 1:
                    if self.new_roll_timer_start is None:
                        self.new_roll_timer_start = datetime.now()
                        print(f"   ⏰ Started 1-minute timer for new roll detection (MQ={mq}≠1, RQ=1)")
                    else:
                        elapsed_time = datetime.now() - self.new_roll_timer_start
                        if elapsed_time >= timedelta(minutes=1):
                            if self.flag == 1:  # Only if roll end was detected (flag=1)
                                self.handle_new_roll_installation()
                            else:
                                print("   ⚠️  1 minute elapsed but flag=0, ignoring new roll")
                                self.new_roll_timer_start = None

                # OTHER CONDITIONS: Reset timer
                else:
                    if self.new_roll_timer_start is not None:
                        self.new_roll_timer_start = None
                        print(f"   🔄 Condition changed (MQ={mq}, RQ={rq}), timer reset")

                time.sleep(1)

            except psycopg2.Error as e:
                print(f"❌ Database error: {e}")
                logging.error(f"Database error in main loop: {e}")
                try:
                    self.read_conn.rollback()
                    self.write_conn.rollback()
                except:
                    pass
                time.sleep(1)
                
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                logging.error(f"Unexpected error in main loop: {e}")
                time.sleep(1)

    def __del__(self):
        """Cleanup database connections"""
        try:
            if hasattr(self, 'read_cursor') and self.read_cursor:
                self.read_cursor.close()
            if hasattr(self, 'read_conn') and self.read_conn:
                self.read_conn.close()
            if hasattr(self, 'write_cursor') and self.write_cursor:
                self.write_cursor.close()
            if hasattr(self, 'write_conn') and self.write_conn:
                self.write_conn.close()
            print("✅ Database connections closed")
            logging.info("Database connections closed successfully")
        except Exception as e:
            logging.error(f"Error closing database connections: {e}")

if __name__ == '__main__':
    try:
        sensor_monitor = RollEndSensor_TP("mc17_rollend_config.json")
        sensor_monitor.run()
    except KeyboardInterrupt:
        print("\n⛔ System shutdown requested")
        logging.info("System shutdown requested by user")
    except Exception as e:
        print(f"💥 Critical error: {e}")
        logging.error(f"Critical error in main execution: {e}")
        raise

