import psycopg2
from datetime import datetime
import time
import json

class SetpointUpdater:
    def __init__(self, db_config, mappings):
        self.db_config = db_config
        self.mappings = mappings
        self.conn = None
        self.cursor = None

    def connect_db(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def close_db(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def update_setpoints(self):
        """Fetch data and update setpoints in the database."""
        try:
            for machine, keys in self.mappings.items():
                # Fetch the corresponding JSON data from loop3_checkpoints for the machine
                self.cursor.execute(f"SELECT {machine} FROM loop3_checkpoints")
                loop3_data = self.cursor.fetchone()

                if loop3_data:
                    loop3_json = json.loads(loop3_data[0]) if isinstance(loop3_data[0], str) else loop3_data[0]

                    if isinstance(loop3_json, dict):
                        for cls_key, loop_key in keys.items():
                            # Retrieve SetValue from loop3_checkpoints JSON data
                            set_value = loop3_json.get(loop_key, {}).get("SetValue")

                            if set_value is not None:
                                # Update cls1_setpoints with the new setpoint value
                                self.cursor.execute(f"""
                                    UPDATE cls1_setpoints
                                    SET {machine} = jsonb_set({machine}, '{{{cls_key}, setpoints}}', %s::jsonb, true)
                                    WHERE {machine} IS NOT NULL;
                                """, [json.dumps(set_value)])

            # Commit the changes to the database
            self.conn.commit()
            print(f"Setpoints updated at {datetime.now()}")

        except Exception as e:
            print(f"Error during update: {e}")
            self.conn.rollback()

    def run(self, interval=30):
        """Run the updater periodically."""
        try:
            self.connect_db()
            while True:
                self.update_setpoints()
                time.sleep(interval)
        finally:
            self.close_db()

# Database configuration
db_config = {
    "host": "192.168.4.11",
    "database": "hul",
    "user": "postgres",  
    "password": "ai4m2024"  
}

# Common mapping for machines
common_mapping = {
    "hor_sealer_rear": "HMI_Ver_Seal_Rear_28",
    "hor_sealer_front": "HMI_Ver_Seal_Rear_27",
    "ver_sealer_rear1": "HMI_Ver_Seal_Rear_14",
    "ver_sealer_rear2": "HMI_Ver_Seal_Rear_15",
    "ver_sealer_rear3": "HMI_Ver_Seal_Rear_16",
    "ver_sealer_rear4": "HMI_Ver_Seal_Rear_17",
    "ver_sealer_rear5": "HMI_Ver_Seal_Rear_18",
    "ver_sealer_rear6": "HMI_Ver_Seal_Rear_19",
    "ver_sealer_rear7": "HMI_Ver_Seal_Rear_20",
    "ver_sealer_rear8": "HMI_Ver_Seal_Rear_21",
    "ver_sealer_rear9": "HMI_Ver_Seal_Rear_22",
    "ver_sealer_rear10": "HMI_Ver_Seal_Rear_23",
    "ver_sealer_rear11": "HMI_Ver_Seal_Rear_24",
    "ver_sealer_rear12": "HMI_Ver_Seal_Rear_25",
    "ver_sealer_rear13": "HMI_Ver_Seal_Rear_26",
    "ver_sealer_front1": "HMI_Ver_Seal_Front_1",
    "ver_sealer_front2": "HMI_Ver_Seal_Front_2",
    "ver_sealer_front3": "HMI_Ver_Seal_Front_3",
    "ver_sealer_front4": "HMI_Ver_Seal_Front_4",
    "ver_sealer_front5": "HMI_Ver_Seal_Front_5",
    "ver_sealer_front6": "HMI_Ver_Seal_Front_6",
    "ver_sealer_front7": "HMI_Ver_Seal_Front_7",
    "ver_sealer_front8": "HMI_Ver_Seal_Front_8",
    "ver_sealer_front9": "HMI_Ver_Seal_Front_9",
    "ver_sealer_front10": "HMI_Ver_Seal_Front_10",
    "ver_sealer_front11": "HMI_Ver_Seal_Front_11",
    "ver_sealer_front12": "HMI_Ver_Seal_Front_12",
    "ver_sealer_front13": "HMI_Ver_Seal_Front_13",
}

# Machine-specific mappings
mappings = {
    "mc17": common_mapping,
    "mc18": {k: v.replace("28", "36").replace("27", "35") for k, v in common_mapping.items()},
    "mc19": common_mapping,
    "mc20": common_mapping,
    "mc21": common_mapping,
    "mc22": common_mapping
}

# Instantiate and run the updater
if __name__ == "__main__":
    updater = SetpointUpdater(db_config, mappings)
    updater.run()
