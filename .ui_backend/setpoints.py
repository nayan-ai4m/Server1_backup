import psycopg2
import json
import logging
import time
from typing import Dict, Any
from datetime import datetime
import os
import sys
import traceback

# ==================== DEDICATED DATE-WISE ERROR LOGGER ====================
log_dir = "/home/ai4m/develop/ui_backend/logs"
date_str = datetime.now().strftime("%Y-%m-%d")
date_path = os.path.join(log_dir, date_str)
os.makedirs(date_path, exist_ok=True)  # Auto-create date folder

error_log_file = os.path.join(date_path, "setpoints.log")

error_logger = logging.getLogger("SetpointErrorLogger")
error_logger.setLevel(logging.ERROR)
error_logger.propagate = False  # Prevent duplicate logging

error_handler = logging.FileHandler(error_log_file)
error_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
)
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)

# Optional: Also keep console + old file for INFO (your original behavior)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("setpoints.log"),  # Keeps old behavior
        logging.StreamHandler(),
    ],
)


# Redirect all logging.error() → date-wise file + traceback
def log_error_with_trace(msg, *args, **kwargs):
    error_logger.error(msg, *args, **kwargs)
    error_logger.error(traceback.format_exc())


# =========================================================================

try:
    with open("setpoints_config.json") as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    log_error_with_trace("Error: setpoints_config.json not found")
    sys.exit(1)
except json.JSONDecodeError as e:
    log_error_with_trace(f"Error: Invalid JSON in setpoints_config.json: {e}")
    sys.exit(1)


class SetpointMonitor:
    def __init__(self):
        try:
            db_config = CONFIG["database"]
            self.read_db_config = {
                "dbname": db_config["dbname"],
                "user": db_config["user"],
                "password": db_config["password"],
                "host": db_config["host"],
                "port": db_config["port"],
                "connect_timeout": db_config.get("connect_timeout", 5),
            }
            self.write_db_config = self.read_db_config

            # Loop 3 configuration (existing)
            setpoints_config = CONFIG["setpoints"]
            self.machines = setpoints_config["machines"]
            self.ver_seal_front_tags = setpoints_config["ver_seal_front_tags"]
            self.ver_seal_rear_tags = setpoints_config["ver_seal_rear_tags"]
            self.default_keys = setpoints_config["default_keys"]
            self.mc17_special_tags = setpoints_config["mc17_special_tags"]
            self.mc18_special_tags = setpoints_config["mc18_special_tags"]
            self.mc19_22_special_tags = setpoints_config["mc19_22_special_tags"]

            # Loop 4 configuration (new)
            loop4_config = CONFIG.get("setpoints_loop4", {})
            self.loop4_machines = loop4_config.get("machines", [])
            self.loop4_source_tables = loop4_config.get("source_tables", {})
            self.loop4_ver_seal_front_tags = loop4_config.get("ver_seal_front_tags", [])
            self.loop4_ver_seal_rear_tags = loop4_config.get("ver_seal_rear_tags", [])
            self.loop4_column_mapping = loop4_config.get("column_mapping", {})

            # All machines combined
            self.all_machines = self.machines + self.loop4_machines
        except KeyError as e:
            log_error_with_trace(f"Missing configuration key: {e}")
            sys.exit(1)

        self.retry_count = 0
        self.max_retries = 5
        self.retry_delay = 10
        self.read_conn = None
        self.write_conn = None

        self.initialize_connections()

    def initialize_connections(self):
        try:
            self.read_conn = psycopg2.connect(**self.read_db_config)
            self.write_conn = psycopg2.connect(**self.write_db_config)
            print("Database connections established")
            self.retry_count = 0
        except psycopg2.Error as e:
            log_error_with_trace(f"Database connection failed: {e}")
            self.handle_critical_error()

    def handle_critical_error(self):
        self.retry_count += 1
        if self.retry_count >= self.max_retries:
            log_error_with_trace("Max retries exceeded. Exiting...")
            self.cleanup()
            sys.exit(1)

        logging.warning(
            f"Retrying in {self.retry_delay} seconds (attempt {self.retry_count}/{self.max_retries})"
        )
        time.sleep(self.retry_delay)
        self.initialize_connections()

    def check_connection(self, conn):
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except psycopg2.InterfaceError:
            return False

    def reconnect(self, conn_type):
        try:
            if conn_type == "read":
                if self.read_conn and not self.read_conn.closed:
                    self.read_conn.close()
                self.read_conn = psycopg2.connect(**self.read_db_config)
                return self.read_conn
            else:
                if self.write_conn and not self.write_conn.closed:
                    self.write_conn.close()
                self.write_conn = psycopg2.connect(**self.write_db_config)
                return self.write_conn
        except psycopg2.Error as e:
            log_error_with_trace(f"Reconnection failed: {e}")
            return None

    def cleanup(self):
        try:
            if self.read_conn and not self.read_conn.closed:
                self.read_conn.close()
            if self.write_conn and not self.write_conn.closed:
                self.write_conn.close()
            print("Resources cleaned up")
        except Exception as e:
            log_error_with_trace(f"Cleanup error: {e}")

    def close(self):
        try:
            if self.read_conn and not self.read_conn.closed:
                self.read_conn.close()
            if self.write_conn and not self.write_conn.closed:
                self.write_conn.close()
            logging.info("Closed database connections")
        except Exception as e:
            log_error_with_trace(f"Error closing connections: {e}")
            os._exit(1)

    def format_timestamp(self, dt):
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f%z")

    def round_value(self, value):
        if isinstance(value, (int, float)):
            return round(value, 1)
        return value

    def process_machine_data(
        self,
        raw_data: Any,
        machine: str,
        current_timestamp: str,
        previous_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        try:
            if not raw_data:
                return {
                    "data": {},
                    "TempDisplay": {},
                    "Bandwidth": {},
                    "last_update": {},
                    "Avg": {},
                }

            data = {}
            temp_display = {}
            bandwidth = {}
            last_update = previous_data.get("last_update", {}).copy()

            if isinstance(raw_data, dict):
                tag_mapping = {}
                if machine == "mc17":
                    tag_mapping = self.mc17_special_tags
                elif machine == "mc18":
                    tag_mapping = self.mc18_special_tags
                elif machine in ["mc19", "mc20", "mc21", "mc22"]:
                    tag_mapping = self.mc19_22_special_tags

                for key in self.default_keys:
                    special_tag = [k for k, v in tag_mapping.items() if v == key]
                    if special_tag and special_tag[0] in raw_data:
                        value = raw_data[special_tag[0]]
                        logging.debug(
                            f"Using special tag {special_tag[0]} for {machine}'s {key}"
                        )
                    else:
                        value = raw_data.get(key, 0)

                    if isinstance(value, (bool, int, float, str)):
                        try:
                            processed_value = (
                                float(value)
                                if str(value).replace(".", "", 1).isdigit()
                                else value
                            )
                            processed_value = (
                                self.round_value(processed_value)
                                if isinstance(processed_value, (int, float))
                                else processed_value
                            )
                        except (ValueError, TypeError):
                            processed_value = value

                        previous_value = previous_data.get("data", {}).get(key)
                        if previous_value is not None:
                            try:
                                previous_value = (
                                    float(previous_value)
                                    if str(previous_value).replace(".", "", 1).isdigit()
                                    else previous_value
                                )
                                previous_value = (
                                    self.round_value(previous_value)
                                    if isinstance(previous_value, (int, float))
                                    else previous_value
                                )
                            except (ValueError, TypeError):
                                previous_value = value

                        if previous_value != processed_value:
                            last_update[key] = current_timestamp
                            logging.debug(
                                f"Value changed for {key}: {previous_value} -> {processed_value}"
                            )

                        data[key] = processed_value
                        temp_display[key] = (
                            self.round_value(float(value))
                            if isinstance(value, (int, float))
                            else 0.0
                        )
                        bandwidth[key] = 2

                    elif isinstance(value, dict):
                        rounded_dict = {
                            k: self.round_value(v) if isinstance(v, (int, float)) else v
                            for k, v in value.items()
                        }
                        set_value = self.round_value(rounded_dict.get("SetValue", 0))
                        temp_value = self.round_value(
                            rounded_dict.get("TempDisplay", 0.0)
                        )

                        previous_value = previous_data.get("data", {}).get(key)
                        if previous_value is not None:
                            try:
                                previous_value = (
                                    float(previous_value)
                                    if str(previous_value).replace(".", "", 1).isdigit()
                                    else previous_value
                                )
                                previous_value = (
                                    self.round_value(previous_value)
                                    if isinstance(previous_value, (int, float))
                                    else previous_value
                                )
                            except (ValueError, TypeError):
                                previous_value = set_value

                        if previous_value != set_value:
                            last_update[key] = current_timestamp
                            logging.debug(
                                f"Value changed for {key}: {previous_value} -> {set_value}"
                            )

                        data[key] = set_value
                        temp_display[key] = temp_value
                        bandwidth[key] = 2

                    else:
                        previous_value = previous_data.get("data", {}).get(key)
                        if previous_value != 0:
                            last_update[key] = current_timestamp

                        data[key] = 0
                        temp_display[key] = 0.0
                        bandwidth[key] = 2

            avg_values = {
                "Avg_Ver_Seal_Front_Temps": self.calculate_average(
                    temp_display, self.ver_seal_front_tags
                ),
                "Avg_Ver_Seal_Rear_Temps": self.calculate_average(
                    temp_display, self.ver_seal_rear_tags
                ),
                "Avg_Ver_Seal_Front_SetValues": self.calculate_average_setvalue(
                    data, self.ver_seal_front_tags
                ),
                "Avg_Ver_Seal_Rear_SetValues": self.calculate_average_setvalue(
                    data, self.ver_seal_rear_tags
                ),
                "Hor_Seal_Front_Temp": temp_display.get("HMI_Hor_Seal_Front_27", 0.0),
                "Hor_Seal_Rear_Temp": temp_display.get("HMI_Hor_Seal_Rear_28", 0.0),
                "Hor_Seal_Front_SetValue": data.get("HMI_Hor_Seal_Front_27", 0.0),
                "Hor_Seal_Rear_SetValue": data.get("HMI_Hor_Seal_Rear_28", 0.0),
            }

            return {
                "data": data,
                "TempDisplay": temp_display,
                "Bandwidth": bandwidth,
                "last_update": last_update,
                "Avg": avg_values,
            }

        except Exception as e:
            log_error_with_trace(f"Data processing error for {machine}: {e}")
            return {
                "data": {},
                "TempDisplay": {},
                "Bandwidth": {},
                "last_update": {},
                "Avg": {},
            }

    def calculate_average(self, data_dict: Dict[str, float], tags: list) -> float:
        values = [
            data_dict.get(tag, 0.0)
            for tag in tags
            if isinstance(data_dict.get(tag), (int, float))
        ]
        return self.round_value(sum(values) / len(values)) if values else 0.0

    def calculate_average_setvalue(
        self, data_dict: Dict[str, float], tags: list
    ) -> float:
        values = [
            data_dict.get(tag, 0.0)
            for tag in tags
            if isinstance(data_dict.get(tag), (int, float))
        ]
        return self.round_value(sum(values) / len(values)) if values else 0.0

    def fetch_latest_setpoints(self) -> Dict[str, Any]:
        try:
            if not self.check_connection(self.read_conn):
                self.read_conn = self.reconnect("read")
                if not self.read_conn:
                    raise psycopg2.OperationalError("Read connection unavailable")

            current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f%z")

            previous_data = {}
            if self.write_conn and self.check_connection(self.write_conn):
                try:
                    with self.write_conn.cursor() as write_cursor:
                        write_cursor.execute(
                            "SELECT mc17, mc18, mc19, mc20, mc21, mc22 FROM cls1_setpoints LIMIT 1"
                        )
                        previous_setpoints = write_cursor.fetchone()

                        if previous_setpoints:
                            for i, machine in enumerate(self.machines):
                                if previous_setpoints[i] is None:
                                    previous_data[machine] = {}
                                elif isinstance(previous_setpoints[i], dict):
                                    previous_data[machine] = previous_setpoints[i]
                                else:
                                    try:
                                        if isinstance(previous_setpoints[i], str):
                                            parsed_data = json.loads(
                                                previous_setpoints[i]
                                            )
                                        else:
                                            parsed_data = previous_setpoints[i]

                                        previous_data[machine] = parsed_data
                                        logging.debug(
                                            f"Successfully loaded previous data for {machine}"
                                        )
                                    except (json.JSONDecodeError, TypeError) as e:
                                        log_error_with_trace(
                                            f"Error parsing previous data for {machine}: {e}"
                                        )
                                        previous_data[machine] = {}
                                        os._exit(1)
                except Exception as e:
                    log_error_with_trace(f"Error reading previous data: {e}")
                    os._exit(1)

            query = """
            SELECT
                (SELECT mc17 FROM loop3_checkpoints WHERE mc17 IS NOT NULL AND mc17 != 'null' ORDER BY timestamp DESC LIMIT 1) AS mc17,
                (SELECT mc18 FROM loop3_checkpoints WHERE mc18 IS NOT NULL AND mc18 != 'null' ORDER BY timestamp DESC LIMIT 1) AS mc18,
                (SELECT mc19 FROM loop3_checkpoints WHERE mc19 IS NOT NULL AND mc19 != 'null' ORDER BY timestamp DESC LIMIT 1) AS mc19,
                (SELECT mc20 FROM loop3_checkpoints WHERE mc20 IS NOT NULL AND mc20 != 'null' ORDER BY timestamp DESC LIMIT 1) AS mc20,
                (SELECT mc21 FROM loop3_checkpoints WHERE mc21 IS NOT NULL AND mc21 != 'null' ORDER BY timestamp DESC LIMIT 1) AS mc21,
                (SELECT mc22 FROM loop3_checkpoints WHERE mc22 IS NOT NULL AND mc22 != 'null' ORDER BY timestamp DESC LIMIT 1) AS mc22,
                (SELECT MAX(timestamp) FROM loop3_checkpoints WHERE (mc17 IS NOT NULL AND mc17 != 'null' OR mc18 IS NOT NULL AND mc18 != 'null' OR mc19 IS NOT NULL AND mc19 != 'null' OR mc20 IS NOT NULL AND mc20 != 'null' OR mc21 IS NOT NULL AND mc21 != 'null' OR mc22 IS NOT NULL AND mc22 != 'null')) AS timestamp
            """

            with self.read_conn.cursor() as cursor:
                cursor.execute(query)
                record = cursor.fetchone()

            if not record:
                logging.info("No data available in loop3_checkpoints")
                return {}

            result = {}
            for i, machine in enumerate(self.machines):
                if i < len(record):
                    raw_data = record[i]
                    try:
                        if raw_data and isinstance(raw_data, str):
                            raw_data = json.loads(raw_data)
                        result[machine] = self.process_machine_data(
                            raw_data,
                            machine,
                            current_timestamp,
                            previous_data.get(machine, {}),
                        )
                    except Exception as e:
                        log_error_with_trace(f"Error processing {machine} data: {e}")
                        result[machine] = {}
                        os._exit(1)
            return result

        except psycopg2.OperationalError as e:
            log_error_with_trace(f"Database operational error: {e}")
            self.handle_critical_error()
            return {}
        except Exception as e:
            log_error_with_trace(f"Unexpected fetch error: {e}")
            return {}

    def fetch_loop4_setpoints(self) -> Dict[str, Any]:
        """Fetch Loop 4 setpoints from individual machine tables (mc25_mid, mc26_mid, etc.)"""
        try:
            if not self.check_connection(self.read_conn):
                self.read_conn = self.reconnect("read")
                if not self.read_conn:
                    raise psycopg2.OperationalError("Read connection unavailable")

            current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f%z")

            # Get previous data from cls1_setpoints table
            previous_data = {}
            if self.write_conn and self.check_connection(self.write_conn):
                try:
                    with self.write_conn.cursor() as write_cursor:
                        write_cursor.execute(
                            "SELECT mc25, mc26, mc27, mc28, mc29, mc30 FROM cls1_setpoints LIMIT 1"
                        )
                        previous_setpoints = write_cursor.fetchone()

                        if previous_setpoints:
                            for i, machine in enumerate(self.loop4_machines):
                                if previous_setpoints[i] is None:
                                    previous_data[machine] = {}
                                elif isinstance(previous_setpoints[i], dict):
                                    previous_data[machine] = previous_setpoints[i]
                                else:
                                    try:
                                        if isinstance(previous_setpoints[i], str):
                                            parsed_data = json.loads(previous_setpoints[i])
                                        else:
                                            parsed_data = previous_setpoints[i]
                                        previous_data[machine] = parsed_data
                                    except (json.JSONDecodeError, TypeError) as e:
                                        log_error_with_trace(f"Error parsing previous Loop 4 data for {machine}: {e}")
                                        previous_data[machine] = {}
                except Exception as e:
                    log_error_with_trace(f"Error reading previous Loop 4 data: {e}")

            result = {}
            with self.read_conn.cursor() as cursor:
                for machine in self.loop4_machines:
                    table_name = self.loop4_source_tables.get(machine)
                    if not table_name:
                        logging.warning(f"No source table defined for {machine}")
                        result[machine] = {}
                        continue

                    try:
                        # Fetch latest record from machine-specific table
                        cursor.execute(f"SELECT * FROM {table_name} ORDER BY timestamp DESC LIMIT 1")
                        record = cursor.fetchone()

                        if not record:
                            logging.warning(f"No data available for {machine} in {table_name}")
                            result[machine] = {}
                            continue

                        # Get column names
                        column_names = [desc[0] for desc in cursor.description]
                        row_dict = dict(zip(column_names, record))

                        # Build Loop 4 data structure in NATIVE format (vert_front_X_sv)
                        # DO NOT transform to Loop 3 format - keep Loop 4 keys as-is
                        result[machine] = self.build_loop4_structure(
                            row_dict,
                            machine,
                            current_timestamp,
                            previous_data.get(machine, {}),
                        )

                    except Exception as e:
                        log_error_with_trace(f"Error processing Loop 4 {machine} data: {e}")
                        result[machine] = {}

            return result

        except psycopg2.OperationalError as e:
            log_error_with_trace(f"Loop 4 database operational error: {e}")
            self.handle_critical_error()
            return {}
        except Exception as e:
            log_error_with_trace(f"Unexpected Loop 4 fetch error: {e}")
            return {}

    def build_loop4_structure(self, row_dict: Dict[str, Any], machine: str, current_timestamp: str, previous_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build Loop 4 data structure in NATIVE format (vert_front_X_sv, not HMI_Ver_Seal_Front_X)
        This keeps Loop 4 data different from Loop 3 format
        """
        machine_data = {
            "data": {},
            "TempDisplay": {},
            "Avg": {},
            "last_update": {}
        }

        # Get previous last_update or initialize
        prev_last_update = previous_data.get("last_update", {}) if previous_data else {}

        # Temperature Setpoints (SV) - 26 columns with Loop 4 native keys
        for i in range(1, 14):
            # Front temperatures
            front_sv_key = f"vert_front_{i}_sv"
            front_pv_key = f"vert_front_{i}_pv"

            current_sv = row_dict.get(front_sv_key)
            previous_sv = previous_data.get("data", {}).get(front_sv_key) if previous_data else None

            machine_data["data"][front_sv_key] = current_sv
            machine_data["TempDisplay"][f"vert_front_{i}_av"] = row_dict.get(front_pv_key)

            # Update timestamp only if value changed
            if current_sv != previous_sv:
                machine_data["last_update"][front_sv_key] = current_timestamp
            else:
                machine_data["last_update"][front_sv_key] = prev_last_update.get(front_sv_key, current_timestamp)

            # Rear temperatures (mapped as vert_rear_X instead of front_14-26)
            rear_sv_key = f"vert_rear_{i}_sv"
            rear_pv_key = f"vert_rear_{i}_pv"

            current_rear_sv = row_dict.get(rear_sv_key)
            previous_rear_sv = previous_data.get("data", {}).get(rear_sv_key) if previous_data else None

            machine_data["data"][rear_sv_key] = current_rear_sv
            machine_data["TempDisplay"][f"vert_rear_{i}_av"] = row_dict.get(rear_pv_key)

            # Update timestamp only if value changed
            if current_rear_sv != previous_rear_sv:
                machine_data["last_update"][rear_sv_key] = current_timestamp
            else:
                machine_data["last_update"][rear_sv_key] = prev_last_update.get(rear_sv_key, current_timestamp)

        # Horizontal temperatures
        machine_data["data"]["hor_temp_27_front_sv"] = row_dict.get("hor_temp_27_sv_front")
        machine_data["data"]["hor_temp_28_rear_sv"] = row_dict.get("hor_temp_28_sv_rear")
        machine_data["TempDisplay"]["hor_temp_27_front_av"] = row_dict.get("hor_temp_27_pv_front")
        machine_data["TempDisplay"]["hor_temp_28_rear_av"] = row_dict.get("hor_temp_28_pv_rear")

        # Calculate averages
        front_pv_values = [row_dict.get(f"vert_front_{i}_pv") for i in range(1, 14) if row_dict.get(f"vert_front_{i}_pv") is not None]
        rear_pv_values = [row_dict.get(f"vert_rear_{i}_pv") for i in range(1, 14) if row_dict.get(f"vert_rear_{i}_pv") is not None]

        machine_data["Avg"]["front_avg"] = round(sum(front_pv_values) / len(front_pv_values), 2) if front_pv_values else 0
        machine_data["Avg"]["rear_avg"] = round(sum(rear_pv_values) / len(rear_pv_values), 2) if rear_pv_values else 0

        # Stroke data
        machine_data["data"]["hori_sealer_stroke_1"] = row_dict.get("hori_sealer_stroke_1")
        machine_data["data"]["hori_sealer_stroke_2"] = row_dict.get("hori_sealer_stroke_2")
        machine_data["data"]["vert_sealer_stroke_1"] = row_dict.get("vert_sealer_stroke_1")
        machine_data["data"]["vert_sealer_stroke_2"] = row_dict.get("vert_sealer_stroke_2")

        # Machine degree settings
        machine_data["data"]["hori_sealer_fwd_start_deg_hmi"] = row_dict.get("hori_sealer_fwd_start_deg_hmi")
        machine_data["data"]["hori_sealer_fwd_end_deg_hmi"] = row_dict.get("hori_sealer_fwd_end_deg_hmi")
        machine_data["data"]["hori_sealer_rev_start_deg_hmi"] = row_dict.get("hori_sealer_rev_start_deg_hmi")
        machine_data["data"]["hori_sealer_rev_end_deg_hmi"] = row_dict.get("hori_sealer_rev_end_deg_hmi")
        machine_data["data"]["vert_sealer_fwd_start_deg_hmi"] = row_dict.get("vert_sealer_fwd_start_deg_hmi")
        machine_data["data"]["vert_sealer_fwd_end_deg_hmi"] = row_dict.get("vert_sealer_fwd_end_deg_hmi")
        machine_data["data"]["vert_sealer_fwd_end_start_deg_hmi"] = row_dict.get("vert_sealer_fwd_end_start_deg_hmi")
        machine_data["data"]["vert_sealer_rev_start_deg_hmi"] = row_dict.get("vert_sealer_rev_start_deg_hmi")
        machine_data["data"]["vert_sealer_rev_end_deg_hmi"] = row_dict.get("vert_sealer_rev_end_deg_hmi")

        # Rotary valve settings (single hopper: MC27-30)
        machine_data["data"]["rotary_valve_fwd_start_deg_hmi"] = row_dict.get("rotary_valve_fwd_start_deg_hmi")
        machine_data["data"]["rotary_valve_fwd_end_deg_hmi"] = row_dict.get("rotary_valve_fwd_end_deg_hmi")
        machine_data["data"]["rotary_valve_rev_start_deg_hmi"] = row_dict.get("rotary_valve_rev_start_deg_hmi")
        machine_data["data"]["rotary_valve_rev_end_deg_hmi"] = row_dict.get("rotary_valve_rev_end_deg_hmi")
        machine_data["data"]["rotary_valve_stroke_length_1"] = row_dict.get("rotary_valve_stroke_length_1")

        # Filling settings (single hopper)
        machine_data["data"]["filling_fwd_start_deg"] = row_dict.get("filling_fwd_start_deg")
        machine_data["data"]["filling_fwd_end_deg"] = row_dict.get("filling_fwd_end_deg")
        machine_data["data"]["filling_rev_start_deg"] = row_dict.get("filling_rev_start_deg")
        machine_data["data"]["filling_rev_end_deg"] = row_dict.get("filling_rev_end_deg")

        # Valve settings (Left) - dual hopper: MC25, MC26
        machine_data["data"]["valve_fwd_start_deg_hmi_left"] = row_dict.get("valve_fwd_start_deg_hmi_left")
        machine_data["data"]["valve_fwd_end_start_deg_hmi_left"] = row_dict.get("valve_fwd_end_start_deg_hmi_left")
        machine_data["data"]["valve_rev_start_deg_hmi_left"] = row_dict.get("valve_rev_start_deg_hmi_left")
        machine_data["data"]["valve_rev_end_deg_hmi_left"] = row_dict.get("valve_rev_end_deg_hmi_left")

        # Valve settings (Right)
        machine_data["data"]["valve_fwd_start_deg_hmi_right"] = row_dict.get("valve_fwd_start_deg_hmi_right")
        machine_data["data"]["valve_fwd_end_start_deg_hmi_right"] = row_dict.get("valve_fwd_end_start_deg_hmi_right")
        machine_data["data"]["valve_rev_start_deg_hmi_right"] = row_dict.get("valve_rev_start_deg_hmi_right")
        machine_data["data"]["valve_rev_end_deg_hmi_right"] = row_dict.get("valve_rev_end_deg_hmi_right")

        # Filling settings (Left)
        machine_data["data"]["filling_fwd_start_deg_left"] = row_dict.get("filling_fwd_start_deg_left")
        machine_data["data"]["filling_fwd_end_deg_left"] = row_dict.get("filling_fwd_end_deg_left")
        machine_data["data"]["filling_rev_start_deg_left"] = row_dict.get("filling_rev_start_deg_left")
        machine_data["data"]["filling_rev_end_deg_left"] = row_dict.get("filling_rev_end_deg_left")

        # Filling settings (Right)
        machine_data["data"]["filling_fwd_start_deg_right"] = row_dict.get("filling_fwd_start_deg_right")
        machine_data["data"]["filling_fwd_end_deg_right"] = row_dict.get("filling_fwd_end_deg_right")
        machine_data["data"]["filling_rev_start_deg_right"] = row_dict.get("filling_rev_start_deg_right")
        machine_data["data"]["filling_rev_end_deg_right"] = row_dict.get("filling_rev_end_deg_right")

        # Stroke lengths
        machine_data["data"]["rotary_valve_stroke_length_left"] = row_dict.get("rotary_valve_stroke_length_left")
        machine_data["data"]["rotary_valve_stroke_length_right"] = row_dict.get("rotary_valve_stroke_length_right")
        machine_data["data"]["filling_stroke_length_left"] = row_dict.get("filling_stroke_length_left")
        machine_data["data"]["filling_stroke_length_right"] = row_dict.get("filling_stroke_length_right")
        machine_data["data"]["filling_stroke_length"] = row_dict.get("filling_stroke_length")

        # Hopper settings (Left) - dual hopper
        machine_data["data"]["hopper_left_extreme_low_level"] = row_dict.get("hopper_left_extreme_low_level")
        machine_data["data"]["hopper_left_low_level"] = row_dict.get("hopper_left_low_level")
        machine_data["data"]["hopper_left_high_level"] = row_dict.get("hopper_left_high_level")

        # Hopper settings (Right) - dual hopper
        machine_data["data"]["hopper_right_extreme_low_level"] = row_dict.get("hopper_right_extreme_low_level")
        machine_data["data"]["hopper_right_low_level"] = row_dict.get("hopper_right_low_level")
        machine_data["data"]["hopper_right_high_level"] = row_dict.get("hopper_right_high_level")

        # Hopper settings (Single) - single hopper
        machine_data["data"]["hopper_extreme_low_level"] = row_dict.get("hopper_extreme_low_level")
        machine_data["data"]["hopper_low_level"] = row_dict.get("hopper_low_level")
        machine_data["data"]["hopper_high_level"] = row_dict.get("hopper_high_level")

        # Other settings
        machine_data["data"]["machine_speed"] = row_dict.get("machine_speed")
        machine_data["data"]["puller_enable"] = row_dict.get("puller_enable")
        machine_data["data"]["filling_on_off"] = row_dict.get("filling_on_off")

        return machine_data

    def sync_setpoints_data(self) -> bool:
        try:
            # Fetch Loop 3 data (existing)
            setpoints_data = self.fetch_latest_setpoints()
            if not setpoints_data:
                logging.warning("No Loop 3 data to sync")
                return False

            # Fetch Loop 4 data (new)
            loop4_data = self.fetch_loop4_setpoints()
            if not loop4_data:
                logging.warning("No Loop 4 data available")

            # Merge both loops
            all_data = {**setpoints_data, **loop4_data}

            current_timestamp = datetime.now()

            # Debug logging for all machines
            for machine in self.all_machines:
                if machine in all_data:
                    last_update = all_data[machine].get("last_update", {})
                    if last_update:
                        logging.debug(
                            f"{machine} has {len(last_update)} last_update entries"
                        )

            if not self.check_connection(self.write_conn):
                self.write_conn = self.reconnect("write")
                if not self.write_conn:
                    raise psycopg2.OperationalError("Write connection unavailable")

            with self.write_conn.cursor() as cursor:
                # Prepare JSON data for all machines (Loop 3 + Loop 4)
                json_data = []
                for machine in self.all_machines:
                    machine_data = all_data.get(machine, {})
                    json_str = json.dumps(machine_data)
                    json_data.append(json_str)

                    test_parse = json.loads(json_str)
                    if test_parse.get("last_update") != machine_data.get("last_update"):
                        logging.warning(
                            f"JSON serialization issue with {machine} last_update"
                        )

                cursor.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'cls1_setpoints')"
                )
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    logging.info("Creating setpoints table with Loop 3 & 4 columns")
                    cursor.execute(
                        """
                        CREATE TABLE cls1_setpoints (
                            last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            mc17 jsonb, mc18 jsonb, mc19 jsonb, mc20 jsonb, mc21 jsonb, mc22 jsonb,
                            mc25 jsonb, mc26 jsonb, mc27 jsonb, mc28 jsonb, mc29 jsonb, mc30 jsonb
                        )
                    """
                    )
                else:
                    # Check if Loop 4 columns exist, add them if not
                    cursor.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_name = 'cls1_setpoints' AND column_name IN ('mc25', 'mc26', 'mc27', 'mc28', 'mc29', 'mc30')
                    """)
                    existing_loop4_cols = [row[0] for row in cursor.fetchall()]

                    for machine in self.loop4_machines:
                        if machine not in existing_loop4_cols:
                            logging.info(f"Adding column {machine} to cls1_setpoints")
                            cursor.execute(f"ALTER TABLE cls1_setpoints ADD COLUMN IF NOT EXISTS {machine} jsonb")

                cursor.execute("SELECT COUNT(*) FROM cls1_setpoints")
                has_records = cursor.fetchone()[0] > 0
                if not has_records:
                    cursor.execute(
                        """
                        INSERT INTO cls1_setpoints (mc17, mc18, mc19, mc20, mc21, mc22, mc25, mc26, mc27, mc28, mc29, mc30, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                        (*json_data, current_timestamp),
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE cls1_setpoints SET
                            mc17 = %s, mc18 = %s, mc19 = %s, mc20 = %s, mc21 = %s, mc22 = %s,
                            mc25 = %s, mc26 = %s, mc27 = %s, mc28 = %s, mc29 = %s, mc30 = %s,
                            last_updated = %s
                    """,
                        (*json_data, current_timestamp),
                    )

                self.write_conn.commit()
                logging.info("Successfully synced setpoints data")
                return True

        except psycopg2.OperationalError as e:
            log_error_with_trace(f"Sync database error: {e}")
            raise
        except Exception as e:
            log_error_with_trace(f"Sync error: {e}")
            raise

    def monitor(self):
        try:
            while True:
                start_time = time.time()

                try:
                    if not self.sync_setpoints_data():
                        logging.warning("Sync failed, retrying...")
                        time.sleep(5)
                        continue
                except Exception as e:
                    log_error_with_trace(f"Critical sync error, will exit: {e}")
                    raise

                elapsed = time.time() - start_time
                sleep_time = max(1, 1 - elapsed)
                logging.info(f"Cycle completed in {elapsed:.2f}s")
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            logging.info("Graceful shutdown")
        except Exception as e:
            log_error_with_trace(f"Monitor crash: {e}")
        finally:
            self.cleanup()
            sys.exit(1 if self.retry_count >= self.max_retries else 0)


if __name__ == "__main__":
    monitor = SetpointMonitor()
    monitor.monitor()
