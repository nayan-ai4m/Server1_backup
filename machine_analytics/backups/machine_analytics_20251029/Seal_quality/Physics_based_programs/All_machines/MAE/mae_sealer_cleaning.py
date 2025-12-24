import psycopg2
import logging
import json
from collections import deque
from datetime import datetime
import os
import traceback
import time
import asyncio
from typing import Dict, Any, Optional
import nats
from dataclasses import asdict, dataclass

# ===== Create daily log folder =====
today_str = datetime.now().strftime("%Y-%m-%d")
log_dir = os.path.join("logs", today_str)
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, "machine_spread.log")

# ===== Logging setup =====
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ===== Load Config =====
try:
    with open("config.json", "r") as f:
        CONFIG = json.load(f)
except Exception as e:
    logging.exception("Failed to load config.json")
    raise

DB_CONFIG_HUL = CONFIG["databases"]["hul"]
DB_CONFIG_SHORT = CONFIG["databases"]["short_data_hul"]
PLC_TAGS = {
    "17": {"front": "HMI_Hor_Seal_Front_27", "rear": "HMI_Hor_Seal_Rear_28"},
    "19": {"front": "HMI_Hor_Seal_Front_27", "rear": "HMI_Hor_Seal_Rear_28"},
    "20": {"front": "HMI_Hor_Seal_Front_27", "rear": "HMI_Hor_Seal_Rear_28"},
    "21": {"front": "HMI_Hor_Seal_Front_27", "rear": "HMI_Hor_Seal_Rear_28"},
    "22": {"front": "HMI_Hor_Seal_Front_27", "rear": "HMI_Hor_Seal_Rear_28"},
    "18": {"rear1": "HMI_Hor_Seal_Rear_35", "rear2": "HMI_Hor_Seal_Rear_3"},
}

nats_client: Optional[nats.NATS] = None
# ================= Payloads =================
@dataclass
class Payload:
    plc: str
    name: str
    command: str
    value: float
    status: bool

@dataclass
class Response:
    plc: str
    ack:bool
    message: str



async def get_nats_connection():
    global nats_client
    if nats_client is None or not nats_client.is_connected:
        nats_client = await nats.connect(
            "nats://192.168.1.149:4222",
            connect_timeout=5,
            max_reconnect_attempts=3,
            reconnect_time_wait=1
        )
    return nats_client


async def send_nats_request(plc: str, command: str, name: str, value: float, status: bool) -> Dict[str, Any]:
    try:
        nc = await get_nats_connection()

        if plc in ["17", "18"]:
            topic = "adv.217"
        elif plc in ["19", "20"]:
            topic = "adv.160"
        elif plc in ["21", "22"]:
            topic = "adv.150"
        elif plc in ["25", "26"]:
            topic = "adv.154"
        elif plc in ["27", "30"]:
            topic = "adv.153"
        else:
            topic = f"plc.{plc}"

        payload = Payload(
            plc=plc,
            name=name,
            command=command,
            value=value,
            status=status
        )

        response = await nc.request(
            topic,
            json.dumps(asdict(payload)).encode(),
            timeout=5.0
        )

        payload_response = Response(**json.loads(response.data.decode()))
        logging.info(f"payload_response:{payload_response}")
        return asdict(payload_response)

    except nats.errors.TimeoutError:
        raise HTTPException(status_code=504, detail="NATS request timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NATS communication error: {str(e)}")

class SpreadMonitor:
    def __init__(self, machine_id, config):
        self.machine_id = machine_id
        self.table_status = f"mc{machine_id}"
        self.table_autoencoder = f"mc{machine_id}_autoencoder"

        # Parameters from config
        self.spread_queue = deque(maxlen=config["queue_size"])
        self.threshold = config["threshold"]
        self.temp_step = config["temp_step"]
        self.upper_limit = config.get("upper_limit", 165)   # fallback 165
        self.lower_limit = config.get("lower_limit", 145)     # fallback 0
        self.last_spread = None
        self.front_set_temp = None
        self.rear_set_temp = None
        self.last_front_temp = self.front_set_temp
        self.last_rear_temp = self.rear_set_temp 
        
    
    def check_spread_behavior(self):
        try:
            if len(self.spread_queue) < 10:
                return None  # not enough data yet

            last_10 = list(self.spread_queue)[-10:]

            increasing = all(last_10[i] <= last_10[i + 1] for i in range(len(last_10) - 1))
            decreasing = all(last_10[i] >= last_10[i + 1] for i in range(len(last_10) - 1))
            self.last_action = None  
            # Spread consistently increasing
            if increasing and (last_10[-1] - last_10[0]) > self.threshold:
                proposed_front = (self.front_set_temp or 0) + self.temp_step
                proposed_rear = (self.rear_set_temp or 0) + self.temp_step

                if proposed_front > self.upper_limit or proposed_rear > self.upper_limit:
                    logging.warning(
                        f"[MC{self.machine_id}] Temp increase blocked. "
                        f"Proposed FRONT={proposed_front}°C, REAR={proposed_rear}°C "
                        f"(limit={self.upper_limit}°C). Keeping FRONT={self.front_set_temp}, "
                        f"REAR={self.rear_set_temp}."
                    )
                else:
                    self.front_set_temp = proposed_front
                    self.rear_set_temp = proposed_rear
                    logging.info(
                        f"[MC{self.machine_id}] Spread increasing, threshold crossed. "
                        f"Updated FRONT={self.front_set_temp}°C, REAR={self.rear_set_temp}°C."
                    )
                    self.last_action = "increase" 
                    # asyncio.create_task(self.write_to_plc(self.front_set_temp, self.rear_set_temp))

            # Spread consistently decreasing
            elif decreasing and (last_10[0] - last_10[-1]) > self.threshold:
                proposed_front = (self.last_front_temp or 0) 
                proposed_rear = (self.last_rear_temp or 0) 

                if proposed_front < self.lower_limit or proposed_rear < self.lower_limit:
                    logging.warning(
                        f"[MC{self.machine_id}] Temp decrease blocked. "
                        f"Proposed FRONT={proposed_front}°C, REAR={proposed_rear}°C "
                        f"(limit={self.lower_limit}°C). Keeping FRONT={self.front_set_temp}, "
                        f"REAR={self.rear_set_temp}."
                    )
                else:
                    self.front_set_temp = proposed_front
                    self.rear_set_temp = proposed_rear
                    logging.info(
                        f"[MC{self.machine_id}] Spread decreasing, threshold crossed. "
                        f"Updated FRONT={self.front_set_temp}°C, REAR={self.rear_set_temp}°C."
                    )
                    self.last_action = "decrease"
                    # asyncio.create_task(self.write_to_plc(self.front_set_temp, self.rear_set_temp))

            else:
                logging.info(
                    f"[MC{self.machine_id}] Spread stable/no major trend, no temp change. "
                    f"FRONT={self.front_set_temp}°C, REAR={self.rear_set_temp}°C."
                )
            if self.last_action == "increase" and not decreasing:
                logging.warning(f"[MC{self.machine_id}] Temp increase applied but spread not reducing yet.")

            if self.last_action == "decrease" and not increasing:
                logging.warning(f"[MC{self.machine_id}] Temp decrease applied but spread not increasing yet.")

        except Exception:
            logging.exception(f"[MC{self.machine_id}] Error checking spread behavior")


    async def write_to_plc(self, new_front_temp, new_rear_temp):            
        try:
            logging.info(
                f"[MC{self.machine_id}] Writing new temps to PLC (commented): "
                f"FRONT={new_front_temp}°C, REAR={new_rear_temp}°C"
            )
            plc_id = str(self.machine_id)
            if plc_id not in PLC_TAGS:
                logging.warning(f"[MC{plc_id}] No PLC tags configured.")
                return

            tags = PLC_TAGS[plc_id]

            # For machines with front + rear
            if "front" in tags and front_temp is not None:
                await send_nats_request(
                    plc=plc_id,
                    command="update",
                    name=tags["front"],
                    value=front_temp,
                    status=True
                )
                logging.info(f"[MC{plc_id}] Sent FRONT temp {front_temp}°C to {tags['front']}")

            if "rear" in tags and rear_temp is not None:
                await send_nats_request(
                    plc=plc_id,
                    command="update",
                    name=tags["rear"],
                    value=rear_temp,
                    status=True
                )
                logging.info(f"[MC{plc_id}] Sent REAR temp {rear_temp}°C to {tags['rear']}")

            # For MC18 rear1 + rear2
            if "rear1" in tags and "rear2" in tags and rear_temp is not None:
                for tag in [tags["rear1"], tags["rear2"]]:
                    await send_nats_request(
                        plc=plc_id,
                        command="update",
                        name=tag,
                        value=rear_temp,
                        status=True
                    )
                    logging.info(f"[MC{plc_id}] Sent REAR temp {rear_temp}°C to {tag}")
            
        except Exception:
            logging.exception(f"[MC{self.machine_id}] Error writing to PLC")
    
    def fetch_machine_status(self):
        query = f"SELECT timestamp, status FROM {self.table_status} ORDER BY timestamp DESC LIMIT 1;"
        try:
            with psycopg2.connect(**DB_CONFIG_HUL) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()
            if row:
                return {"timestamp": row[0], "status": row[1]}
        except Exception:
            logging.exception(f"[MC{self.machine_id}] Error fetching machine status")
        return None

    def fetch_latest_spread(self):
        autoencoder_table = f"mc{self.machine_id}_autoencoder"
        mid_table = f"mc{self.machine_id}_mid"

        spread_row = None
        temp_row = None

        try:
            # 1️⃣ Fetch latest spread from autoencoder (short_data_hul DB)
            query_spread = f"""
            SELECT timestamp, pressure_spread
            FROM {autoencoder_table}
            ORDER BY timestamp DESC LIMIT 1;
            """
            with psycopg2.connect(**DB_CONFIG_SHORT) as conn:
                with conn.cursor() as cur:
                    cur.execute(query_spread)
                    spread_row = cur.fetchone()

            # 2️⃣ Fetch latest temps from mid table (hul DB)
            query_temp = f"""
            SELECT timestamp, hor_sealer_front_1_temp, hor_sealer_rear_1_temp
            FROM {mid_table}
            ORDER BY timestamp DESC LIMIT 1;
            """
            with psycopg2.connect(**DB_CONFIG_HUL) as conn:
                with conn.cursor() as cur:
                    cur.execute(query_temp)
                    temp_row = cur.fetchone()

            # 3️⃣ Merge results
            if spread_row and temp_row:
                return {
                    "timestamp": spread_row[0],   # timestamp from encoder
                    "spread": spread_row[1],
                    "front_temp": temp_row[1],
                    "rear_temp": temp_row[2]
                }

        except Exception:
            logging.exception(f"[MC{self.machine_id}] Error fetching latest spread/temps")
            return None


    def update_queue(self, spread_value):
        try:
            self.spread_queue.append(spread_value)
            #logging.info(f"[MC{self.machine_id}] Spread queue updated: {list(self.spread_queue)}")
        except Exception:
            logging.exception(f"[MC{self.machine_id}] Error updating queue")

    def run_monitor(self):
        try:
            status = self.fetch_machine_status()
            if not status or status["status"] != 1:
                logging.info(f"[MC{self.machine_id}] Machine not running.")
                return

            spread_data = self.fetch_latest_spread()
            if not spread_data or spread_data["spread"] is None:
                logging.warning(f"[MC{self.machine_id}] No spread data found.")
                return

            current_spread = spread_data["spread"]
            current_front_temp = spread_data["front_temp"]
            current_rear_temp = spread_data["rear_temp"]

            if self.last_spread is None:
                # First cycle → initialize
                self.front_set_temp = current_front_temp or self.front_set_temp
                self.rear_set_temp = current_rear_temp or self.rear_set_temp
            else:
                if current_spread < self.last_spread:
                    # Spread reduced → reset to previous temps
                    #logging.info(
                    #    f"[MC{self.machine_id}]"
                    #    f"resetting temps to FRONT={self.last_front_temp}, REAR={self.last_rear_temp}"
                    #)
                    self.front_set_temp = self.last_front_temp
                    self.rear_set_temp = self.last_rear_temp
                else:
                    # Spread stable or increased → keep or update
                    if current_front_temp is not None:
                        self.front_set_temp = current_front_temp
                    if current_rear_temp is not None:
                        self.rear_set_temp = current_rear_temp
            self.last_spread = current_spread
            self.last_front_temp = self.front_set_temp
            self.last_rear_temp = self.rear_set_temp

            self.update_queue(current_spread)
            self.check_spread_behavior()
        except Exception:
            logging.exception(f"[MC{self.machine_id}] Error in run_monitor")

    def write_to_plc(self, new_temp):
        try:
            logging.info(f"[MC{self.machine_id}] Writing new temp {new_temp}°C to PLC (commented).")
            # Actual PLC write would go here
        except Exception:
            logging.exception(f"[MC{self.machine_id}] Error writing to PLC")

if __name__ == "__main__":
    try:
        # Create monitors once and reuse them
        monitors = {mid: SpreadMonitor(machine_id=mid, config=CONFIG) for mid in CONFIG["machines"]}

        while True:
            for mid, monitor in monitors.items():
                try:
                    monitor.run_monitor()
                except Exception:
                    logging.exception(f"[MC{mid}] Fatal error in main loop")

            # sleep for N seconds before next cycle
            time.sleep(CONFIG.get("poll_interval", 5))  # default 5s if not in config

    except KeyboardInterrupt:
        logging.info("Program stopped manually.")
