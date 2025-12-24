import psycopg2
import asyncio
import logging
import json
from dataclasses import asdict, dataclass
from typing import Dict, Any, Optional
from datetime import datetime
import nats
from fastapi import HTTPException

# ================= Logging =================
today_str = datetime.now().strftime("%Y-%m-%d")
log_file = f"logs/{today_str}/mc17_temp_control.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)

# ================= DB Config =================
DB_CONFIG = {
    "dbname": "hul",
    "user": "postgres",
    "password": "ai4m2024",   # update if needed
    "host": "localhost",
    "port": 5432
}

# ================= PLC Tags (MC17 only) =================
PLC_TAGS = {
    "front": "HMI_Hor_Seal_Front_27",
    "rear": "HMI_Hor_Seal_Rear_28"
}

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



# ================= NATS Connection =================
nats_client: Optional[nats.NATS] = None

async def get_nats_connection():
    """Create or reuse NATS connection"""
    global nats_client
    try:
        if nats_client is None or not nats_client.is_connected:
            nats_client = await nats.connect(
                "nats://192.168.1.149:4222",
                connect_timeout=5,
                max_reconnect_attempts=3,
                reconnect_time_wait=1
            )
        return nats_client
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed to connect to NATS server: {str(e)}")

async def send_nats_request(plc: str, command: str, name: str, value: float, status: bool) -> Dict[str, Any]:
    """Send request to NATS server with retry logic"""
    try:
        nc = await get_nats_connection()
        print(f"NC:{nc}")

        # Topic mapping
        if plc in ["17", "18"]:
            topic = "adv.217"
        elif plc in ["19", "20"]:
            topic = "adv.160"
        elif plc in ["21", "22"]:
            topic = "adv.150"
        elif plc in ["25","26"]:
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

# ================= DB Functions =================
def get_latest_temps():
    """Fetch the latest front/rear temps from mc17_mid table"""
    query = """
        SELECT hor_sealer_front_1_temp,hor_sealer_rear_1_temp
        FROM mc17_mid
        ORDER BY timestamp DESC
        LIMIT 1;
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(query)
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return row[0], row[1]
    except Exception:
        logging.exception("Error fetching latest temps from DB")
    return None, None

# ================= Main Logic =================
async def main():
    front, rear = get_latest_temps()
    if front is None or rear is None:
        logging.error("No temperature data available from DB")
        return

    logging.info(f"Latest DB Temps: FRONT={front}°C, REAR={rear}°C")

    # Example: increase by +1
    new_front = front + 1
    new_rear = rear + 1

    logging.info(f"New Temps: FRONT={new_front}°C, REAR={new_rear}°C")

    # Write to PLC
    try:
        resp1 = await send_nats_request(
            plc="17",
            command="update",
            name=PLC_TAGS["front"],
            value=new_front,
            status=True
        )
        resp2 = await send_nats_request(
            plc="17",
            command="update",
            name=PLC_TAGS["rear"],
            value=new_rear,
            status=True
        )
        logging.info(f"PLC Write Success: FRONT={resp1}, REAR={resp2}")
    except Exception as e:
        logging.exception(f"Error writing to PLC: {e}")

if __name__ == "__main__":
    asyncio.run(main())

