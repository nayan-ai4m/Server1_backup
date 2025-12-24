#!/usr/bin/env python3
"""
MC30 PLC Logger (5-second polling) - Supports Float + Int + Bool registers
- FLOAT/INT tags → mc30_mid table (INSERT)
- BOOL tags (Leaping, Roll_End) → loop4_leaping_rollend table (UPDATE mc30 column)
- All other BOOL tags → mc30_mid table (INSERT, treated like other tags)
"""

import socket
import time
import threading
import queue
import logging
import re
import struct
from dataclasses import dataclass
from datetime import datetime
import json
import psycopg2

# ---------------- CONFIG ----------------
SAMPLE_INTERVAL_MS = 5000
READ_TIMEOUT = 2.0

CONFIG_FLOAT = "config_float.json"
CONFIG_INT = "config_int.json"
CONFIG_BOOL = "config_bool.json"
SCALING_CONFIG = "5s_scaling_config.json"
DB_CONFIG_FILE = "config.json"
TABLE_NAME_MID = "mc30_mid"
TABLE_NAME_LEAPING = "loop4_leaping_rollend"

PLC_IP = "141.141.143.3"
PLC_PORT = 4002

# Special BOOL tags that go to loop4_leaping_rollend table
LEAPING_ROLLEND_TAGS = {"leaping", "roll_end"}

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------- HELPER ----------------
def to_snake(name: str) -> str:
    """Convert tag names to snake_case for database consistency"""
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")

@dataclass
class PLCTag:
    name: str
    address: int
    data_type: str  # FLOAT, INT, BOOL
    index: int = 0

# ---------------- MC PROTOCOL ----------------
class OptimizedMCProtocol:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port
        self.sock = None
        self.recv_buffer = bytearray(8192)  # Larger buffer for MC30
        self.send_buffer = bytearray(64)

    def connect(self) -> bool:
        try:
            if self.sock: self.sock.close()
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.sock.settimeout(READ_TIMEOUT)
            self.sock.connect((self.ip, self.port))
            logger.info(f"✅ Connected to PLC at {self.ip}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"PLC connection failed: {e}")
            self.sock = None
            return False

    def reconnect(self):
        while True:
            try:
                if self.sock: self.sock.close()
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.sock.settimeout(READ_TIMEOUT)
                self.sock.connect((self.ip, self.port))
                logger.info(f"✅ Reconnected to PLC at {self.ip}:{self.port}")
                return True
            except Exception as e:
                logger.warning(f"⚠️ PLC reconnection failed, retrying in 5s: {e}")
                self.sock = None
                time.sleep(5)

    def read_registers(self, start_address: int, word_count: int):
        """Read R-registers (0xAF = R register type)"""
        if not self.sock and not self.connect(): return None
        buf = self.send_buffer
        buf[0:21] = bytes([
            0x50,0x00,0x00,0xFF,0xFF,0x03,0x00,0x0C,0x00,0x10,0x00,
            0x01,0x04,0x00,0x00,
            start_address & 0xFF,
            (start_address >> 8) & 0xFF,
            0x00,
            0xAF,  # R-register device code
            word_count & 0xFF,
            (word_count >> 8) & 0xFF
        ])
        try:
            self.sock.send(buf[:21])
            received = self.sock.recv_into(self.recv_buffer)
            if received < 11 or self.recv_buffer[9] != 0 or self.recv_buffer[10] != 0:
                logger.warning(f"Invalid R-register response: received={received}")
                return None
            return self.recv_buffer[11:11+word_count*2]
        except Exception as e:
            logger.warning(f"PLC R-register read failed: {e}")
            self.sock = None
            return None

    def read_bits(self, start_address: int, bit_count: int):
        """Read BOOL bits from M-registers (0x90 = M register type)"""
        if not self.sock and not self.connect(): return None
        buf = self.send_buffer
        buf[0:21] = bytes([
            0x50,0x00,0x00,0xFF,0xFF,0x03,0x00,0x0C,0x00,0x10,0x00,
            0x01,0x04,0x00,0x00,
            start_address & 0xFF,
            (start_address >> 8) & 0xFF,
            (start_address >> 16) & 0xFF,
            0x90,  # M-register device code
            bit_count & 0xFF,
            (bit_count >> 8) & 0xFF
        ])
        try:
            self.sock.send(buf[:21])
            received = self.sock.recv_into(self.recv_buffer)
            if received < 11 or self.recv_buffer[9] != 0 or self.recv_buffer[10] != 0:
                logger.warning(f"Invalid M-register response: received={received}")
                return None
            data_bytes = self.recv_buffer[11:11 + ((bit_count + 1)//2)]
            bits = []
            for b in data_bytes:
                bits.append(b & 1)
                bits.append((b >> 4) & 1)
            return bits[:bit_count]
        except Exception as e:
            logger.warning(f"PLC M-register read failed: {e}")
            self.sock = None
            return None

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None

# ---------------- POSTGRES LOGGER ----------------
class PostgresLogger:
    def __init__(self, db_config, table_name_mid, headers_mid):
        self.db_config = db_config
        self.table_name_mid = table_name_mid
        self.headers_mid = headers_mid
        self._connect()
        self._ensure_mid_table()

    def _connect(self):
        self.conn = psycopg2.connect(**self.db_config)
        self.conn.autocommit = True

    def _ensure_mid_table(self):
        """Ensure mc30_mid table exists"""
        with self.conn.cursor() as cur:
            cols = ", ".join([f'"{h}" DOUBLE PRECISION' for h in self.headers_mid])
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name_mid} (
                    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    {cols}
                )
            """)
        logger.info(f"Postgres table {self.table_name_mid} ready")

    def reconnect(self):
        while True:
            try:
                if self.conn:
                    try: self.conn.close()
                    except: pass
                self._connect()
                logger.info("✅ Reconnected to PostgreSQL")
                return True
            except Exception as e:
                logger.warning(f"⚠️ Database reconnection failed, retrying in 5s: {e}")
                time.sleep(3.7)

    def insert_mid_row(self, row):
        """Insert FLOAT/INT data into mc30_mid table"""
        try:
            cols = ", ".join([f'"{h}"' for h in self.headers_mid])
            placeholders = ", ".join(["%s"] * len(row))
            sql = f"INSERT INTO {self.table_name_mid} (timestamp, {cols}) VALUES (%s, {placeholders})"
            ts = datetime.now().astimezone()
            with self.conn.cursor() as cur:
                cur.execute(sql, [ts] + row)
        except Exception as e:
            logger.error(f"Failed to insert mid row: {e}")
            raise

    def update_leaping_rollend(self, leaping: int, roll_end: int):
        """Update mc30 column in loop4_leaping_rollend table (single row with id=1)"""
        try:
            sql = """
                UPDATE loop4_leaping_rollend
                SET mc30 = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            """
            mc30_data = json.dumps({"leaping": leaping, "roll_end": roll_end})
            with self.conn.cursor() as cur:
                cur.execute(sql, [mc30_data])
        except Exception as e:
            logger.error(f"Failed to update leaping/rollend: {e}")
            raise

    def close(self):
        self.conn.close()
        logger.info("Postgres closed")

# ---------------- TAG LOADERS ----------------
def load_tags(float_file, int_file, bool_file):
    """Load all tags and categorize them:
    - mid_tags: FLOAT/INT + non-special BOOL tags → mc30_mid table
    - special_bool_tags: leaping/roll_end → loop4_leaping_rollend table
    - all_bool_tags: all BOOL tags for M-register reading
    """
    all_tags = []

    with open(float_file) as f: float_cfg = json.load(f)
    with open(int_file) as f: int_cfg = json.load(f)
    with open(bool_file) as f: bool_cfg = json.load(f)

    # FLOAT tags (R-registers, 2 words = 32 bits)
    for k, v in float_cfg.items():
        all_tags.append(PLCTag(to_snake(k), int(v[1:]), "FLOAT"))

    # INT tags (R-registers, 1 word = 16 bits)
    for k, v in int_cfg.items():
        all_tags.append(PLCTag(to_snake(k), int(v[1:]), "INT"))

    # BOOL tags (M-registers)
    for k, v in bool_cfg.items():
        all_tags.append(PLCTag(to_snake(k), int(v[1:]), "BOOL"))

    # Separate tags by destination
    mid_tags = [t for t in all_tags if t.data_type in ("FLOAT", "INT")]
    all_bool_tags = [t for t in all_tags if t.data_type == "BOOL"]

    # Special BOOL tags for leaping/rollend table
    special_bool_tags = [t for t in all_bool_tags if t.name in LEAPING_ROLLEND_TAGS]

    # Other BOOL tags go to mid table
    other_bool_tags = [t for t in all_bool_tags if t.name not in LEAPING_ROLLEND_TAGS]
    mid_tags.extend(other_bool_tags)

    # Calculate R-register range (for FLOAT + INT only, BOOL uses M-registers)
    r_tags = [t for t in mid_tags if t.data_type in ("FLOAT", "INT")]
    if r_tags:
        min_addr = min(t.address for t in r_tags)
        max_addr = max(t.address + (1 if t.data_type == "FLOAT" else 0) for t in r_tags)
        word_count = max_addr - min_addr + 1
        for t in r_tags:
            t.index = t.address - min_addr
    else:
        min_addr = word_count = 0

    return mid_tags, all_bool_tags, special_bool_tags, min_addr, word_count

def apply_transformation(val, tag: PLCTag, transform_config):
    """Apply scaling to FLOAT/INT tags using R-register address"""
    if val is None or tag.data_type not in ("FLOAT", "INT"):
        return val
    register_addr = f"R{tag.address}"
    if register_addr not in transform_config:
        return val
    try:
        mul, add = transform_config[register_addr]
        return round((val * mul) + add, 2)
    except Exception as e:
        logger.warning(f"Scaling error for {tag.name} ({register_addr}): {e}")
        return val

# ---------------- PLC READER ----------------
def plc_reader_thread(data_queue, bool_queue, stop_event, plc, mid_tags, all_bool_tags, special_bool_tags, min_addr, word_count, transform_config):
    while not stop_event.is_set():
        try:
            start = time.perf_counter()

            # ============ Read M-registers (ALL BOOL) first ============
            bool_values_dict = {}
            if all_bool_tags:
                m_min = min(t.address for t in all_bool_tags)
                m_max = max(t.address for t in all_bool_tags)
                bit_count = m_max - m_min + 1
                bits = plc.read_bits(m_min, bit_count)

                if bits:
                    for t in all_bool_tags:
                        idx = t.address - m_min
                        bool_values_dict[t.name] = bits[idx] if idx < len(bits) else 0
                else:
                    # Read failed, reconnect
                    plc.reconnect()
                    continue

            # ============ Read R-registers (FLOAT + INT) ============
            raw_data = plc.read_registers(min_addr, word_count)
            if not raw_data:
                plc.reconnect()
                continue

            # Build row values for mid_tags (FLOAT/INT + other BOOL tags)
            mid_row_values = []
            for t in mid_tags:
                val = None
                if t.data_type == "INT" and t.index < len(raw_data)//2:
                    val = struct.unpack("<h", raw_data[t.index*2:t.index*2+2])[0]
                    val = apply_transformation(val, t, transform_config)
                elif t.data_type == "FLOAT" and t.index*2+3 < len(raw_data):
                    val = struct.unpack("<f", raw_data[t.index*2:t.index*2+4])[0]
                    val = apply_transformation(val, t, transform_config)
                elif t.data_type == "BOOL":
                    # Non-special BOOL tag, get from bool_values_dict
                    val = bool_values_dict.get(t.name, 0)

                mid_row_values.append(val)

            data_queue.put_nowait(mid_row_values)

            # ============ Queue special BOOL tags (leaping/roll_end) ============
            if special_bool_tags:
                special_bool_values = {t.name: bool_values_dict.get(t.name, 0) for t in special_bool_tags}
                bool_queue.put_nowait(special_bool_values)

            # Maintain polling interval
            elapsed = time.perf_counter() - start
            time.sleep(max(0, (SAMPLE_INTERVAL_MS / 1000) - elapsed))

        except Exception as e:
            logger.error(f"PLC read error: {e}", exc_info=True)
            time.sleep(1)

# ---------------- DB WORKER ----------------
def db_worker_thread(data_queue, bool_queue, stop_event, pg_logger):
    while not stop_event.is_set() or not data_queue.empty() or not bool_queue.empty():
        try:
            # Process FLOAT/INT data (insert into mc30_mid)
            try:
                mid_row = data_queue.get(timeout=0.1)
                try:
                    pg_logger.insert_mid_row(mid_row)
                except Exception:
                    logger.warning("Reconnecting to DB...")
                    pg_logger.reconnect()
                    pg_logger.insert_mid_row(mid_row)
            except queue.Empty:
                pass

            # Process BOOL data (update loop4_leaping_rollend)
            try:
                bool_values = bool_queue.get(timeout=0.1)
                leaping = bool_values.get("leaping", 0)
                roll_end = bool_values.get("roll_end", 0)
                try:
                    pg_logger.update_leaping_rollend(leaping, roll_end)
                except Exception:
                    logger.warning("Reconnecting to DB...")
                    pg_logger.reconnect()
                    pg_logger.update_leaping_rollend(leaping, roll_end)
            except queue.Empty:
                pass

        except Exception as e:
            logger.error(f"DB worker error: {e}", exc_info=True)

# ---------------- MAIN ----------------
def main():
    mid_tags, all_bool_tags, special_bool_tags, min_addr, word_count = load_tags(CONFIG_FLOAT, CONFIG_INT, CONFIG_BOOL)

    with open(SCALING_CONFIG) as f:
        transform_config = json.load(f)

    with open(DB_CONFIG_FILE) as f:
        db_config = json.load(f)["database"]

    headers_mid = [t.name for t in mid_tags]

    plc = OptimizedMCProtocol(PLC_IP, PLC_PORT)
    plc.connect()

    pg_logger = PostgresLogger(db_config, TABLE_NAME_MID, headers_mid)

    data_queue = queue.Queue(maxsize=100)
    bool_queue = queue.Queue(maxsize=100)
    stop_event = threading.Event()

    reader = threading.Thread(
        target=plc_reader_thread,
        args=(data_queue, bool_queue, stop_event, plc, mid_tags, all_bool_tags, special_bool_tags, min_addr, word_count, transform_config),
        daemon=True
    )

    db_worker = threading.Thread(
        target=db_worker_thread,
        args=(data_queue, bool_queue, stop_event, pg_logger),
        daemon=True
    )

    reader.start()
    db_worker.start()
    logger.info("Started PLC reader and DB worker threads")

    # Count tags by type
    float_count = len([t for t in mid_tags if t.data_type == "FLOAT"])
    int_count = len([t for t in mid_tags if t.data_type == "INT"])
    other_bool_count = len([t for t in mid_tags if t.data_type == "BOOL"])

    logger.info(f"→ {TABLE_NAME_MID} table: {float_count} FLOAT + {int_count} INT + {other_bool_count} other BOOL tags")
    logger.info(f"→ {TABLE_NAME_LEAPING} table: {len(special_bool_tags)} special BOOL tags (leaping/roll_end)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping threads...")
        stop_event.set()
        reader.join(timeout=3)
        db_worker.join(timeout=3)
    finally:
        plc.close()
        pg_logger.close()
        logger.info("Shutdown complete")

if __name__ == "__main__":
    main()
