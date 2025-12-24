#!/usr/bin/env python3
"""
Simple MC Protocol Boolean Tag Reader for MC27 & MC30
Reads two M-register tags (Leaping & Roll End) from both PLCs and prints to console
"""

import socket
import time
import logging
from datetime import datetime
from threading import Thread, Lock

# Configuration
PLC_CONFIG = {
    "MC27": {
        "ip": "141.141.141.209",
        "port": 4002,
    },
    "MC30": {
        "ip": "141.141.143.3",
        "port": 4004,
    }
}

READ_TIMEOUT = 0.5
POLL_INTERVAL = 1.0  # seconds

# Tag definitions (same for both PLCs)
TAGS = {
    "Leaping": 236,    # M236
    "Roll End": 237    # M237
}

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Thread-safe printing
print_lock = Lock()


class MCProtocol:
    """Mitsubishi MC Protocol for reading M-registers (boolean)"""

    def __init__(self, name: str, ip: str, port: int):
        self.name = name
        self.ip = ip
        self.port = port
        self.sock = None
        self.recv_buffer = bytearray(4096)
        self.send_buffer = bytearray(64)

    def connect(self) -> bool:
        """Connect to PLC"""
        try:
            if self.sock:
                self.sock.close()
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.sock.settimeout(READ_TIMEOUT)
            self.sock.connect((self.ip, self.port))
            logger.info(f"[{self.name}] Connected to PLC at {self.ip}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Connection failed: {e}")
            self.sock = None
            return False

    def reconnect(self):
        """Reconnect to PLC with retry logic"""
        while True:
            try:
                if self.sock:
                    self.sock.close()
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.sock.settimeout(READ_TIMEOUT)
                self.sock.connect((self.ip, self.port))
                logger.info(f"[{self.name}] Reconnected to PLC at {self.ip}:{self.port}")
                return True
            except Exception as e:
                logger.warning(f"[{self.name}] Reconnection failed, retrying in 5s: {e}")
                self.sock = None
                time.sleep(5)

    def read_bits(self, start_address: int, bit_count: int):
        """Read boolean bits from M-registers"""
        if not self.sock and not self.connect():
            return None

        # Build MC protocol command for bit read
        buf = self.send_buffer
        buf[0:21] = bytes([
            0x50, 0x00, 0x00, 0xFF, 0xFF, 0x03, 0x00, 0x0C, 0x00, 0x10, 0x00,
            0x01, 0x04, 0x00, 0x00,
            start_address & 0xFF,
            (start_address >> 8) & 0xFF,
            (start_address >> 16) & 0xFF,
            0x90,  # Device code for M-registers
            bit_count & 0xFF,
            (bit_count >> 8) & 0xFF
        ])

        try:
            self.sock.send(buf[:21])
            received = self.sock.recv_into(self.recv_buffer)

            # Check response validity
            if received < 11 or self.recv_buffer[9] != 0 or self.recv_buffer[10] != 0:
                return None

            # Extract bit data
            data_bytes = self.recv_buffer[11:11 + ((bit_count + 1) // 2)]
            bits = []
            for b in data_bytes:
                bits.append(b & 1)
                bits.append((b >> 4) & 1)

            return bits[:bit_count]

        except Exception as e:
            logger.warning(f"[{self.name}] Bit read failed: {e}")
            self.sock = None
            return None

    def close(self):
        """Close connection"""
        if self.sock:
            self.sock.close()
            self.sock = None


def read_tags(plc, tags_dict):
    """Read multiple M-register tags and return as dictionary"""
    if not tags_dict:
        return {}

    # Find the range of addresses to read
    addresses = list(tags_dict.values())
    min_addr = min(addresses)
    max_addr = max(addresses)
    bit_count = max_addr - min_addr + 1

    # Read the range
    bits = plc.read_bits(min_addr, bit_count)

    if bits is None:
        return None

    # Map bits to tag names
    result = {}
    for name, address in tags_dict.items():
        idx = address - min_addr
        if idx < len(bits):
            result[name] = bool(bits[idx])
        else:
            result[name] = None

    return result


def plc_reader_thread(plc_name, plc_config):
    """Thread function to read from a single PLC"""
    plc = MCProtocol(plc_name, plc_config["ip"], plc_config["port"])

    if not plc.connect():
        logger.error(f"[{plc_name}] Failed to connect to PLC. Thread exiting.")
        return

    try:
        while True:
            start_time = time.perf_counter()

            # Read tags
            values = read_tags(plc, TAGS)

            if values is None:
                logger.warning(f"[{plc_name}] Read failed. Reconnecting...")
                plc.reconnect()
                continue

            # Print to console (thread-safe)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            leaping = 1 if values['Leaping'] else 0
            roll_end = 1 if values['Roll End'] else 0

            with print_lock:
                print(f"[{timestamp}] [{plc_name}] Leaping: {leaping}, Roll End: {roll_end}")

            # Maintain polling interval
            elapsed = time.perf_counter() - start_time
            time.sleep(max(0, POLL_INTERVAL - elapsed))

    except Exception as e:
        logger.error(f"[{plc_name}] Thread error: {e}")
    finally:
        plc.close()
        logger.info(f"[{plc_name}] Disconnected from PLC")


def main():
    """Main function - starts threads for both PLCs"""
    logger.info("Starting continuous reading from MC27 and MC30. Press Ctrl+C to stop.")
    logger.info(f"Reading tags: {', '.join(TAGS.keys())}")
    print("-" * 80)

    # Create and start threads for both PLCs
    threads = []
    for plc_name, plc_config in PLC_CONFIG.items():
        thread = Thread(target=plc_reader_thread, args=(plc_name, plc_config), daemon=True)
        thread.start()
        threads.append(thread)

    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping...")
        logger.info("Waiting for threads to finish...")
        time.sleep(2)  # Give threads time to clean up


if __name__ == "__main__":
    main()
