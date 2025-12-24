#!/usr/bin/env python3
"""
File: plc_main.py
Date: July 25, 2025
Version: 1.0
Author: Sushant Chougule
Owner: Mesmerise Softtech Private Limited
Description: A Python service for fetching data from Mitsubishi PLC over SLMP protocol,
for specific parameters, at a frequency of 1 sec and storing it in PostgreSQL Database.

Copyright © 2025 [Mesmerise Softtech Private Limited]. All rights reserved.
"""

import asyncio
import socket
import struct
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
import logging
import os
from dataclasses import dataclass, field
from contextlib import contextmanager

# Configure logging
LOG_DIR = "./" if os.name == "posix" else "C:\\Logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "PlcService_Error.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


@dataclass
class PlcRecord:
    """Data class representing a PLC record with all measured parameters."""

    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Temperature Set Values - Vertical Front
    temp_set_value_vert_front_1: Optional[int] = None
    temp_set_value_vert_front_2: Optional[int] = None
    temp_set_value_vert_front_3: Optional[int] = None
    temp_set_value_vert_front_4: Optional[int] = None
    temp_set_value_vert_front_5: Optional[int] = None
    temp_set_value_vert_front_6: Optional[int] = None
    temp_set_value_vert_front_7: Optional[int] = None
    temp_set_value_vert_front_8: Optional[int] = None
    temp_set_value_vert_front_9: Optional[int] = None
    temp_set_value_vert_front_10: Optional[int] = None
    temp_set_value_vert_front_11: Optional[int] = None
    temp_set_value_vert_front_12: Optional[int] = None
    temp_set_value_vert_front_13: Optional[int] = None

    # Temperature Set Values - Vertical Rear
    temp_set_value_vert_rear_1: Optional[int] = None
    temp_set_value_vert_rear_2: Optional[int] = None
    temp_set_value_vert_rear_3: Optional[int] = None
    temp_set_value_vert_rear_4: Optional[int] = None
    temp_set_value_vert_rear_5: Optional[int] = None
    temp_set_value_vert_rear_6: Optional[int] = None
    temp_set_value_vert_rear_7: Optional[int] = None
    temp_set_value_vert_rear_8: Optional[int] = None
    temp_set_value_vert_rear_9: Optional[int] = None
    temp_set_value_vert_rear_10: Optional[int] = None
    temp_set_value_vert_rear_11: Optional[int] = None
    temp_set_value_vert_rear_12: Optional[int] = None
    temp_set_value_vert_rear_13: Optional[int] = None

    # Temperature Set Values - Horizontal
    temp_set_value_hor_temp_27_sv_front: Optional[int] = None
    temp_set_value_hor_temp_28_sv_rear: Optional[int] = None

    # Temperature Actual Values - Vertical Front
    temp_actual_value_vert_front_1: Optional[int] = None
    temp_actual_value_vert_front_2: Optional[int] = None
    temp_actual_value_vert_front_3: Optional[int] = None
    temp_actual_value_vert_front_4: Optional[int] = None
    temp_actual_value_vert_front_5: Optional[int] = None
    temp_actual_value_vert_front_6: Optional[int] = None
    temp_actual_value_vert_front_7: Optional[int] = None
    temp_actual_value_vert_front_8: Optional[int] = None
    temp_actual_value_vert_front_9: Optional[int] = None
    temp_actual_value_vert_front_10: Optional[int] = None
    temp_actual_value_vert_front_11: Optional[int] = None
    temp_actual_value_vert_front_12: Optional[int] = None
    temp_actual_value_vert_front_13: Optional[int] = None

    # Temperature Actual Values - Vertical Rear
    temp_actual_value_vert_rear_1: Optional[int] = None
    temp_actual_value_vert_rear_2: Optional[int] = None
    temp_actual_value_vert_rear_3: Optional[int] = None
    temp_actual_value_vert_rear_4: Optional[int] = None
    temp_actual_value_vert_rear_5: Optional[int] = None
    temp_actual_value_vert_rear_6: Optional[int] = None
    temp_actual_value_vert_rear_7: Optional[int] = None
    temp_actual_value_vert_rear_8: Optional[int] = None
    temp_actual_value_vert_rear_9: Optional[int] = None
    temp_actual_value_vert_rear_10: Optional[int] = None
    temp_actual_value_vert_rear_11: Optional[int] = None
    temp_actual_value_vert_rear_12: Optional[int] = None
    temp_actual_value_vert_rear_13: Optional[int] = None

    # Temperature Actual Values - Horizontal
    temp_actual_value_hor_temp_27_pv_front: Optional[int] = None
    temp_actual_value_hor_temp_28_pv_rear: Optional[int] = None

    # Other integer values
    hopper_level: Optional[int] = None
    hopper_extreme_low_level: Optional[int] = None
    hopper_low_level: Optional[int] = None
    hopper_high_level: Optional[int] = None
    shift_a: Optional[int] = None
    shift_b: Optional[int] = None
    shift_c: Optional[int] = None
    actual_cld_count: Optional[int] = None
    machine_speed: Optional[int] = None
    puller_speed: Optional[int] = None

    # Float/Real values
    hori_sealer_fwd_start_deg_hmi: Optional[float] = None
    hori_sealer_end_start_deg_hmi: Optional[float] = None
    hori_sealer_rev_start_deg_hmi: Optional[float] = None
    hori_sealer_rev_end_deg_hmi: Optional[float] = None
    hori_sealer_stroke_1: Optional[float] = None
    hori_sealer_stroke_2: Optional[float] = None
    hs_stroke_2_start_deg: Optional[float] = None
    vert_sealer_fwd_start_deg_hmi: Optional[float] = None
    vert_sealer_end_start_deg_hmi: Optional[float] = None
    vert_sealer_rev_start_deg_hmi: Optional[float] = None
    vert_sealer_rev_end_deg_hmi: Optional[float] = None
    vert_sealer_stroke_1: Optional[float] = None
    vert_sealer_stroke_2: Optional[float] = None
    vs_stroke_2_start_deg: Optional[float] = None
    fwd_start_deg: Optional[float] = None
    fwd_end_deg: Optional[float] = None
    rev_start_deg: Optional[float] = None
    rev_end_deg: Optional[float] = None
    stroke_length_1: Optional[float] = None
    valve_fwd_start_deg_hmi: Optional[float] = None
    valve_end_start_deg_hmi: Optional[float] = None
    valve_rev_start_deg_hmi: Optional[float] = None
    valve_rev_end_deg_hmi: Optional[float] = None
    valve_stroke_length_1: Optional[float] = None


class PLCService:
    """Main PLC service class for data collection and database operations."""

    def __init__(self):
        self.conn_str = "host=localhost port=5432 dbname=postgres user=postgres password=unilever2024"
        self.plc_ip = "141.141.143.3"
        self.plc_port = 4003
        self.socket = None
        self.using_buffer_a = True
        self.buffer_start_time = datetime.min
        self.buffer_a: List[PlcRecord] = []
        self.buffer_b: List[PlcRecord] = []

        # Define register mappings
        self.int_register_keys = {
            # Temperature Set Values
            "temp set value vert.front-1",
            "temp set value vert.front-2",
            "temp set value vert.front-3",
            "temp set value vert.front-4",
            "temp set value vert.front-5",
            "temp set value vert.front-6",
            "temp set value vert.front-7",
            "temp set value vert.front-8",
            "temp set value vert.front-9",
            "temp set value vert.front-10",
            "temp set value vert.front-11",
            "temp set value vert.front-12",
            "temp set value vert.front-13",
            "temp set value vert.rear-1",
            "temp set value vert.rear-2",
            "temp set value vert.rear-3",
            "temp set value vert.rear-4",
            "temp set value vert.rear-5",
            "temp set value vert.rear-6",
            "temp set value vert.rear-7",
            "temp set value vert.rear-8",
            "temp set value vert.rear-9",
            "temp set value vert.rear-10",
            "temp set value vert.rear-11",
            "temp set value vert.rear-12",
            "temp set value vert.rear-13",
            "temp set value hor. temp-27 sv (front)",
            "temp set value hor. temp-28 sv (rear)",
            # Temperature Actual Values
            "temp actual value vert.front-1",
            "temp actual value vert.front-2",
            "temp actual value vert.front-3",
            "temp actual value vert.front-4",
            "temp actual value vert.front-5",
            "temp actual value vert.front-6",
            "temp actual value vert.front-7",
            "temp actual value vert.front-8",
            "temp actual value vert.front-9",
            "temp actual value vert.front-10",
            "temp actual value vert.front-11",
            "temp actual value vert.front-12",
            "temp actual value vert.front-13",
            "temp actual value vert.rear-1",
            "temp actual value vert.rear-2",
            "temp actual value vert.rear-3",
            "temp actual value vert.rear-4",
            "temp actual value vert.rear-5",
            "temp actual value vert.rear-6",
            "temp actual value vert.rear-7",
            "temp actual value vert.rear-8",
            "temp actual value vert.rear-9",
            "temp actual value vert.rear-10",
            "temp actual value vert.rear-11",
            "temp actual value vert.rear-12",
            "temp actual value vert.rear-13",
            "temp actual value hor. temp-27 pv (front)",
            "temp actual value hor. temp-28 pv (rear)",
            # Other values
            "hopper level",
            "hopper extreme low level",
            "hopper low level",
            "hopper high level",
            "shift a",
            "shift b",
            "shift c",
            "actual cld count",
            "machine speed",
            "puller speed",
            "cam_position",
        }

        self.real_tag_descriptions = {
            "r1000_r1001": "Hori Sealer Fwd Start Deg HMI",
            "r1002_r1003": "Hori Sealer End Start Deg HMI",
            "r1004_r1005": "Hori Sealer Rev Start Deg HMI",
            "r1006_r1007": "Hori Sealer Rev End Deg HMI",
            "r1008_r1009": "Hori Sealer Stroke-1",
            "r1010_r1011": "Hori Sealer Stroke-2",
            "r1012_r1013": "HS_Stroke-2 Start Deg",
            "r1014_r1015": "Vert Sealer Fwd Start Deg HMI",
            "r1016_r1017": "Vert Sealer End Start Deg HMI",
            "r1018_r1019": "Vert Sealer Rev Start Deg HMI",
            "r1020_r1021": "Vert Sealer Rev End Deg HMI",
            "r1022_r1023": "Vert Sealer Stroke-1",
            "r1024_r1025": "Vert Sealer Stroke-2",
            "r1026_r1027": "VS_Stroke-2 Start Deg",
            "r1028_r1029": "FWD START DEG",
            "r1030_r1031": "FWD END DEG",
            "r1032_r1033": "REV START DEG",
            "r1034_r1035": "REV END DEG",
            "r1036_r1037": "STROKE LENGTH-1",
            "r1038_r1039": "Valve Fwd Start Deg HMI",
            "r1040_r1041": "Valve End Start Deg HMI",
            "r1042_r1043": "Valve Rev Start Deg HMI",
            "r1044_r1045": "Valve Rev End Deg HMI",
            "r1046_r1047": "Valve Stroke Length-1",
        }

        self.register_map = {
            # Temperature Set Values
            "temp set value vert.front-1": 0x4B0,
            "temp set value vert.front-2": 0x4B1,
            "temp set value vert.front-3": 0x4B2,
            "temp set value vert.front-4": 0x4B3,
            "temp set value vert.front-5": 0x4B4,
            "temp set value vert.front-6": 0x4B5,
            "temp set value vert.front-7": 0x4B6,
            "temp set value vert.front-8": 0x4B7,
            "temp set value vert.front-9": 0x4B8,
            "temp set value vert.front-10": 0x4B9,
            "temp set value vert.front-11": 0x4BA,
            "temp set value vert.front-12": 0x4BB,
            "temp set value vert.front-13": 0x4BC,
            "temp set value vert.rear-1": 0x4BD,
            "temp set value vert.rear-2": 0x4BE,
            "temp set value vert.rear-3": 0x4BF,
            "temp set value vert.rear-4": 0x4C0,
            "temp set value vert.rear-5": 0x4C1,
            "temp set value vert.rear-6": 0x4C2,
            "temp set value vert.rear-7": 0x4C3,
            "temp set value vert.rear-8": 0x4C4,
            "temp set value vert.rear-9": 0x4C5,
            "temp set value vert.rear-10": 0x4C6,
            "temp set value vert.rear-11": 0x4C7,
            "temp set value vert.rear-12": 0x4C8,
            "temp set value vert.rear-13": 0x4C9,
            "temp set value hor. temp-27 sv (front)": 0x4CA,
            "temp set value hor. temp-28 sv (rear)": 0x4CB,
            # Temperature Actual Values
            "temp actual value vert.front-1": 0x4CC,
            "temp actual value vert.front-2": 0x4CD,
            "temp actual value vert.front-3": 0x4CE,
            "temp actual value vert.front-4": 0x4CF,
            "temp actual value vert.front-5": 0x4D0,
            "temp actual value vert.front-6": 0x4D1,
            "temp actual value vert.front-7": 0x4D2,
            "temp actual value vert.front-8": 0x4D3,
            "temp actual value vert.front-9": 0x4D4,
            "temp actual value vert.front-10": 0x4D5,
            "temp actual value vert.front-11": 0x4D6,
            "temp actual value vert.front-12": 0x4D7,
            "temp actual value vert.front-13": 0x4D8,
            "temp actual value vert.rear-1": 0x4D9,
            "temp actual value vert.rear-2": 0x4DA,
            "temp actual value vert.rear-3": 0x4DB,
            "temp actual value vert.rear-4": 0x4DC,
            "temp actual value vert.rear-5": 0x4DD,
            "temp actual value vert.rear-6": 0x4DE,
            "temp actual value vert.rear-7": 0x4DF,
            "temp actual value vert.rear-8": 0x4E0,
            "temp actual value vert.rear-9": 0x4E1,
            "temp actual value vert.rear-10": 0x4E2,
            "temp actual value vert.rear-11": 0x4E3,
            "temp actual value vert.rear-12": 0x4E4,
            "temp actual value vert.rear-13": 0x4E5,
            "temp actual value hor. temp-27 pv (front)": 0x4E6,
            "temp actual value hor. temp-28 pv (rear)": 0x4E7,
            # Other values
            "hopper level": 0x4E8,
            "hopper extreme low level": 0x4E9,
            "hopper low level": 0x4EA,
            "hopper high level": 0x4EB,
            "shift a": 0x4EC,
            "shift b": 0x4ED,
            "shift c": 0x4EE,
            "actual cld count": 0x4EF,
            "machine speed": 0x4F0,
            "puller speed": 0x4F3,
            # Float registers
            **{f"r{1000+i}": 0x3E8 + i for i in range(48)},
        }

        # Create reverse mapping
        self.address_to_tag_map = {v: k for k, v in self.register_map.items()}

        # Float pairs for combining registers
        self.float_pairs = [(f"r{1000+i}", f"r{1000+i+1}") for i in range(0, 47, 2)]
        self.float_register_keys = {f"{pair[0]}_{pair[1]}" for pair in self.float_pairs}

        # Read ranges
        self.read_ranges = [(0x4B0, 70), (0x3E8, 50)]

        # Individual float halves
        self.individual_float_halves = {f"r{1000+i}" for i in range(48)}

    def connect_tcp(self) -> bool:
        """Establish TCP connection to PLC."""
        try:
            logger.info(f"Connecting to PLC {self.plc_ip}:{self.plc_port}...")
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(1.0)
            self.socket.connect((self.plc_ip, self.plc_port))
            logger.info("✅ Connected to PLC.")
            return True
        except Exception as e:
            logger.error(f"❌ TCP connection failed: {e}")
            return False

    def is_connected(self) -> bool:
        """Check if TCP connection is still active."""
        if not self.socket:
            return False
        try:
            # Send a test packet to check connection
            self.socket.send(b"", socket.MSG_DONTWAIT)
            return True
        except (socket.error, OSError):
            return False

    def read_registers_in_batches(self) -> Dict[str, float]:
        """Read PLC registers in batches and return processed values."""
        result = {}
        temp_ints = {}

        if not self.socket:
            logger.error("No socket connection available")
            return result

        for start, count in self.read_ranges:
            try:
                # Construct SLMP request packet
                request = bytearray(
                    [
                        0x50,
                        0x00,
                        0x00,
                        0xFF,
                        0xFF,
                        0x03,
                        0x00,
                        0x0C,
                        0x00,
                        0x10,
                        0x00,
                        0x01,
                        0x04,
                        0x00,
                        0x00,
                        start & 0xFF,
                        (start >> 8) & 0xFF,
                        0x00,
                        0xAF,
                        count & 0xFF,
                        (count >> 8) & 0xFF,
                    ]
                )

                self.socket.send(request)
                response = self.socket.recv(11 + 2 * count)

                if len(response) < 11 + 2 * count:
                    logger.warning("Incomplete response received")
                    continue

                if response[9] != 0 or response[10] != 0:
                    logger.error("PLC returned error in response")
                    continue

                # Parse register values
                for i in range(count):
                    addr = start + i
                    val = struct.unpack("<H", response[11 + 2 * i : 11 + 2 * i + 2])[0]
                    if addr in self.address_to_tag_map:
                        temp_ints[self.address_to_tag_map[addr]] = val

            except Exception as e:
                logger.warning(f"⚠️ Read error: {e}")
                continue

        # Process float pairs
        for tag1, tag2 in self.float_pairs:
            if tag1 in temp_ints and tag2 in temp_ints:
                # Combine two 16-bit registers into one 32-bit float
                bytes_data = struct.pack("<HH", temp_ints[tag1], temp_ints[tag2])
                float_val = struct.unpack("<f", bytes_data)[0]
                float_key = f"{tag1}_{tag2}"
                result[float_key.upper()] = float_val

        # Add integer values (excluding float halves)
        for tag, val in temp_ints.items():
            if tag not in self.individual_float_halves:
                result[tag.upper()] = float(val)

        return result

    def map_values_to_record(self, values: Dict[str, float]) -> PlcRecord:
        """Map read values to PlcRecord object."""
        record = PlcRecord()

        for tag, val in values.items():
            tag_upper = tag.upper()

            # Skip individual float halves
            if tag.lower() in self.individual_float_halves:
                continue

            # Map temperature set values - vertical front
            if tag_upper == "TEMP SET VALUE VERT.FRONT-1":
                record.temp_set_value_vert_front_1 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.FRONT-2":
                record.temp_set_value_vert_front_2 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.FRONT-3":
                record.temp_set_value_vert_front_3 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.FRONT-4":
                record.temp_set_value_vert_front_4 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.FRONT-5":
                record.temp_set_value_vert_front_5 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.FRONT-6":
                record.temp_set_value_vert_front_6 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.FRONT-7":
                record.temp_set_value_vert_front_7 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.FRONT-8":
                record.temp_set_value_vert_front_8 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.FRONT-9":
                record.temp_set_value_vert_front_9 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.FRONT-10":
                record.temp_set_value_vert_front_10 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.FRONT-11":
                record.temp_set_value_vert_front_11 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.FRONT-12":
                record.temp_set_value_vert_front_12 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.FRONT-13":
                record.temp_set_value_vert_front_13 = int(val)

            # Map temperature set values - vertical rear
            elif tag_upper == "TEMP SET VALUE VERT.REAR-1":
                record.temp_set_value_vert_rear_1 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.REAR-2":
                record.temp_set_value_vert_rear_2 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.REAR-3":
                record.temp_set_value_vert_rear_3 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.REAR-4":
                record.temp_set_value_vert_rear_4 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.REAR-5":
                record.temp_set_value_vert_rear_5 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.REAR-6":
                record.temp_set_value_vert_rear_6 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.REAR-7":
                record.temp_set_value_vert_rear_7 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.REAR-8":
                record.temp_set_value_vert_rear_8 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.REAR-9":
                record.temp_set_value_vert_rear_9 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.REAR-10":
                record.temp_set_value_vert_rear_10 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.REAR-11":
                record.temp_set_value_vert_rear_11 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.REAR-12":
                record.temp_set_value_vert_rear_12 = int(val)
            elif tag_upper == "TEMP SET VALUE VERT.REAR-13":
                record.temp_set_value_vert_rear_13 = int(val)

            # Map horizontal temperature set values
            elif tag_upper == "TEMP SET VALUE HOR. TEMP-27 SV (FRONT)":
                record.temp_set_value_hor_temp_27_sv_front = int(val)
            elif tag_upper == "TEMP SET VALUE HOR. TEMP-28 SV (REAR)":
                record.temp_set_value_hor_temp_28_sv_rear = int(val)

            # Map temperature actual values - vertical front
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-1":
                record.temp_actual_value_vert_front_1 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-2":
                record.temp_actual_value_vert_front_2 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-3":
                record.temp_actual_value_vert_front_3 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-4":
                record.temp_actual_value_vert_front_4 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-5":
                record.temp_actual_value_vert_front_5 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-6":
                record.temp_actual_value_vert_front_6 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-7":
                record.temp_actual_value_vert_front_7 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-8":
                record.temp_actual_value_vert_front_8 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-9":
                record.temp_actual_value_vert_front_9 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-10":
                record.temp_actual_value_vert_front_10 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-11":
                record.temp_actual_value_vert_front_11 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-12":
                record.temp_actual_value_vert_front_12 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.FRONT-13":
                record.temp_actual_value_vert_front_13 = int(val)

            # Map temperature actual values - vertical rear
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-1":
                record.temp_actual_value_vert_rear_1 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-2":
                record.temp_actual_value_vert_rear_2 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-3":
                record.temp_actual_value_vert_rear_3 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-4":
                record.temp_actual_value_vert_rear_4 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-5":
                record.temp_actual_value_vert_rear_5 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-6":
                record.temp_actual_value_vert_rear_6 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-7":
                record.temp_actual_value_vert_rear_7 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-8":
                record.temp_actual_value_vert_rear_8 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-9":
                record.temp_actual_value_vert_rear_9 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-10":
                record.temp_actual_value_vert_rear_10 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-11":
                record.temp_actual_value_vert_rear_11 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-12":
                record.temp_actual_value_vert_rear_12 = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE VERT.REAR-13":
                record.temp_actual_value_vert_rear_13 = int(val)

            # Map horizontal temperature actual values
            elif tag_upper == "TEMP ACTUAL VALUE HOR. TEMP-27 PV (FRONT)":
                record.temp_actual_value_hor_temp_27_pv_front = int(val)
            elif tag_upper == "TEMP ACTUAL VALUE HOR. TEMP-28 PV (REAR)":
                record.temp_actual_value_hor_temp_28_pv_rear = int(val)

            # Map other integer values
            elif tag_upper == "HOPPER LEVEL":
                record.hopper_level = int(val)
            elif tag_upper == "HOPPER EXTREME LOW LEVEL":
                record.hopper_extreme_low_level = int(val)
            elif tag_upper == "HOPPER LOW LEVEL":
                record.hopper_low_level = int(val)
            elif tag_upper == "HOPPER HIGH LEVEL":
                record.hopper_high_level = int(val)
            elif tag_upper == "SHIFT A":
                record.shift_a = int(val)
            elif tag_upper == "SHIFT B":
                record.shift_b = int(val)
            elif tag_upper == "SHIFT C":
                record.shift_c = int(val)
            elif tag_upper == "ACTUAL CLD COUNT":
                record.actual_cld_count = int(val)
            elif tag_upper == "MACHINE SPEED":
                record.machine_speed = int(val)
            elif tag_upper == "PULLER SPEED":
                record.puller_speed = int(val)

            # Map combined float registers
            elif tag_upper == "R1000_R1001":
                record.hori_sealer_fwd_start_deg_hmi = val
            elif tag_upper == "R1002_R1003":
                record.hori_sealer_end_start_deg_hmi = val
            elif tag_upper == "R1004_R1005":
                record.hori_sealer_rev_start_deg_hmi = val
            elif tag_upper == "R1006_R1007":
                record.hori_sealer_rev_end_deg_hmi = val
            elif tag_upper == "R1008_R1009":
                record.hori_sealer_stroke_1 = val
            elif tag_upper == "R1010_R1011":
                record.hori_sealer_stroke_2 = val
            elif tag_upper == "R1012_R1013":
                record.hs_stroke_2_start_deg = val
            elif tag_upper == "R1014_R1015":
                record.vert_sealer_fwd_start_deg_hmi = val
            elif tag_upper == "R1016_R1017":
                record.vert_sealer_end_start_deg_hmi = val
            elif tag_upper == "R1018_R1019":
                record.vert_sealer_rev_start_deg_hmi = val
            elif tag_upper == "R1020_R1021":
                record.vert_sealer_rev_end_deg_hmi = val
            elif tag_upper == "R1022_R1023":
                record.vert_sealer_stroke_1 = val
            elif tag_upper == "R1024_R1025":
                record.vert_sealer_stroke_2 = val
            elif tag_upper == "R1026_R1027":
                record.vs_stroke_2_start_deg = val
            elif tag_upper == "R1028_R1029":
                record.fwd_start_deg = val
            elif tag_upper == "R1030_R1031":
                record.fwd_end_deg = val
            elif tag_upper == "R1032_R1033":
                record.rev_start_deg = val
            elif tag_upper == "R1034_R1035":
                record.rev_end_deg = val
            elif tag_upper == "R1036_R1037":
                record.stroke_length_1 = val
            elif tag_upper == "R1038_R1039":
                record.valve_fwd_start_deg_hmi = val
            elif tag_upper == "R1040_R1041":
                record.valve_end_start_deg_hmi = val
            elif tag_upper == "R1042_R1043":
                record.valve_rev_start_deg_hmi = val
            elif tag_upper == "R1044_R1045":
                record.valve_rev_end_deg_hmi = val
            elif tag_upper == "R1046_R1047":
                record.valve_stroke_length_1 = val
            else:
                logger.debug(f"Unknown tag {tag}, skipping.")

        return record

    @contextmanager
    def get_db_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(self.conn_str)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def write_to_db(self, records: List[PlcRecord]):
        """Write records to database using COPY command."""
        if not records:
            return

        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Prepare data for COPY
                    data_rows = []
                    for record in records:
                        row = (
                            record.timestamp,
                            record.temp_set_value_vert_front_1,
                            record.temp_set_value_vert_front_2,
                            record.temp_set_value_vert_front_3,
                            record.temp_set_value_vert_front_4,
                            record.temp_set_value_vert_front_5,
                            record.temp_set_value_vert_front_6,
                            record.temp_set_value_vert_front_7,
                            record.temp_set_value_vert_front_8,
                            record.temp_set_value_vert_front_9,
                            record.temp_set_value_vert_front_10,
                            record.temp_set_value_vert_front_11,
                            record.temp_set_value_vert_front_12,
                            record.temp_set_value_vert_front_13,
                            record.temp_set_value_vert_rear_1,
                            record.temp_set_value_vert_rear_2,
                            record.temp_set_value_vert_rear_3,
                            record.temp_set_value_vert_rear_4,
                            record.temp_set_value_vert_rear_5,
                            record.temp_set_value_vert_rear_6,
                            record.temp_set_value_vert_rear_7,
                            record.temp_set_value_vert_rear_8,
                            record.temp_set_value_vert_rear_9,
                            record.temp_set_value_vert_rear_10,
                            record.temp_set_value_vert_rear_11,
                            record.temp_set_value_vert_rear_12,
                            record.temp_set_value_vert_rear_13,
                            record.temp_set_value_hor_temp_27_sv_front,
                            record.temp_set_value_hor_temp_28_sv_rear,
                            record.temp_actual_value_vert_front_1,
                            record.temp_actual_value_vert_front_2,
                            record.temp_actual_value_vert_front_3,
                            record.temp_actual_value_vert_front_4,
                            record.temp_actual_value_vert_front_5,
                            record.temp_actual_value_vert_front_6,
                            record.temp_actual_value_vert_front_7,
                            record.temp_actual_value_vert_front_8,
                            record.temp_actual_value_vert_front_9,
                            record.temp_actual_value_vert_front_10,
                            record.temp_actual_value_vert_front_11,
                            record.temp_actual_value_vert_front_12,
                            record.temp_actual_value_vert_front_13,
                            record.temp_actual_value_vert_rear_1,
                            record.temp_actual_value_vert_rear_2,
                            record.temp_actual_value_vert_rear_3,
                            record.temp_actual_value_vert_rear_4,
                            record.temp_actual_value_vert_rear_5,
                            record.temp_actual_value_vert_rear_6,
                            record.temp_actual_value_vert_rear_7,
                            record.temp_actual_value_vert_rear_8,
                            record.temp_actual_value_vert_rear_9,
                            record.temp_actual_value_vert_rear_10,
                            record.temp_actual_value_vert_rear_11,
                            record.temp_actual_value_vert_rear_12,
                            record.temp_actual_value_vert_rear_13,
                            record.temp_actual_value_hor_temp_27_pv_front,
                            record.temp_actual_value_hor_temp_28_pv_rear,
                            record.hopper_level,
                            record.hopper_extreme_low_level,
                            record.hopper_low_level,
                            record.hopper_high_level,
                            record.shift_a,
                            record.shift_b,
                            record.shift_c,
                            record.actual_cld_count,
                            record.machine_speed,
                            record.puller_speed,
                            record.hori_sealer_fwd_start_deg_hmi,
                            record.hori_sealer_end_start_deg_hmi,
                            record.hori_sealer_rev_start_deg_hmi,
                            record.hori_sealer_rev_end_deg_hmi,
                            record.hori_sealer_stroke_1,
                            record.hori_sealer_stroke_2,
                            record.hs_stroke_2_start_deg,
                            record.vert_sealer_fwd_start_deg_hmi,
                            record.vert_sealer_end_start_deg_hmi,
                            record.vert_sealer_rev_start_deg_hmi,
                            record.vert_sealer_rev_end_deg_hmi,
                            record.vert_sealer_stroke_1,
                            record.vert_sealer_stroke_2,
                            record.vs_stroke_2_start_deg,
                            record.fwd_start_deg,
                            record.fwd_end_deg,
                            record.rev_start_deg,
                            record.rev_end_deg,
                            record.stroke_length_1,
                            record.valve_fwd_start_deg_hmi,
                            record.valve_end_start_deg_hmi,
                            record.valve_rev_start_deg_hmi,
                            record.valve_rev_end_deg_hmi,
                            record.valve_stroke_length_1,
                        )
                        data_rows.append(row)

                    # Use COPY for efficient bulk insert
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO "1s_Read_Data" (
                            timestamp, temp_set_value_vert_front_1, temp_set_value_vert_front_2,
                            temp_set_value_vert_front_3, temp_set_value_vert_front_4, temp_set_value_vert_front_5,
                            temp_set_value_vert_front_6, temp_set_value_vert_front_7, temp_set_value_vert_front_8,
                            temp_set_value_vert_front_9, temp_set_value_vert_front_10, temp_set_value_vert_front_11,
                            temp_set_value_vert_front_12, temp_set_value_vert_front_13, temp_set_value_vert_rear_1,
                            temp_set_value_vert_rear_2, temp_set_value_vert_rear_3, temp_set_value_vert_rear_4,
                            temp_set_value_vert_rear_5, temp_set_value_vert_rear_6, temp_set_value_vert_rear_7,
                            temp_set_value_vert_rear_8, temp_set_value_vert_rear_9, temp_set_value_vert_rear_10,
                            temp_set_value_vert_rear_11, temp_set_value_vert_rear_12, temp_set_value_vert_rear_13,
                            temp_set_value_hor_temp_27_sv_front, temp_set_value_hor_temp_28_sv_rear,
                            temp_actual_value_vert_front_1, temp_actual_value_vert_front_2, temp_actual_value_vert_front_3,
                            temp_actual_value_vert_front_4, temp_actual_value_vert_front_5, temp_actual_value_vert_front_6,
                            temp_actual_value_vert_front_7, temp_actual_value_vert_front_8, temp_actual_value_vert_front_9,
                            temp_actual_value_vert_front_10, temp_actual_value_vert_front_11, temp_actual_value_vert_front_12,
                            temp_actual_value_vert_front_13, temp_actual_value_vert_rear_1, temp_actual_value_vert_rear_2,
                            temp_actual_value_vert_rear_3, temp_actual_value_vert_rear_4, temp_actual_value_vert_rear_5,
                            temp_actual_value_vert_rear_6, temp_actual_value_vert_rear_7, temp_actual_value_vert_rear_8,
                            temp_actual_value_vert_rear_9, temp_actual_value_vert_rear_10, temp_actual_value_vert_rear_11,
                            temp_actual_value_vert_rear_12, temp_actual_value_vert_rear_13, temp_actual_value_hor_temp_27_pv_front,
                            temp_actual_value_hor_temp_28_pv_rear, hopper_level, hopper_extreme_low_level, hopper_low_level,
                            hopper_high_level, shift_a, shift_b, shift_c, actual_cld_count, machine_speed, puller_speed,
                            hori_sealer_fwd_start_deg_hmi, hori_sealer_end_start_deg_hmi, hori_sealer_rev_start_deg_hmi,
                            hori_sealer_rev_end_deg_hmi, hori_sealer_stroke_1, hori_sealer_stroke_2, hs_stroke_2_start_deg,
                            vert_sealer_fwd_start_deg_hmi, vert_sealer_end_start_deg_hmi, vert_sealer_rev_start_deg_hmi,
                            vert_sealer_rev_end_deg_hmi, vert_sealer_stroke_1, vert_sealer_stroke_2, vs_stroke_2_start_deg,
                            fwd_start_deg, fwd_end_deg, rev_start_deg, rev_end_deg, stroke_length_1,
                            valve_fwd_start_deg_hmi, valve_end_start_deg_hmi, valve_rev_start_deg_hmi,
                            valve_rev_end_deg_hmi, valve_stroke_length_1
                        ) VALUES %s""",
                        data_rows,
                    )
                    conn.commit()
                    logger.debug(f"Inserted {len(records)} records to database")

        except Exception as e:
            logger.error(f"❌ DB insert error: {e}")
            raise

    async def write_insert_log(
        self,
        db_start: datetime,
        db_end: datetime,
        plc_read_start: datetime,
        plc_read_end: datetime,
        record_count: int,
        error_message: Optional[str],
        first_record_time: datetime,
        last_record_time: datetime,
    ):
        """Write performance log to database."""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO "1s_Read_Data_Logs"
                        (start_time_db, end_time_db, db_storage_time, plc_read_start, 
                         plc_read_end, plc_read_time, record_count, error_message, 
                         first_record_time, last_record_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                        (
                            db_start,
                            db_end,
                            int((db_end - db_start).total_seconds() * 1000),
                            plc_read_start,
                            plc_read_end,
                            int((plc_read_end - plc_read_start).total_seconds() * 1000),
                            record_count,
                            error_message,
                            first_record_time,
                            last_record_time,
                        ),
                    )
                    conn.commit()
        except Exception as e:
            logger.error(f"❌ Error inserting read log: {e}")

    async def read_plc_loop(self):
        """Main PLC reading loop."""
        i_count = 0
        first_record_time = datetime.min
        last_record_time = datetime.min
        icnt = 0

        logger.info("Starting PLC read loop")

        while True:
            try:
                if not self.is_connected():
                    logger.error("❌ PLC disconnected. Exiting.")
                    break

                plc_read_start = datetime.now()
                values = self.read_registers_in_batches()
                plc_read_end = datetime.now()

                record = self.map_values_to_record(values)

                if icnt == 0:
                    first_record_time = record.timestamp
                elif icnt == 9:
                    last_record_time = record.timestamp

                icnt += 1

                # Add to appropriate buffer
                buffer = self.buffer_a if self.using_buffer_a else self.buffer_b
                buffer.append(record)

                # Process buffer when it reaches 10 records
                if len(buffer) >= 10:
                    icnt = 0
                    to_write = buffer.copy()
                    buffer.clear()

                    db_start_time = datetime.now()
                    write_error = None

                    try:
                        await asyncio.get_event_loop().run_in_executor(
                            None, self.write_to_db, to_write
                        )
                    except Exception as ex:
                        write_error = ex
                        logger.error(f"❌ Exception writing to DB: {ex}")

                    db_end_time = datetime.now()

                    await self.write_insert_log(
                        db_start_time,
                        db_end_time,
                        plc_read_start,
                        plc_read_end,
                        len(to_write),
                        str(write_error) if write_error else None,
                        first_record_time,
                        last_record_time,
                    )

                    # Switch buffer
                    self.using_buffer_a = not self.using_buffer_a

                i_count += 1
                await asyncio.sleep(1.0)  # 1 second interval

            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(1.0)

    async def run(self):
        """Main service entry point."""
        logger.info("Service Start: Initializing...")

        if not self.connect_tcp():
            logger.error("❌ Initial connection failed. Exiting.")
            return

        self.buffer_start_time = datetime.now()
        await self.read_plc_loop()

    def __del__(self):
        """Cleanup resources."""
        if self.socket:
            self.socket.close()


class PlcMain:
    """Main entry point class."""

    @staticmethod
    def run():
        """Run the PLC service."""
        service = PLCService()
        asyncio.run(service.run())


def main():
    """Main function."""
    try:
        PlcMain.run()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Service error: {e}")


if __name__ == "__main__":
    main()
