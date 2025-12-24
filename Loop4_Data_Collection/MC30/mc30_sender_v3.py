"""
NATS Sender Script for Server System
Orchestrates PLC parameter sweep via NATS messaging to AdvTech receiver
Performs automated S1/S2 stroke optimization with database monitoring
"""

import asyncio
import json
import nats
import time
import sys
import csv
import os
from datetime import datetime
import psycopg2
from psycopg2 import OperationalError

# ============================ CONFIGURATION LOADER ============================

def load_config():
    """Load configuration from config_sender.json"""
    config_path = os.path.join(os.path.dirname(__file__), "config_sender.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, 'r') as f:
        return json.load(f)

# Load configuration
CONFIG = load_config()

# NATS Configuration
NATS_SERVER = CONFIG["nats"]["server"]
NATS_TOPIC = CONFIG["nats"]["topic"]
NATS_TIMEOUT = CONFIG["nats"]["timeout"]

# PLC Configuration
PLC_ID = CONFIG["plc"]["id"]

# Database Configuration
READ_DB_CONFIG = CONFIG["database"]

# Sweep Parameters
TAG_S1_NAME = CONFIG["plc_tags"]["s1_tag_name"]
TAG_S2_NAME = CONFIG["plc_tags"]["s2_tag_name"]
TAG_S1 = CONFIG["plc_tags"]["s1_register"]
TAG_S2 = CONFIG["plc_tags"]["s2_register"]
M82_TRIGGER = CONFIG["plc_tags"]["m82_trigger"]

S1_START = CONFIG["sweep_parameters"]["s1_start"]
S1_END = CONFIG["sweep_parameters"]["s1_end"]
S1_STEP = CONFIG["sweep_parameters"]["s1_step"]

S2_START = CONFIG["sweep_parameters"]["s2_start"]
S2_END = CONFIG["sweep_parameters"]["s2_end"]
S2_STEP = CONFIG["sweep_parameters"]["s2_step"]

STABILIZATION_TIME = CONFIG["timing"]["stabilization_time"]
STATUS_POLL_INTERVAL = CONFIG["timing"]["status_poll_interval"]
RECOVERY_WAIT_TIME = CONFIG["timing"]["recovery_wait_time"]

# CSV Configuration
CSV_FILENAME = CONFIG["csv"]["filename"]
CSV_HEADER = CONFIG["csv"]["header"]

# ============================ NATS CLIENT WRAPPER ============================

class PLCNatsClient:
    """NATS client wrapper for PLC operations compatible with mc27_mc30_plc_write.py"""

    def __init__(self, nats_connection):
        self.nc = nats_connection

    async def write_tag(self, tag_name, value, timeout=NATS_TIMEOUT):
        """
        Write a value to PLC via NATS using production receiver format.
        M82 is triggered automatically by the receiver after each D register write.

        Args:
            tag_name: Tag name in database (e.g., "s1_stroke")
            value: Value to write
            timeout: Response timeout in seconds

        Returns:
            dict: Response from receiver, or error dict
        """
        message = {
            "plc": PLC_ID,
            "name": tag_name,
            "command": "update",
            "value": value,
            "status": True
        }

        try:
            print(f"Sending: {message}")
            response = await self.nc.request(
                NATS_TOPIC,
                json.dumps(message).encode(),
                timeout=timeout
            )

            response_data = json.loads(response.data.decode())
            print(f"Response: {response_data}")

            return response_data

        except asyncio.TimeoutError:
            print(f"ERROR: Timeout waiting for response: tag={tag_name}")
            return {"ack": False, "error": "NATS request timeout"}
        except Exception as e:
            print(f"ERROR: NATS command error: {e}")
            return {"ack": False, "error": str(e)}

    async def write_s1(self, value):
        """Write S1 stroke value (M82 triggered automatically by receiver)"""
        response = await self.write_tag(TAG_S1_NAME, value)
        if response.get("ack"):
            m82_status = "✓ M82 triggered" if response.get("m82_triggered") else "✗ M82 failed"
            print(f"✓ Wrote {TAG_S1_NAME} (D{TAG_S1}) = {value:.4f} [{m82_status}]")
            return True
        else:
            print(f"ERROR: Failed to write {TAG_S1_NAME}: {response.get('error')}")
            return False

    async def write_s2(self, value):
        """Write S2 stroke value (M82 triggered automatically by receiver)"""
        response = await self.write_tag(TAG_S2_NAME, value)
        if response.get("ack"):
            m82_status = "✓ M82 triggered" if response.get("m82_triggered") else "✗ M82 failed"
            print(f"✓ Wrote {TAG_S2_NAME} (D{TAG_S2}) = {value:.4f} [{m82_status}]")
            return True
        else:
            print(f"ERROR: Failed to write {TAG_S2_NAME}: {response.get('error')}")
            return False

# ============================ DATABASE FUNCTIONS ============================

def connect_db():
    """Connect to PostgreSQL database with retry"""
    max_retries = 3
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**READ_DB_CONFIG)
            print(f"✓ Connected to database at {READ_DB_CONFIG['host']}")
            return conn
        except OperationalError as e:
            print(f"WARNING: Database connection attempt {i+1}/{max_retries} failed: {e}")
            if i < max_retries - 1:
                time.sleep(2)
    raise ConnectionError("Failed to connect to database")

def get_machine_status(db_conn):
    """Get current machine status from database"""
    try:
        cursor = db_conn.cursor()
        query = """
            SELECT timestamp, status
            FROM mc30_fast
            ORDER BY timestamp DESC
            LIMIT 1;
        """
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()

        if result:
            timestamp, status = result
            return status, timestamp
        return None, None
    except Exception as e:
        print(f"ERROR: Failed to get machine status: {e}")
        return None, None

def get_average_instant_load(db_conn):
    """Get average horizontal instant load from recent data"""
    try:
        cursor = db_conn.cursor()
        query = """
            SELECT timestamp, horizontal_instant_load
            FROM mc30_fast
            WHERE cam_position BETWEEN 170 AND 210
            ORDER BY timestamp DESC
            LIMIT 30;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            loads = [row[1] for row in results if row[1] is not None]
            if loads:
                avg_load = sum(loads) / len(loads)
                print(f"Calculated average load: {avg_load:.2f} from {len(loads)} samples")
                return avg_load

        print("WARNING: No load data available")
        return None
    except Exception as e:
        print(f"ERROR: Failed to get average load: {e}")
        return None

# ============================ HELPER FUNCTIONS ============================

class MachineStoppedException(Exception):
    """Exception raised when machine stops during operation"""
    pass

def frange(start, end, step):
    """Generate float range including endpoint"""
    values = []
    current = start
    if step > 0:
        while current <= end:
            values.append(round(current, 2))
            current += step
    else:
        while current >= end:
            values.append(round(current, 2))
            current += step
    return values

def create_csv_if_not_exists():
    """Create CSV file with header if it doesn't exist"""
    if not os.path.exists(CSV_FILENAME):
        with open(CSV_FILENAME, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)
        print(f"✓ Created CSV file: {CSV_FILENAME}")
    else:
        print(f"CSV file already exists: {CSV_FILENAME}")

def append_to_csv(cycle_id, s1, s2, avg_load):
    """Append a row to the CSV file"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(CSV_FILENAME, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([cycle_id, s1, s2, avg_load, timestamp])
        print(
            f"CSV: Cycle {cycle_id} | S1={s1} | S2={s2} | Avg Load={avg_load:.2f}"
            if isinstance(avg_load, (int, float))
            else f"CSV: Cycle {cycle_id} | S1={s1} | S2={s2} | Avg Load={avg_load}"
        )
    except Exception as e:
        print(f"ERROR: Failed to write to CSV: {e}")

async def wait_for_ready_status(plc_client, db_conn, initial_s1, initial_s2):
    """Wait until machine status becomes 1 (ready)"""
    print("Checking machine status...")

    while True:
        status, timestamp = get_machine_status(db_conn)

        if status is None:
            print("WARNING: Could not read machine status, retrying...")
            await asyncio.sleep(STATUS_POLL_INTERVAL)
            continue

        if status == 1:
            print(f"✓ Machine is READY (status={status})")
            return True
        else:
            print(f"WARNING: Machine is NOT READY (status={status}). Writing initial values and waiting...")

            # Write initial values (M82 triggered after each write by receiver)
            await plc_client.write_s1(initial_s1)
            await asyncio.sleep(0.1)
            await plc_client.write_s2(initial_s2)
            print(f"Wrote initial values: S1={initial_s1}, S2={initial_s2}")

            # Wait for machine to start
            print(f"Waiting {RECOVERY_WAIT_TIME} seconds for machine to recover...")
            await asyncio.sleep(RECOVERY_WAIT_TIME)

            # Check status again
            status, timestamp = get_machine_status(db_conn)
            if status == 1:
                print("✓ Machine has recovered and is READY")
                return True
            else:
                print("Machine still not ready, continuing to wait...")
                await asyncio.sleep(STATUS_POLL_INTERVAL)

async def check_machine_running(db_conn):
    """Check if machine is running and raise exception if stopped"""
    status, _ = get_machine_status(db_conn)

    if status is None:
        print("WARNING: Could not read machine status during monitoring")
        return

    if status != 1:
        print(f"❌ MACHINE STOPPED! Status changed to {status}")
        raise MachineStoppedException(f"Machine stopped with status={status}")

# ============================ MAIN PROGRAM ============================

async def main():
    """Main orchestration function"""
    print("\n" + "="*80)
    print("PLC PARAMETER SWEEP - MC27 Stroke & Load Optimization (NATS Version)")
    print("="*80)
    print(f"NATS Server: {NATS_SERVER}")
    print(f"NATS Topic: {NATS_TOPIC}")
    print(f"Database: {READ_DB_CONFIG['host']}:{READ_DB_CONFIG['port']}")
    print(f"Output CSV: {CSV_FILENAME}")
    print("="*80)

    nc = None
    db_conn = None
    initial_s1 = None
    initial_s2 = None

    try:
        # Step 1: Connect to NATS
        print("Step 1: Connecting to NATS server...")
        nc = await nats.connect(NATS_SERVER)
        print("✓ Connected to NATS")

        plc_client = PLCNatsClient(nc)

        # Step 2: Connect to Database
        print("Step 2: Connecting to Database...")
        db_conn = connect_db()

        # Step 3: Set fallback values (production receiver doesn't support READ)
        print("Step 3: Using fallback values for machine recovery...")
        initial_s1 = S1_START
        initial_s2 = S2_START
        print(f"Fallback values: S1={initial_s1:.2f}, S2={initial_s2:.2f}")
        print("NOTE: These values will be used if machine is not ready")

        # Step 4: Prepare CSV
        print("Step 4: Preparing CSV file...")
        create_csv_if_not_exists()

        # Step 5: Create parameter list
        print("Step 5: Generating parameter combinations...")
        s1_values = frange(S1_START, S1_END, S1_STEP)
        s2_values = frange(S2_START, S2_END, S2_STEP)

        parameter_pairs = []
        for s1 in s1_values:
            for s2 in s2_values:
                parameter_pairs.append((s1, s2))

        total_combinations = len(parameter_pairs)
        print(f"✓ Total combinations to test: {total_combinations}")
        print(f"  S1 range: {S1_START} to {S1_END} step {S1_STEP} ({len(s1_values)} values)")
        print(f"  S2 range: {S2_START} to {S2_END} step {S2_STEP} ({len(s2_values)} values)")

        print("\n" + "-"*80)
        input("Press Enter to start the parameter sweep...")
        print("-"*80 + "\n")

        # Step 6: Parameter sweep loop
        cycle_id = 0

        for idx, (s1, s2) in enumerate(parameter_pairs):
            print(f"\n{'='*60}")
            print(f"Combination {idx+1}/{total_combinations}: S1={s1}, S2={s2}")
            print(f"{'='*60}")

            # Wait for machine to be ready
            await wait_for_ready_status(plc_client, db_conn, initial_s1, initial_s2)

            # Write new stroke values to PLC with retry
            # M82 is triggered automatically by the receiver after each D register write
            write_success = False
            while not write_success:
                print(f"Writing to PLC: S1={s1}, S2={s2}")

                # Write S1 (M82 triggered by receiver)
                success_s1 = await plc_client.write_s1(s1)

                if not success_s1:
                    print("❌ Failed to write S1, retrying...")
                    await asyncio.sleep(2)
                    continue

                # Small delay between writes
                await asyncio.sleep(0.1)

                # Write S2 (M82 triggered by receiver)
                success_s2 = await plc_client.write_s2(s2)

                if success_s1 and success_s2:
                    print(f"✓ Successfully wrote S1={s1}, S2={s2}")
                    print(f"✓ M{M82_TRIGGER} triggered after each write")
                    write_success = True
                else:
                    print("❌ Failed to write S2, retrying...")
                    await asyncio.sleep(2)

            # Stabilization time with machine status monitoring
            print(f"Waiting {STABILIZATION_TIME} seconds for stabilization...")
            await asyncio.sleep(STABILIZATION_TIME)

            # Check if machine is still running after stabilization
            await check_machine_running(db_conn)

            # Read and record data for CURRENT combination
            cycle_id += 1
            print(f"Reading data for CURRENT combination: S1={s1}, S2={s2}")

            avg_load = get_average_instant_load(db_conn)

            if avg_load is not None:
                append_to_csv(cycle_id, s1, s2, avg_load)
            else:
                print("WARNING: Could not get average load, writing NULL to CSV")
                append_to_csv(cycle_id, s1, s2, "NULL")

        print("\n" + "="*80)
        print("✓ PARAMETER SWEEP COMPLETED SUCCESSFULLY!")
        print(f"✓ Total cycles recorded: {cycle_id}")
        print(f"✓ Results saved to: {CSV_FILENAME}")
        print("="*80)

    except KeyboardInterrupt:
        print("\n\n⚠️  Program interrupted by user (Ctrl+C)")
    except MachineStoppedException as e:
        print(f"\n\n❌ MACHINE STOPPED DURING OPERATION!")
        print(f"❌ Reason: {e}")
        print("❌ Aborting parameter sweep...")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Step 8: Restore initial values and cleanup
        print("\nRestoring fallback PLC values...")
        if nc and initial_s1 is not None and initial_s2 is not None:
            try:
                plc_client = PLCNatsClient(nc)
                # Restore values (M82 triggered after each write by receiver)
                await plc_client.write_s1(initial_s1)
                await asyncio.sleep(0.1)
                await plc_client.write_s2(initial_s2)
                print(f"✓ Restored: S1={initial_s1:.2f}, S2={initial_s2:.2f}")
                print(f"✓ M{M82_TRIGGER} triggered after each restore")
            except Exception as e:
                print(f"ERROR: Failed to restore fallback values: {e}")

        if nc:
            try:
                await nc.drain()
                print("✓ NATS connection closed")
            except:
                pass

        if db_conn:
            try:
                db_conn.close()
                print("✓ Database connection closed")
            except:
                pass

# ============================ ENTRY POINT ============================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)
