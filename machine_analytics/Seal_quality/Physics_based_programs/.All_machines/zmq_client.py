# server1_zmq_client.py
import asyncio
import json
import os
import signal
import logging
import zmq
import zmq.asyncio
from datetime import datetime
from pathlib import Path

# ---------- Configuration ----------
ZMQ_SERVER2_ADDRESS = "tcp://192.168.1.168:8030"  # ZMQ subscriber address
BASE_DIR = Path(__file__).parent
CONFIG_DIR = BASE_DIR           # keep configs in ./config_mc
CONFIG_DIR.mkdir(parents=True, exist_ok=True)

# ZMQ settings
ZMQ_RECEIVE_TIMEOUT = 1.0   # seconds
ZMQ_RECONNECT_INTERVAL = 5.0  # seconds

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("zmq-config-client")

# ---------- Helpers ----------
def atomic_write_json(file_path: Path, data: dict) -> None:
    """
    Write JSON atomically: write to temp file and os.replace.
    Ensures fsync to reduce corruption risk.
    """
    tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, file_path)  # atomic on POSIX


# ---------- ZMQ Subscriber Task ----------
async def zmq_subscriber_task(stop_event: asyncio.Event):
    """ZMQ subscriber task to receive config updates from Server 2."""
    context = zmq.asyncio.Context()
    socket = None

    try:
        socket = context.socket(zmq.SUB)
        socket.connect(ZMQ_SERVER2_ADDRESS)
        socket.setsockopt(zmq.SUBSCRIBE, b"")  # Subscribe to all messages

        logger.info("🔌 Connected to ZMQ Server at %s", ZMQ_SERVER2_ADDRESS)

        while not stop_event.is_set():
            try:
                # Non-blocking receive with timeout
                message = await asyncio.wait_for(
                    socket.recv_string(zmq.NOBLOCK),
                    timeout=1.0
                )

                logger.info("📥 ZMQ: Received message: %s", message)

                # Process the ZMQ message
                try:
                    data = json.loads(message)
                    msg_type = data.get("type")
                    machine_id = data.get("machine_id")
                    config = data.get("updated_config")

                    logger.info("📦 ZMQ: Parsed message type=%s machine_id=%s", msg_type, machine_id)

                    # Process config updates from ZMQ
                    if msg_type in ("config_updated", "machine_config_updated") and machine_id and config:
                        file_name = f"config_mc{machine_id}.json"
                        file_path = CONFIG_DIR / file_name

                        try:
                            logger.info("💾 ZMQ: Saving config for machine %s -> %s", machine_id, file_path)
                            atomic_write_json(file_path, config)
                            logger.info("✅ ZMQ: Updated %s successfully", file_name)

                        except Exception as e:
                            logger.error("❌ ZMQ: Failed to save config for machine %s: %s", machine_id, e)

                    else:
                        logger.warning("⚠️ ZMQ: Ignored message type: %s", msg_type)

                except json.JSONDecodeError:
                    logger.warning("⚠️ ZMQ: Received non-JSON message: %s", message)

            except asyncio.TimeoutError:
                # Timeout is expected - continue loop
                continue
            except zmq.Again:
                # No message available - continue loop
                continue
            except Exception as e:
                logger.error("❌ ZMQ: Error receiving message: %s", e)
                await asyncio.sleep(1)

    except Exception as e:
        logger.error("❌ ZMQ: Subscriber error: %s", e)
    finally:
        if socket:
            socket.close()
        if context:
            context.term()
        logger.info("🔌 ZMQ: Subscriber disconnected")


# ---------- Main Client Runner ----------
async def run_client(stop_event: asyncio.Event):
    """Run ZMQ subscriber to receive config updates from Server 2."""
    logger.info("🚀 Starting Server 1 ZMQ client")

    while not stop_event.is_set():
        try:
            # Start ZMQ subscriber
            await zmq_subscriber_task(stop_event)
        except Exception as e:
            logger.error("❌ ZMQ client error: %s", e)
            if not stop_event.is_set():
                logger.info("🔄 Reconnecting to ZMQ in %.1f seconds", ZMQ_RECONNECT_INTERVAL)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=ZMQ_RECONNECT_INTERVAL)
                    break  # stop_event set during wait
                except asyncio.TimeoutError:
                    continue  # timeout, loop tries to reconnect

    logger.info("🛑 ZMQ client stopped")


# ---------- Graceful Shutdown ----------
def _install_signal_handlers(loop, stop_event: asyncio.Event):
    for sig in (signal.SIGINT, signal.SIGTERM): 
        loop.add_signal_handler(sig, stop_event.set)


# ---------- Entrypoint ----------
def main():
    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()
    _install_signal_handlers(loop, stop_event)
    try:
        loop.run_until_complete(run_client(stop_event))
    finally:
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
