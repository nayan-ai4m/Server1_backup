#!/usr/bin/env python3
"""
Daily Log Cleanup – systemd version
Deletes all YYYY-MM-DD folders in /home/ai4m/develop/ui_backend/logs/ except today.
"""

import os
import shutil
import logging
from datetime import datetime
from pathlib import Path

LOG_DIR = Path("/home/ai4m/develop/ui_backend/logs")
TODAY_STR = datetime.now().strftime("%Y-%m-%d")

def setup_logging():
    logger = logging.getLogger("LogCleanup")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(LOG_DIR / "cleanup.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger

def main():
    log = setup_logging()
    log.info("=== Daily log cleanup started ===")

    if not LOG_DIR.exists():
        log.error(f"Log directory missing: {LOG_DIR}")
        return

    deleted = 0
    kept = 0

    for item in LOG_DIR.iterdir():
        if not item.is_dir():
            continue
        name = item.name

        # Only process YYYY-MM-DD folders
        if not (len(name) == 10 and name[4] == name[7] == '-'):
            continue

        try:
            folder_date = datetime.strptime(name, "%Y-%m-%d").date()
        except ValueError:
            continue

        if name == TODAY_STR:
            log.info(f"KEEP → {name}")
            kept += 1
        else:
            log.info(f"DELETE → {name}")
            try:
                shutil.rmtree(item)
                deleted += 1
            except Exception as e:
                log.error(f"Failed to delete {name}: {e}")

    log.info(f"=== Cleanup complete | Kept: {kept} | Deleted: {deleted} ===")

if __name__ == "__main__":
    main()
