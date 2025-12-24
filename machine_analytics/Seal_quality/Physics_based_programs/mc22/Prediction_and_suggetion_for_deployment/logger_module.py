import csv
import os
from datetime import datetime, timedelta

LOG_FILE = "logs/monitoring_log.csv"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def log_event(message, timestamp):
    """
    Append log entry with machine timestamp (+5:30 hrs) to CSV.
    `timestamp` must be a datetime object or string in "%Y-%m-%d %H:%M:%S" format.
    """
    # Ensure timestamp is datetime
    if isinstance(timestamp, str):
        timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

    # Convert machine time to IST (+5h30m)
    ist_timestamp = timestamp + timedelta(hours=5, minutes=30)
    ts_str = ist_timestamp.strftime("%Y-%m-%d %H:%M:%S")

    file_exists = os.path.isfile(LOG_FILE)

    with open(LOG_FILE, mode="a", newline="") as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["Timestamp (IST)", "Comment"])
        writer.writerow([ts_str, message])

    print(f"[LOGGED] {ts_str} - {message}")



# import csv
# import os
# from datetime import datetime

# LOG_FILE = "logs/monitoring_log.csv"
# os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# def log_event(message):
#     """Append timestamped log entry to CSV."""
#     timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     file_exists = os.path.isfile(LOG_FILE)

#     with open(LOG_FILE, mode="a", newline="") as file:
#         writer = csv.writer(file)
#         if not file_exists:
#             writer.writerow(["Timestamp", "Comment"])
#         writer.writerow([timestamp, message])

#     print(f"[LOGGED] {timestamp} - {message}")
