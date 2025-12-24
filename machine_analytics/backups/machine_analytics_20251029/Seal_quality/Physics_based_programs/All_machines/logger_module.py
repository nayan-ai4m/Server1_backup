import csv
import os
from datetime import datetime, timedelta
import threading

# Base log directory
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Thread lock for file operations
log_lock = threading.Lock()

# Machine list for validation
VALID_MACHINES = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']

def get_log_file_path(machine_id):
    """Get the log file path for a specific machine"""
    if machine_id not in VALID_MACHINES:
        raise ValueError(f"Invalid machine_id: {machine_id}. Valid machines: {VALID_MACHINES}")
    
    return os.path.join(LOG_DIR, f"monitoring_log_{machine_id}.csv")

def log_event(message, timestamp, machine_id):
    """
    Append log entry with machine timestamp (+5:30 hrs) to machine-specific CSV.
    
    Args:
        message (str): Log message to write
        timestamp (datetime or str): Machine timestamp in "%Y-%m-%d %H:%M:%S" format
        machine_id (str): Machine identifier (mc17, mc18, etc.)
    """
    try:
        # Validate machine_id
        if machine_id not in VALID_MACHINES:
            print(f"[ERROR] Invalid machine_id: {machine_id}. Valid machines: {VALID_MACHINES}")
            return False
        
        # Ensure timestamp is datetime
        if isinstance(timestamp, str):
            timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        elif not isinstance(timestamp, datetime):
            print(f"[ERROR] Invalid timestamp type for {machine_id}: {type(timestamp)}")
            return False

        # Convert machine time to IST (+5h30m)
        ist_timestamp = timestamp + timedelta(hours=5, minutes=30)
        ts_str = ist_timestamp.strftime("%Y-%m-%d %H:%M:%S")

        log_file = get_log_file_path(machine_id)
        file_exists = os.path.isfile(log_file)

        # Use thread lock to prevent concurrent file access
        with log_lock:
            with open(log_file, mode="a", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                
                # Write header if file is new
                if not file_exists:
                    writer.writerow(["Timestamp (IST)", "Machine ID", "Comment"])
                
                # Write log entry
                writer.writerow([ts_str, machine_id.upper(), message])

        # Console output with machine identification
        print(f"[LOGGED {machine_id.upper()}] {ts_str} - {message}")
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to log event for {machine_id}: {e}")
        return False

def log_event_now(message, machine_id):
    """
    Log event with current timestamp
    
    Args:
        message (str): Log message to write
        machine_id (str): Machine identifier (mc17, mc18, etc.)
    """
    current_time = datetime.now()
    return log_event(message, current_time, machine_id)

def log_system_event(message, machine_id=None):
    """
    Log system-wide events (can be machine-specific or general)
    
    Args:
        message (str): Log message to write
        machine_id (str, optional): Machine identifier. If None, logs to all machines
    """
    current_time = datetime.now()
    success_count = 0
    
    if machine_id:
        # Log to specific machine
        return log_event(f"[SYSTEM] {message}", current_time, machine_id)
    else:
        # Log to all machines
        for mid in VALID_MACHINES:
            if log_event(f"[SYSTEM] {message}", current_time, mid):
                success_count += 1
        
        return success_count == len(VALID_MACHINES)

def read_recent_logs(machine_id, num_entries=10):
    """
    Read recent log entries for a specific machine
    
    Args:
        machine_id (str): Machine identifier
        num_entries (int): Number of recent entries to return
        
    Returns:
        list: List of dictionaries with log entries, or empty list if error
    """
    try:
        if machine_id not in VALID_MACHINES:
            print(f"[ERROR] Invalid machine_id: {machine_id}")
            return []
        
        log_file = get_log_file_path(machine_id)
        
        if not os.path.exists(log_file):
            print(f"[INFO] Log file does not exist for {machine_id}")
            return []
        
        entries = []
        with open(log_file, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            entries = list(reader)
        
        # Return last num_entries
        return entries[-num_entries:] if len(entries) > num_entries else entries
        
    except Exception as e:
        print(f"[ERROR] Failed to read logs for {machine_id}: {e}")
        return []

def get_log_stats(machine_id):
    """
    Get statistics about log file for a specific machine
    
    Args:
        machine_id (str): Machine identifier
        
    Returns:
        dict: Statistics about the log file
    """
    try:
        if machine_id not in VALID_MACHINES:
            return {"error": f"Invalid machine_id: {machine_id}"}
        
        log_file = get_log_file_path(machine_id)
        
        if not os.path.exists(log_file):
            return {
                "machine_id": machine_id,
                "file_exists": False,
                "total_entries": 0,
                "file_size": 0
            }
        
        # Get file size
        file_size = os.path.getsize(log_file)
        
        # Count entries
        entry_count = 0
        first_entry = None
        last_entry = None
        
        with open(log_file, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            entries = list(reader)
            entry_count = len(entries)
            
            if entries:
                first_entry = entries[0]["Timestamp (IST)"]
                last_entry = entries[-1]["Timestamp (IST)"]
        
        return {
            "machine_id": machine_id,
            "file_exists": True,
            "total_entries": entry_count,
            "file_size": file_size,
            "first_entry": first_entry,
            "last_entry": last_entry,
            "file_path": log_file
        }
        
    except Exception as e:
        return {"error": f"Failed to get stats for {machine_id}: {e}"}

def get_all_log_stats():
    """
    Get log statistics for all machines
    
    Returns:
        dict: Statistics for all machines
    """
    stats = {}
    for machine_id in VALID_MACHINES:
        stats[machine_id] = get_log_stats(machine_id)
    return stats

def clear_logs(machine_id):
    """
    Clear log file for specific machine (USE WITH CAUTION)
    
    Args:
        machine_id (str): Machine identifier
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if machine_id not in VALID_MACHINES:
            print(f"[ERROR] Invalid machine_id: {machine_id}")
            return False
        
        log_file = get_log_file_path(machine_id)
        
        with log_lock:
            if os.path.exists(log_file):
                os.remove(log_file)
                print(f"[INFO] Log file cleared for {machine_id}")
            else:
                print(f"[INFO] Log file does not exist for {machine_id}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to clear logs for {machine_id}: {e}")
        return False

def backup_logs(machine_id, backup_suffix=None):
    """
    Create backup of log file for specific machine
    
    Args:
        machine_id (str): Machine identifier
        backup_suffix (str, optional): Suffix for backup file. If None, uses timestamp
        
    Returns:
        str: Backup file path if successful, None otherwise
    """
    try:
        if machine_id not in VALID_MACHINES:
            print(f"[ERROR] Invalid machine_id: {machine_id}")
            return None
        
        log_file = get_log_file_path(machine_id)
        
        if not os.path.exists(log_file):
            print(f"[INFO] No log file to backup for {machine_id}")
            return None
        
        # Generate backup filename
        if backup_suffix is None:
            backup_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        backup_file = os.path.join(LOG_DIR, f"monitoring_log_{machine_id}_backup_{backup_suffix}.csv")
        
        # Copy file
        import shutil
        with log_lock:
            shutil.copy2(log_file, backup_file)
        
        print(f"[INFO] Log backup created for {machine_id}: {backup_file}")
        return backup_file
        
    except Exception as e:
        print(f"[ERROR] Failed to backup logs for {machine_id}: {e}")
        return None

def initialize_all_log_files():
    """
    Initialize log files for all machines (create directories and files if needed)
    
    Returns:
        dict: Status for each machine
    """
    results = {}
    
    for machine_id in VALID_MACHINES:
        try:
            log_file = get_log_file_path(machine_id)
            
            if not os.path.exists(log_file):
                # Create empty log file with header
                with open(log_file, mode="w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    writer.writerow(["Timestamp (IST)", "Machine ID", "Comment"])
                
                results[machine_id] = "Created"
            else:
                results[machine_id] = "Already exists"
                
        except Exception as e:
            results[machine_id] = f"Error: {e}"
    
    return results

# ===== Utility Functions for Monitoring =====
def log_machine_start(machine_id, timestamp):
    """Log machine start event"""
    return log_event("Machine started", timestamp, machine_id)

def log_machine_stop(machine_id, timestamp):
    """Log machine stop event"""
    return log_event("Machine stopped", timestamp, machine_id)

def log_temperature_adjustment(machine_id, timestamp, suggested_temp, reason=""):
    """Log temperature adjustment suggestion"""
    message = f"Temperature adjustment suggested: {suggested_temp:.2f}°C"
    if reason:
        message += f" - {reason}"
    return log_event(message, timestamp, machine_id)

def log_pressure_adjustment(machine_id, timestamp, suggested_s1, suggested_s2, reason=""):
    """Log pressure/stroke adjustment suggestion"""
    message = f"Stroke adjustment suggested: S1={suggested_s1:.3f}, S2={suggested_s2:.3f}"
    if reason:
        message += f" - {reason}"
    return log_event(message, timestamp, machine_id)

def log_config_change(machine_id, timestamp, changed_params):
    """Log configuration parameter changes"""
    message = f"Configuration changed: {changed_params}"
    return log_event(message, timestamp, machine_id)

# ===== Main execution (for testing) =====
if __name__ == "__main__":
    print("Testing Multi-Machine Logger Module")
    print("=" * 50)
    
    # Initialize all log files
    print("1. Initializing log files...")
    init_results = initialize_all_log_files()
    for machine, status in init_results.items():
        print(f"   {machine}: {status}")
    
    # Test logging to each machine
    print("\n2. Testing log entries...")
    test_machines = ['mc17', 'mc19', 'mc21']
    for machine in test_machines:
        success = log_event_now(f"Test message for {machine}", machine)
        print(f"   {machine}: {'Success' if success else 'Failed'}")
    
    # Test system event logging
    print("\n3. Testing system event logging...")
    log_system_event("Multi-machine monitoring system started")
    
    # Get statistics
    print("\n4. Log file statistics:")
    stats = get_all_log_stats()
    for machine, stat in stats.items():
        if 'error' not in stat and stat['file_exists']:
            print(f"   {machine}: {stat['total_entries']} entries, {stat['file_size']} bytes")
        elif 'error' in stat:
            print(f"   {machine}: Error - {stat['error']}")
        else:
            print(f"   {machine}: No log file")
    
    print("\nLogger module test completed!")