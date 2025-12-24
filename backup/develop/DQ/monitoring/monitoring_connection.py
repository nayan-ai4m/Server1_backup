import platform
import subprocess
import concurrent.futures
from datetime import datetime
import time
import json
import os

# Define your log path here
LOG_PATH = "/home/ai4m/develop/DQ/monitoring/network_monitor.log"

def ping(ip):
    """
    Ping an IP address and return whether it's reachable.
    Returns: tuple (ip, is_reachable)
    """
    param = '-n' if platform.system().lower() == 'windows' else '-c'
    command = ['ping', param, '1', ip]
    try:
        subprocess.check_output(command, stderr=subprocess.STDOUT)
        return (ip, True)
    except subprocess.CalledProcessError:
        return (ip, False)

def check_network_status(machines):
    """Check the status of all machines in parallel."""
    connected = []
    disconnected = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ip = {executor.submit(ping, ip): (name, ip) 
                       for name, ip in machines.items()}
        
        for future in concurrent.futures.as_completed(future_to_ip):
            name, ip = future_to_ip[future]
            try:
                ip, is_reachable = future.result()
                if is_reachable:
                    connected.append((name, ip))
                else:
                    disconnected.append((name, ip))
            except Exception as e:
                print(f"Error checking {ip}: {str(e)}")
                disconnected.append((name, ip))
    
    return connected, disconnected

def write_log(message):
    """Write a message to the log file with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{timestamp}] {message}\n"
    
    
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    
    with open(LOG_PATH, 'a') as f:
        f.write(log_message)

def print_and_log(message):
    """Print a message and log it."""
    print(message)
    write_log(message)

def main():
    # Define equipment by group
    machines = {
        # Loop 3 Equipment
        "Loop 3 Mixer": "141.141.142.11",
        "Loop 3 Machine 17": "141.141.141.128",
        "Loop 3 Machine 18": "141.141.141.138",
        "Loop 3 Machine 19": "141.141.141.52",
        "Loop 3 Machine 20": "141.141.141.62",
        "Loop 3 Machine 21": "141.141.141.72",
        "Loop 3 Machine 22": "141.141.141.82",
        "Loop 3 Case Taping": "141.141.143.69",
        "Loop 3 Checkweigher": "141.141.143.85",
        
        # Loop 4 Equipment
        "Loop 4 Mixer": "141.141.142.31",
        "Loop 4 Machine 25": "141.141.141.201",
        "Loop 4 Machine 26": "141.141.141.203",
        "Loop 4 Machine 27": "141.141.141.209",
        "Loop 4 Machine 28": "141.141.143.9",
        "Loop 4 Machine 29": "141.141.143.1",
        "Loop 4 Machine 30": "141.141.143.3",
        "Loop 4 Case Taping": "141.141.143.70",
        "Loop 4 Checkweigher": "141.141.143.86"
    }

    print_and_log(f"Network monitor started. Logging to: {LOG_PATH}")
    print_and_log(f"Monitoring {len(machines)} machines every 5 minutes.")
    
    previous_connected = set()
    previous_disconnected = set()

    while True:
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            connected, disconnected = check_network_status(machines)
            
            
            current_connected = set(name for name, _ in connected)
            current_disconnected = set(name for name, _ in disconnected)
            
            
            if current_connected != previous_connected or current_disconnected != previous_disconnected:
                # Log the changes
                newly_connected = current_connected - previous_connected
                newly_disconnected = current_disconnected - previous_disconnected
                
                if newly_connected:
                    print_and_log(f"Newly connected machines:")
                    for name in newly_connected:
                        print_and_log(f"  - {name}")
                
                if newly_disconnected:
                    print_and_log(f"Newly disconnected machines:")
                    for name in newly_disconnected:
                        print_and_log(f"  - {name}")
                
                print_and_log(f"Summary:")
                print_and_log(f"  Total machines: {len(machines)}")
                print_and_log(f"  Connected: {len(connected)}")
                print_and_log(f"  Disconnected: {len(disconnected)}")
                print_and_log("-" * 60)
                
                # Update previous state
                previous_connected = current_connected
                previous_disconnected = current_disconnected
            
            # Sleep for 5 minutes
            time.sleep(300)
            
        except KeyboardInterrupt:
            print_and_log("\nMonitoring stopped by user.")
            break
        except Exception as e:
            print_and_log(f"Error: {str(e)}")
            time.sleep(300)  # Still wait 5 minutes on error

if __name__ == "__main__":
    main()
