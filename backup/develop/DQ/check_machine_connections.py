import platform
import subprocess
import concurrent.futures
from datetime import datetime

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
    
    print(f"Starting network check at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total machines to check: {len(machines)}\n")
    
    # Check all machines
    connected, disconnected = check_network_status(machines)
    
    # Print results by category
    def print_category_status(title, machines_list, category_machines):
        print(f"\n{title}:")
        print("-" * 60)
        found = False
        for name, ip in sorted(machines_list):
            if any(key in name for key in category_machines):
                print(f"{name:<40} {ip}")
                found = True
        if not found:
            print("No machines in this category")
    
    # Print connected machines by category
    print("\nConnected Machines")
    print("=" * 60)
    print_category_status("Loop 3 Equipment", connected, ["Loop 3"])
    print_category_status("Loop 4 Equipment", connected, ["Loop 4"])
    
    # Print disconnected machines by category
    print("\nDisconnected Machines")
    print("=" * 60)
    print_category_status("Loop 3 Equipment", disconnected, ["Loop 3"])
    print_category_status("Loop 4 Equipment", disconnected, ["Loop 4"])
    
    # Print summary
    print(f"\nSummary:")
    print(f"Total machines: {len(machines)}")
    print(f"Connected: {len(connected)}")
    print(f"Disconnected: {len(disconnected)}")

if __name__ == "__main__":
    main()
