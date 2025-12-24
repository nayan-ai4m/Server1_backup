import os
import time
import subprocess
import json
import re
import socket
import smtplib
import psutil
from email.mime.text import MIMEText
import smtplib
from prometheus_client import start_http_server, Gauge, Info


# Load configuration from JSON file
with open("monitoring_config.json", "r") as config_file:
    config = json.load(config_file)

# Accessing values
SLEEP_INTERVAL_SECONDS = config["SLEEP_INTERVAL_SECONDS"]
OUTPUT_FILE_PATH = config["OUTPUT_FILE_PATH"]
ERROR_MESSAGE = config["ERROR_MESSAGE"]
DELAY = config["DELAY"]
RETRY_COUNT = config["RETRY_COUNT"]
CAMERA_RESPONSE_THRESHOLD_MS = config["CAMERA_RESPONSE_THRESHOLD_MS"]
CAMERA_NAME_MAPPING = config["CAMERA_NAME_MAPPING"]
SERVICE_NAME_MAPPING = config["SERVICE_NAME_MAPPING"]




IP_LIST = config["IP_LIST"]
CAMERA_IP_LIST = config["CAMERA_IP_LIST"]
RECEIVER_EMAILS = config["RECEIVER_EMAILS"]
SENDER_EMAIL = config["SENDER_EMAIL"]
PASSWORD = config["PASSWORD"]
SERVICES = config["SERVICES"]



def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.254.254.254', 1))
        ip_address = s.getsockname()[0]
    except Exception:
        ip_address = 'N/A'
    finally:
        s.close()
    return ip_address

def calculate_percentage(part, total):
    return round((part / total) * 100) if total != 0 else 0

def check_performance(system_info):
    thresholds = {
        'cpu': 95,
        'memory': 75,  # Adjusted memory threshold
        'gpu': 95,
        'disk': 75,
    }

    performance_status = {
        'cpu': system_info['cpu']['total_usage_percentage'],
        'memory': system_info['memory']['used_ram_percentage'],
        'gpu': system_info['gpu']['load_percentage'] if 'gpu' in system_info else 0,
        'disk': system_info['disk']['used_disk_percentage'],
    }

    status_messages = []
    for key, value in performance_status.items():
        if value > thresholds[key]:
            status_messages.append(f"{key.upper()} OVER_USED: {value:.2f}%")

    return performance_status, status_messages, thresholds

def get_systemd_services():
    result = subprocess.run(['systemctl', 'list-units', '--type=service', '--all', '--no-pager', '--no-legend'],
                            stdout=subprocess.PIPE, universal_newlines=True)
    services = result.stdout.splitlines()

    service_list = []
    for service in services:
        parts = service.split()
        if len(parts) > 4:
            service_name = parts[0].split('.service')[0]
            active_state = 'inactive' if parts[2] == 'inactive' else 'active'
            service_list.append({'service_name': service_name, 'active_state': active_state})

    return service_list

def get_gpu_stats():
    """Fetch GPU data using `nvidia-smi`."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,name,utilization.gpu,memory.total,memory.used,temperature.gpu", "--format=csv,noheader,nounits"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        if result.returncode != 0:
            return {"GPU Error": result.stderr.strip()}

        gpus = []
        for line in result.stdout.strip().split("\n"):
            index, name, usage, total_mem, used_mem, temp = line.split(", ")
            gpus.append({
                "GPU Index": int(index),
                "GPU Name": name,
                "GPU Usage (%)": float(usage),
                "GPU Memory Used (MB)": float(used_mem),
                "GPU Memory Total (MB)": float(total_mem),
                "GPU Temperature (°C)": float(temp)
            })

        return gpus

    except Exception as e:
        return {"GPU Error": str(e)}




def collect_system_info(server1):
    system_info = {}

    system_info['ip_address'] = get_ip_address()

    # CPU Information
    cpu_times = psutil.cpu_times_percent(interval=1, percpu=False)
    total_cpu_percentage = calculate_percentage(cpu_times.user + cpu_times.nice + cpu_times.system, 100)
    system_info['cpu'] = {
        'total_usage_percentage': round(total_cpu_percentage)
    }
    
    # Memory Information
    memory_data = psutil.virtual_memory()
    ram_total = memory_data.total / 1e9  # Convert to GB
    ram_used = memory_data.used / 1e9
    ram_free = memory_data.available / 1e9
    system_info['memory'] = {
        'total_ram_gb': ram_total,
        'used_ram_gb': ram_used,
        'free_ram_gb': ram_free,
        'used_ram_percentage': calculate_percentage(ram_used, ram_total),
        'free_ram_percentage': calculate_percentage(ram_free, ram_total)
    }
    
    ## GPU Information
    gpu_data = get_gpu_stats()

    # Ensure we have at least one GPU and extract its details
    # if isinstance(gpu_data, list) and len(gpu_data) > 0:
    #     system_info['gpu'] = {
    #         'load_percentage': gpu_data[0]["GPU Usage (%)"],  # Extracting first GPU's usage
    #         'memory_used': gpu_data[0]["GPU Memory Used (MB)"],
    #         'memory_total': gpu_data[0]["GPU Memory Total (MB)"]
    #     }
    if isinstance(gpu_data, list) and len(gpu_data) > 0:
        memory_used = gpu_data[0]["GPU Memory Used (MB)"]
        memory_total = gpu_data[0]["GPU Memory Total (MB)"]
        
        system_info['gpu'] = {
            'load_percentage': round((memory_used / memory_total) * 100) if memory_total > 0 else 0,
            'memory_used': memory_used,
            'memory_total': memory_total
        }

    else:
        system_info['gpu'] = {'load_percentage': 0, 'memory_used': 0, 'memory_total': 0}  # Default values



    
    
    # Disk Information
    disk_data = psutil.disk_usage('/')
    disk_total = disk_data.total / 1e9  # Convert to GB
    disk_used = disk_data.used / 1e9
    disk_available = disk_data.free / 1e9
    system_info['disk'] = {
        'total_disk_gb': disk_total,
        'used_disk_gb': disk_used,
        'available_disk_gb': disk_available,
        'used_disk_percentage': calculate_percentage(disk_used, disk_total),
        'available_disk_percentage': calculate_percentage(disk_available, disk_total)
    }
   
    
    
    # Get systemd services
    system_info['services'] = get_systemd_services()

    return system_info


def check_ip_status():
    current_ip = get_ip_address()
    ip_list = [current_ip] + IP_LIST  # Include current live system IP

    ip_status = {}
    for ip in ip_list:
        status = "online"  # Default to online
        for _ in range(RETRY_COUNT):
            try:
                ping_command = f"ping -c 1 {ip}"
                ping_result = os.system(ping_command)
                if ping_result == 0:
                    status = "online"
                    break
            except Exception:
                status = "offline"
        else:
            status = "offline"
        ip_status[ip] = status

    return ip_status

def check_camera_ip_status():
    camera_ip_status = {}
    for ip in CAMERA_IP_LIST:
        status = "offline"  # Default to offline
        response_times = []
        
        for _ in range(3):  # Ping 3 times
            try:
                # Run ping command and capture output
                result = subprocess.run(["ping", "-c", "1", ip], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                
                # Extract response time using regex
                match = re.search(r'time=(\d+\.\d+) ms', result.stdout)
                if match:
                    time_ms = float(match.group(1))
                    response_times.append(time_ms)
                
            except Exception as e:
                print(f"Error while pinging {ip}: {e}")
                continue
        
        # Determine status and response time
        if len(response_times) >= 2:  # Ensure we have at least 2 response times
            response_time = response_times[1]  # Take the response time from the second successful ping
            status = "online"
        else:
            response_time = float('inf')  # Set a high value if the IP is offline
            status = "offline"
        
        # Format response time as `time=0.556 ms`
        formatted_response_time = f"time={response_time:.3f} ms" if response_time != float('inf') else "time=N/A"

        camera_ip_status[ip] = {
            'status': status,
            'response_time_ms': formatted_response_time
        }

    return camera_ip_status

def send_email_notification(subject, message):
    """Send email with the formatted alert message."""
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(SENDER_EMAIL, PASSWORD)
            for receiver_email in RECEIVER_EMAILS:
                email_body = f"Subject: {subject}\n\n{message}"
                server.sendmail(SENDER_EMAIL, receiver_email, email_body)
                print(f"Notification email sent to {receiver_email}")
    except Exception as e:
        print(f"Failed to send email: {e}")

def send_performance_alert(status_messages):
    """Send an email alert for system performance issues."""
    subject = "Performance Alert: Server Device"
    message = " Performance issues detected: \n\n" + "\n".join(f"- {msg}" for msg in status_messages)
    send_email_notification(subject, message)



def send_service_alert(service_name, config):
    """
    Send an alert email when a critical service is stopped and fails to restart.
    Args:
        service_name: The service name from check_services()
        config: Configuration dictionary containing service name mappings
    """
    # Get the full service path from SERVICES
    full_service_path = None
    for service in config["SERVICES"]:
        if service_name in service:
            full_service_path = service
            break
    
    if not full_service_path:
        full_service_path = service_name

    # Get the display name using the full path
    service_display_name = config["SERVICE_NAME_MAPPING"].get(full_service_path, service_name)

    subject = f"HUL Haridwar Dark Cascade Service Down Alert"
    message = (
        f"Service Alert: {service_display_name} is Down\n\n"
        f"ALERT: The service {service_display_name} is not running and could not be restarted.\n\n"
        f"Service Path: {full_service_path}\n\n"
        f"Immediate action required!"
    )
    
    print(message)
    send_email_notification(subject, message)


def send_ip_status_alert(offline_ips):
    """Send an alert email when IP addresses go offline."""
    if not offline_ips:
        return  # No need to send an email if no IPs are offline

    subject = "HUL Haridwar Dark Cascade IP Status Offline"
    message_lines = ["  The following devices are offline: \n"]

    for ip in offline_ips:
        machine_name = CAMERA_NAME_MAPPING.get(ip, "Unknown Device")
        message_lines.append(f"- **{machine_name}** :: ({ip}) Offline")

    message = "\n".join(message_lines)
    
    send_email_notification(subject, message)



def send_camera_ip_status_alert(alerts):
    """Send an alert email for camera IP status with formatted response times."""
    subject = "HUL Haridwar Dark Cascade Camera IP Down Alert"
    messages = []

    for ip, details in alerts.items():
        machine_name = CAMERA_NAME_MAPPING.get(ip, "Unknown Device")

        try:
            response_time_str = details['response_time_ms']
            response_time = float(response_time_str.split('=')[1].split(' ')[0]) if response_time_str != "time=N/A" else None

            if details['status'] == "offline":
                messages.append(f"- **{machine_name}** :: ({ip}) Offline")
            elif response_time and response_time > CAMERA_RESPONSE_THRESHOLD_MS:
                messages.append(f"- **{machine_name}** :: ({ip}) High Response Time: {response_time:.2f} ms")
        except ValueError:
            messages.append(f"- **{machine_name}** :: {ip})  Offline")

    if messages:
        formatted_message = "  Camera Status Update:  \n\n" + "\n".join(messages)
        send_email_notification(subject, formatted_message)



def check_services():
    status_dict = {}

    for service in SERVICES:
        service_name = os.path.basename(service)
        if subprocess.run(["pgrep", "-f", service], stdout=subprocess.PIPE, stderr=subprocess.PIPE).returncode == 0:
            print(f"{service} is running")
            status_dict[service_name] = "running"
        else:
            print(f"{service} stopped")
            status_dict[service_name] = "stopped"
           # send_service_alert(service_name)

            time.sleep(DELAY)
            if subprocess.run(["pgrep", "-f", service], stdout=subprocess.PIPE, stderr=subprocess.PIPE).returncode != 0:
                print(f"Failed to start {service}")
                status_dict[service_name] = "stopped"
                send_service_alert(service_name,config)

    return status_dict

class ServerMonitor:
    def __init__(self):
        # Create Prometheus Gauges for the metrics
        self.cpu_usage_gauge = Gauge('cpu_usage', 'CPU usage in percentage')
        self.gpu_usage_gauge = Gauge('gpu_usage', 'GPU usage in percentage')
        self.ram_usage_gauge = Gauge('ram_usage', 'RAM usage in percentage')
        self.uptime_gauge = Gauge('uptime', 'System uptime in seconds')
        self.network = Info('network', 'Network interfaces and their addresses')
        self.network_bytes_recv_gauge = Gauge('network_bytes_received', 'Network bytes received', ['interface'])
        self.network_packets_sent_gauge = Gauge('network_packets_sent', 'Network packets sent', ['interface'])
        self.network_packets_recv_gauge = Gauge('network_packets_received', 'Network packets received', ['interface'])

    def collect_stats(self, server1):

        try:
            # Collect CPU, RAM, and GPU usage
            cpu_usage = psutil.cpu_percent()
            memory_usage = psutil.virtual_memory().percent

            # GPU usage (if available)
            gpu_usage = 0  # Default value
            gpu_data = get_gpu_stats()
            if isinstance(gpu_data, list) and len(gpu_data) > 0:
                memory_used = gpu_data[0]["GPU Memory Used (MB)"]
                memory_total = gpu_data[0]["GPU Memory Total (MB)"]
                gpu_usage = round((memory_used / memory_total) * 100) if memory_total > 0 else 0


            # Set Prometheus Gauges
            self.cpu_usage_gauge.set(cpu_usage)
            self.ram_usage_gauge.set(memory_usage)
            self.gpu_usage_gauge.set(gpu_usage)

            # Uptime
            uptime = time.time() - psutil.boot_time()
            self.uptime_gauge.set(uptime)

            # Network statistics
            network_info = psutil.net_io_counters(pernic=True)
            for interface, stats in network_info.items():
                self.network_bytes_recv_gauge.labels(interface=interface).set(stats.bytes_recv)
                self.network_packets_sent_gauge.labels(interface=interface).set(stats.packets_sent)
                self.network_packets_recv_gauge.labels(interface=interface).set(stats.packets_recv)

            print(f"CPU: {cpu_usage}% | RAM: {memory_usage}% | GPU: {gpu_usage}% | Uptime: {uptime}s")

        except Exception as e:
            print(f"Error while collecting stats: {e}")



    def run_monitoring(self):
        # Start the Prometheus HTTP server on port 8091
        start_http_server(8091)

        while True:
            try:
                    server1 = psutil
                    system_info = collect_system_info(server1)
                    
                    # Print system information
                    print("System Information:")
                    print(json.dumps(system_info, indent=4))
                    
                    # Print performance status and log messages
                    performance_status, status_messages, thresholds = check_performance(system_info)
                    
                    print("Performance Status:")
                    print(json.dumps(performance_status, indent=4))

                    for message in status_messages:
                        print(message)

                    # Send performance alert if there are any status messages
                    if status_messages:
                        send_performance_alert(status_messages)

                    # Collect IP status and update JSON file
                    ip_status = check_ip_status()

                    # Send IP status alert if any IP is offline
                    offline_ips = [ip for ip, status in ip_status.items() if status == 'offline']
                    if offline_ips:
                        send_ip_status_alert(offline_ips)

                    # Collect camera IP status and response times
                    camera_ip_status = check_camera_ip_status()

                    # **Filter only offline or high response time cameras**
                    affected_cameras = {
                        ip: details
                        for ip, details in camera_ip_status.items()
                        if details['status'] == "offline" or (
                            details['response_time_ms'] != "time=N/A" and float(details['response_time_ms'].split('=')[1].split(' ')[0]) > CAMERA_RESPONSE_THRESHOLD_MS
                        )
                    }

                    # Send alert for affected cameras
                    send_camera_ip_status_alert(affected_cameras)

                    # Collect service status
                    service_status = check_services()

                    # **Filter only stopped services**
                    stopped_services = {name: status for name, status in service_status.items() if status == "stopped"}

                    # Log to JSON file


                    # Log to JSON file
                    # log_data = {
                    #     'timestamp': int(time.time()),
                    #     'system_info': system_info,
                    #     'performance_status': performance_status,
                    #     'thresholds': thresholds,
                    #     'ip_status': ip_status,
                    #     'camera_ip_status': camera_ip_status,  # Include camera IP status
                    #     'service_status': service_status
                    # }
                    log_error_data = {
                        'timestamp': time.strftime("%d-%m-%y %H:%M:%S"),
                        'status_messages' : status_messages,
                        'offline_ips' : offline_ips,
                        'camera_issues': affected_cameras,
                        'stopped_services': list(stopped_services.keys())  # Only log stopped service names

                    }


                    
                    with open(OUTPUT_FILE_PATH, 'a') as f:
                        json.dump(log_error_data, f, indent=4)
                        f.write('\n')

                    # Collect and update Server board metrics
                    self.collect_stats(server1)
                    # self.update_server1_board_metrics(server1)

            except socket.gaierror as e:
                print(f"{ERROR_MESSAGE}: {e}")
                time.sleep(SLEEP_INTERVAL_SECONDS)
            except Exception as e:
                print(f"General error: {e}")

            time.sleep(SLEEP_INTERVAL_SECONDS)

if __name__ == "__main__":
    monitor = ServerMonitor()
    monitor.run_monitoring()

