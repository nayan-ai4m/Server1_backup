import os
import time
import subprocess
import json
import re
import socket
import smtplib
import psutil
from prometheus_client import start_http_server, Gauge, Info


# Constants
SLEEP_INTERVAL_SECONDS = 90
OUTPUT_FILE_PATH = 'cls1_test.json'
ERROR_MESSAGE = "Server is OFF"
DELAY = 5
RETRY_COUNT = 2
# MEMORY_THRESHOLD = 75  # Set a higher threshold for memory usage alerts
CAMERA_RESPONSE_THRESHOLD_MS = 10  # Set threshold for camera response time in milliseconds

# List of IP addresses to check
IP_LIST = ["192.168.4.11"]
CAMERA_IP_LIST = ["192.168.1.201" ,"192.168.1.20","192.168.1.22","192.168.1.27","192.168.1.25","192.168.1.23","192.168.1.24","192.168.1.26","192.168.1.29","192.168.1.28","192.168.1.30"] # Example camera IPs

# List of email addresses to receive notifications
RECEIVER_EMAILS = ["ashutosh.phanse@mitaoe.ac.in"]

# Email details
SENDER_EMAIL = "ashutosh.phanse@ai4mtech.com"
PASSWORD = "cydihsyqgewswfkm" # Replace with a secure method for handling passwords

# List of services to monitor
SERVICES = ['/home/ai4m/develop/data/monitoring/test_m1.py']


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
    try:
        # Create an SMTP connection and send the email to each receiver
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(SENDER_EMAIL, PASSWORD)
            for receiver_email in RECEIVER_EMAILS:
                server.sendmail(SENDER_EMAIL, receiver_email, f"Subject: {subject}\n\n{message}")
                print(f"Notification email sent to {receiver_email}")
    except Exception as e:
        print(f"Failed to send email: {e}")

def send_performance_alert(status_messages):
    subject = "Performance Alert: Server Device"
    message = "Performance issues detected:\n" + "\n".join(status_messages)
    send_email_notification(subject, message)

def send_ip_status_alert(offline_ips):
    subject = "IP Status Alert: Server Device"
    message = "The following IP addresses are offline:\n" + "\n".join(offline_ips)
    send_email_notification(subject, message)

def send_camera_ip_status_alert(alerts):
    subject = "Camera IP Status Alert: Server Device"
    messages = []

    for ip, details in alerts.items():
        try:
            # Extract response time and convert to float
            response_time = float(details['response_time_ms'].split('=')[1].split(' ')[0])

            if details['status'] == "offline":
                messages.append(f"Camera IP {ip} is offline")
            elif response_time > CAMERA_RESPONSE_THRESHOLD_MS:
                messages.append(f"Camera IP {ip} is online with response time {response_time:.2f} ms")
        except ValueError:
            messages.append(f"Camera IP {ip} is offline")

    # Only send email if there are alert messages
    if messages:
        message = "\n".join(messages)
        send_email_notification(subject, message)

def send_service_alert(service_name):
    subject = "Service Alert: Server Device"
    message = f"The service {service_name} is not running and could not be restarted."
    send_email_notification(subject, message)

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
                send_service_alert(service_name)

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

                    # Send camera IP status alert if response times exceed the threshold
                    send_camera_ip_status_alert(camera_ip_status)

                    # Collect service status
                    service_status = check_services()

                    # Log to JSON file
                    log_data = {
                        'timestamp': int(time.time()),
                        'system_info': system_info,
                        'performance_status': performance_status,
                        'thresholds': thresholds,
                        'ip_status': ip_status,
                        'camera_ip_status': camera_ip_status,  # Include camera IP status
                        'service_status': service_status
                    }
                    
                    with open(OUTPUT_FILE_PATH, 'a') as f:
                        json.dump(log_data, f, indent=4)
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

