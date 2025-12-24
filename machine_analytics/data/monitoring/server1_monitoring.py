import os
import time
import subprocess
import json
import re
import socket
import smtplib
import psutil
from prometheus_client import start_http_server, Gauge, Info

class ServerMonitor:
    def __init__(self, config_path='monitoring_config.json'):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # Initialize Prometheus metrics
        self.setup_prometheus_metrics()

    def setup_prometheus_metrics(self):
        """Initialize Prometheus metrics"""
        self.cpu_usage_gauge = Gauge('cpu_usage', 'CPU usage in percentage')
        self.gpu_usage_gauge = Gauge('gpu_usage', 'GPU usage in percentage')
        self.ram_usage_gauge = Gauge('ram_usage', 'RAM usage in percentage')
        self.uptime_gauge = Gauge('uptime', 'System uptime in seconds')
        self.network = Info('network', 'Network interfaces and their addresses')
        self.network_bytes_recv_gauge = Gauge('network_bytes_received', 'Network bytes received', ['interface'])
        self.network_packets_sent_gauge = Gauge('network_packets_sent', 'Network packets sent', ['interface'])
        self.network_packets_recv_gauge = Gauge('network_packets_received', 'Network packets received', ['interface'])

    def get_ip_address(self):
        """Get current system IP address"""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('10.254.254.254', 1))
            ip_address = s.getsockname()[0]
        except Exception:
            ip_address = 'N/A'
        finally:
            s.close()
        return ip_address

    def calculate_percentage(self, part, total):
        """Calculate percentage with rounding"""
        return round((part / total) * 100) if total != 0 else 0

    def get_gpu_stats(self):
        """Fetch GPU statistics using nvidia-smi"""
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

    def collect_system_info(self):
        """Collect comprehensive system information"""
        system_info = {
            'ip_address': self.get_ip_address(),
            'cpu': self.get_cpu_info(),
            'memory': self.get_memory_info(),
            'gpu': self.get_gpu_info(),
            'disk': self.get_disk_info(),
            'services': self.get_systemd_services()
        }
        return system_info

    def get_cpu_info(self):
        """Collect CPU information"""
        cpu_times = psutil.cpu_times_percent(interval=1, percpu=False)
        total_cpu_percentage = self.calculate_percentage(
            cpu_times.user + cpu_times.nice + cpu_times.system, 100)
        return {'total_usage_percentage': round(total_cpu_percentage)}

    def get_memory_info(self):
        """Collect memory information"""
        memory_data = psutil.virtual_memory()
        ram_total = memory_data.total / 1e9
        ram_used = memory_data.used / 1e9
        ram_free = memory_data.available / 1e9
        return {
            'total_ram_gb': ram_total,
            'used_ram_gb': ram_used,
            'free_ram_gb': ram_free,
            'used_ram_percentage': self.calculate_percentage(ram_used, ram_total),
            'free_ram_percentage': self.calculate_percentage(ram_free, ram_total)
        }

    def get_gpu_info(self):
        """Collect GPU information"""
        gpu_data = self.get_gpu_stats()
        if isinstance(gpu_data, list) and len(gpu_data) > 0:
            memory_used = gpu_data[0]["GPU Memory Used (MB)"]
            memory_total = gpu_data[0]["GPU Memory Total (MB)"]
            return {
                'load_percentage': round((memory_used / memory_total) * 100) if memory_total > 0 else 0,
                'memory_used': memory_used,
                'memory_total': memory_total
            }
        return {'load_percentage': 0, 'memory_used': 0, 'memory_total': 0}

    def get_disk_info(self):
        """Collect disk information"""
        disk_data = psutil.disk_usage('/')
        disk_total = disk_data.total / 1e9
        disk_used = disk_data.used / 1e9
        disk_available = disk_data.free / 1e9
        return {
            'total_disk_gb': disk_total,
            'used_disk_gb': disk_used,
            'available_disk_gb': disk_available,
            'used_disk_percentage': self.calculate_percentage(disk_used, disk_total),
            'available_disk_percentage': self.calculate_percentage(disk_available, disk_total)
        }

    def get_systemd_services(self):
        """Get status of systemd services"""
        result = subprocess.run(['systemctl', 'list-units', '--type=service', '--all', '--no-pager', '--no-legend'],
                              stdout=subprocess.PIPE, universal_newlines=True)
        services = result.stdout.splitlines()
        return [{'service_name': parts[0].split('.service')[0],
                'active_state': 'inactive' if parts[2] == 'inactive' else 'active'}
                for parts in (service.split() for service in services)
                if len(parts) > 4]

    def check_ip_status(self):
        """Check status of configured IP addresses"""
        current_ip = self.get_ip_address()
        ip_list = [current_ip] + self.config['network']['server_ips']
        
        ip_status = {}
        for ip in ip_list:
            status = "offline"
            for _ in range(self.config['monitoring']['retry_count']):
                try:
                    if os.system(f"ping -c 1 {ip}") == 0:
                        status = "online"
                        break
                except Exception:
                    continue
            ip_status[ip] = status
        return ip_status

    def check_camera_ip_status(self):
        """Check status and response time of camera IPs"""
        camera_ip_status = {}
        for ip in self.config['network']['camera_ips']:
            status = "offline"
            response_times = []
            
            for _ in range(3):
                try:
                    result = subprocess.run(["ping", "-c", "1", ip],
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE,
                                         universal_newlines=True)
                    match = re.search(r'time=(\d+\.\d+) ms', result.stdout)
                    if match:
                        response_times.append(float(match.group(1)))
                except Exception as e:
                    print(f"Error while pinging {ip}: {e}")
                    continue
            
            if len(response_times) >= 2:
                response_time = response_times[1]
                status = "online"
            else:
                response_time = float('inf')
            
            camera_ip_status[ip] = {
                'status': status,
                'response_time_ms': f"time={response_time:.3f} ms" if response_time != float('inf') else "time=N/A"
            }
        return camera_ip_status

    def send_email_notification(self, subject, message):
        """Send email notification"""
        try:
            with smtplib.SMTP(self.config['email']['smtp_server'],
                            self.config['email']['smtp_port']) as server:
                server.starttls()
                server.login(self.config['email']['sender'],
                           self.config['email']['password'])
                for receiver in self.config['email']['receivers']:
                    server.sendmail(self.config['email']['sender'],
                                  receiver,
                                  f"Subject: {subject}\n\n{message}")
        except Exception as e:
            print(f"Failed to send email: {e}")

    def check_performance(self, system_info):
        """Check system performance against thresholds"""
        performance_status = {
            'cpu': system_info['cpu']['total_usage_percentage'],
            'memory': system_info['memory']['used_ram_percentage'],
            'gpu': system_info['gpu']['load_percentage'],
            'disk': system_info['disk']['used_disk_percentage']
        }

        status_messages = []
        for key, value in performance_status.items():
            if value > self.config['thresholds'][key]:
                status_messages.append(f"{key.upper()} OVER_USED: {value:.2f}%")

        return performance_status, status_messages

    def collect_stats(self):
        """Collect and update Prometheus metrics"""
        try:
            system_info = self.collect_system_info()
            
            self.cpu_usage_gauge.set(system_info['cpu']['total_usage_percentage'])
            self.ram_usage_gauge.set(system_info['memory']['used_ram_percentage'])
            self.gpu_usage_gauge.set(system_info['gpu']['load_percentage'])
            
            uptime = time.time() - psutil.boot_time()
            self.uptime_gauge.set(uptime)

            network_info = psutil.net_io_counters(pernic=True)
            for interface, stats in network_info.items():
                self.network_bytes_recv_gauge.labels(interface=interface).set(stats.bytes_recv)
                self.network_packets_sent_gauge.labels(interface=interface).set(stats.packets_sent)
                self.network_packets_recv_gauge.labels(interface=interface).set(stats.packets_recv)

        except Exception as e:
            print(f"Error while collecting stats: {e}")

    def run_monitoring(self):
        """Main monitoring loop"""
        start_http_server(self.config['monitoring']['prometheus_port'])

        while True:
            try:
                # Collect system information
                system_info = self.collect_system_info()
                performance_status, status_messages = self.check_performance(system_info)
                
                # Check various statuses
                ip_status = self.check_ip_status()
                camera_ip_status = self.check_camera_ip_status()
                
                # Prepare log data
                log_data = {
                    'timestamp': int(time.time()),
                    'system_info': system_info,
                    'performance_status': performance_status,
                    'thresholds': self.config['thresholds'],
                    'ip_status': ip_status,
                    'camera_ip_status': camera_ip_status
                }
                
                # Write to output file
                with open(self.config['monitoring']['output_file_path'], 'a') as f:
                    json.dump(log_data, f, indent=4)
                    f.write('\n')
                
                # Send alerts if needed
                if status_messages:
                    self.send_email_notification(
                        "Performance Alert: Server Device",
                        "Performance issues detected:\n" + "\n".join(status_messages)
                    )
                
                offline_ips = [ip for ip, status in ip_status.items() if status == 'offline']
                if offline_ips:
                    self.send_email_notification(
                        "IP Status Alert: Server Device",
                        "The following IP addresses are offline:\n" + "\n".join(offline_ips)
                    )
                
                # Update Prometheus metrics
                self.collect_stats()
                
            except socket.gaierror as e:
                print(f"{self.config['monitoring']['error_message']}: {e}")
            except Exception as e:
                print(f"General error: {e}")
            
            time.sleep(self.config['monitoring']['sleep_interval_seconds'])

if __name__ == "__main__":
    monitor = ServerMonitor()
    monitor.run_monitoring()
