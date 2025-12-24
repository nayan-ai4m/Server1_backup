import psutil
import subprocess
import json

def get_gpu_stats():
    try:
        # Run the nvidia-smi command to get GPU data in JSON format
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

def system_stats():
    stats = {
        "CPU Usage (%)": psutil.cpu_percent(interval=1),
        "Memory Usage (%)": psutil.virtual_memory().percent,
        "Total RAM (GB)": psutil.virtual_memory().total / (1024 ** 3),
        "Disk Usage (%)": psutil.disk_usage('/').percent,
        "Network Sent (MB)": psutil.net_io_counters().bytes_sent / (1024 ** 2),
        "Network Received (MB)": psutil.net_io_counters().bytes_recv / (1024 ** 2),
        "System Uptime (s)": psutil.boot_time(),
        "GPU Data": get_gpu_stats()
    }

    return stats

print(json.dumps(system_stats(), indent=4))

