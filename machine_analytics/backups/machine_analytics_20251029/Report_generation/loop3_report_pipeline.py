import subprocess
import os
import time

# Scripts that save CSV data
save_data_scripts = [
    "/home/ai4m/develop/machine_analytics/Report_generation/MC17/gen_report.py",
    "/home/ai4m/develop/machine_analytics/Report_generation/MC18/gen_report.py",
    "/home/ai4m/develop/machine_analytics/Report_generation/MC19/gen_report.py",
    "/home/ai4m/develop/machine_analytics/Report_generation/MC20/gen_report.py",
    "/home/ai4m/develop/machine_analytics/Report_generation/MC21/gen_report.py",
    "/home/ai4m/develop/machine_analytics/Report_generation/MC22/gen_report.py"
]

# Scripts that generate reports
create_report_scripts = [
    "/home/ai4m/develop/machine_analytics/Report_generation/MC17/create_report.py",
    "/home/ai4m/develop/machine_analytics/Report_generation/MC18/create_report.py",
    "/home/ai4m/develop/machine_analytics/Report_generation/MC19/create_report.py",
    "/home/ai4m/develop/machine_analytics/Report_generation/MC20/create_report.py",
    "/home/ai4m/develop/machine_analytics/Report_generation/MC21/create_report.py",
    "/home/ai4m/develop/machine_analytics/Report_generation/MC22/create_report.py"
]

def run_scripts(script_list, phase_name):
    print(f"\n🚀 Starting {phase_name} scripts...\n")
    for script in script_list:
        if os.path.exists(script):
            script_dir = os.path.dirname(script)
            print(f"▶ Running: {script}")
            try:
                # Set working directory to script's folder
                subprocess.run(["python", script], check=True, cwd=script_dir)
            except subprocess.CalledProcessError as e:
                print(f"❌ Error running {script}: {e}")
        else:
            print(f"⚠️ Script not found: {script}")
        time.sleep(2)
    print(f"\n✅ Finished {phase_name} scripts.\n")

# Run save_data scripts
run_scripts(save_data_scripts, "Save Data")

# Wait for 2 minutes before running report scripts
print("⏳ Waiting 2 minutes before starting report scripts...\n")
time.sleep(120)

# Run create_report scripts
run_scripts(create_report_scripts, "Create Report")
