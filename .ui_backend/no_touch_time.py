import psycopg2
import json
from datetime import datetime, timedelta
import sys
import csv
from collections import defaultdict

# Usage: python script.py YYYY-MM-DD (optional, defaults to today)
if len(sys.argv) > 1:
    process_date_str = sys.argv[1]
    process_date = datetime.strptime(process_date_str, "%Y-%m-%d").date()
else:
    process_date = datetime.now().date()

# Current time for ongoing shift
current_time = datetime.now()
is_current_day = process_date == current_time.date()

# Database connection (replace with your actual credentials)
conn = psycopg2.connect(
    dbname="hul",
    user="postgres",
    password="ai4m2024",
    host="localhost",
    port="5432"  # default 5432 if omitted
)
cur = conn.cursor()

# Define monitored keys
monitored_keys = [
    "HMI_Filling_Stroke_Deg",
    "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3",
    "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6",
    "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9",
    "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12",
    "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15",
    "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18",
    "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21",
    "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24",
    "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Front_27",
    "HMI_Hor_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2",
    "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "HMI_Hopper_Low_Level",
    "HMI_Hopper_High_Level", "HMI_Hopper_Ex_Low_Level", "Hopper_Level_Percentage"
]

# Determine current shift based on current time
def get_shift(ts):
    hour = ts.hour
    if 7 <= hour < 15:
        return '1', datetime.combine(ts.date(), datetime.strptime("07:00", "%H:%M").time()), datetime.combine(ts.date(), datetime.strptime("15:00", "%H:%M").time())
    elif 15 <= hour < 23:
        return '2', datetime.combine(ts.date(), datetime.strptime("15:00", "%H:%M").time()), datetime.combine(ts.date(), datetime.strptime("23:00", "%H:%M").time())
    else:
        # Shift 3: 23:00 to 07:00 next day
        if hour < 7:
            start = datetime.combine(ts.date() - timedelta(days=1), datetime.strptime("23:00", "%H:%M").time())
            end = datetime.combine(ts.date(), datetime.strptime("07:00", "%H:%M").time())
        else:
            start = datetime.combine(ts.date(), datetime.strptime("23:00", "%H:%M").time())
            end = datetime.combine(ts.date() + timedelta(days=1), datetime.strptime("07:00", "%H:%M").time())
        return '3', start, end

current_shift, shift_start, shift_end = get_shift(current_time)
if is_current_day:
    end_period = min(current_time, shift_end)  # Up to current time or shift end
else:
    end_period = shift_end  # Full shift for historical date
start_period = shift_start

# Fetch status data for current shift
cur.execute("""
SELECT timestamp, status 
FROM mc17 
WHERE timestamp >= %s AND timestamp < %s 
ORDER BY timestamp
""", (start_period, end_period))
status_rows = cur.fetchall()

# Find CRT segments, including ongoing ones
crt_list = []
i = 0
n = len(status_rows)
while i < n:
    if (i == 0 or status_rows[i-1][1] != 1) and status_rows[i][1] == 1:
        start = status_rows[i][0]
        j = i
        while j + 1 < n and status_rows[j + 1][1] == 1:
            j += 1
        if j + 1 < n:
            end = status_rows[j + 1][0]
            is_ongoing = False
        else:
            end = end_period  # Ongoing CRT ends at end_period
            is_ongoing = True
        duration = (end - start).total_seconds() / 60.0
        if duration > 0:
            crt_list.append({
                'machine': 'mc17',
                'start': start,
                'end': end,
                'duration': duration,
                'touch_events': [],
                'shift': current_shift,
                'ntts': [],
                'is_ongoing': is_ongoing
            })
        i = j + 1
    else:
        i += 1

# For each CRT, find touch events and split into NTT
for crt in crt_list:
    cur.execute("""
    SELECT timestamp, mc17 
    FROM loop3_checkpoints 
    WHERE mc17 IS NOT NULL AND mc17 != 'null'
    AND timestamp >= %s AND timestamp <= %s 
    ORDER BY timestamp
    """, (crt['start'], crt['end']))
    param_rows = cur.fetchall()

    prev_values = None
    current_ntt_start = crt['start']
    touch_events = []
    ntts = []

    for row in param_rows:
        current_ts = row[0]
        mc17_json = json.loads(row[1]) if isinstance(row[1], str) else row[1]
        current_values = tuple(mc17_json.get(k, None) for k in monitored_keys)

        if prev_values is not None and current_values != prev_values:
            touch_events.append(current_ts)
            ntt_end = current_ts
            ntt_duration = (ntt_end - current_ntt_start).total_seconds() / 60.0
            ntts.append({
                'start': current_ntt_start,
                'end': ntt_end,
                'duration': ntt_duration,
                'parent': crt['start'],
                'is_ongoing': False
            })
            current_ntt_start = current_ts

        prev_values = current_values

    # Add the last NTT
    ntt_end = crt['end']
    ntt_duration = (ntt_end - current_ntt_start).total_seconds() / 60.0
    if ntt_duration > 0:
        ntts.append({
            'start': current_ntt_start,
            'end': ntt_end,
            'duration': ntt_duration,
            'parent': crt['start'],
            'is_ongoing': crt['is_ongoing']
        })

    crt['touch_events'] = touch_events
    crt['ntts'] = ntts

# Aggregate KPIs for current shift
shift_data = {'crt_durs': [], 'ntt_durs': []}
for crt in crt_list:
    shift_data['crt_durs'].append(crt['duration'])
    for ntt in crt['ntts']:
        shift_data['ntt_durs'].append(ntt['duration'])

# Calculate KPIs
kpi = {}
if shift_data['crt_durs']:
    kpi['mttr'] = sum(shift_data['crt_durs']) / len(shift_data['crt_durs'])
    kpi['max_crt'] = max(shift_data['crt_durs'])
    kpi['max_ntt'] = max(shift_data['ntt_durs']) if shift_data['ntt_durs'] else 0
    kpi['total_ntt'] = sum(shift_data['ntt_durs'])
    kpi['percent_no_touch'] = (kpi['total_ntt'] / sum(shift_data['crt_durs']) * 100) if sum(shift_data['crt_durs']) > 0 else 0
else:
    kpi = {'mttr': 0, 'max_crt': 0, 'max_ntt': 0, 'total_ntt': 0, 'percent_no_touch': 0}

# Write to CSV files
timestamp_str = current_time.strftime("%Y%m%d_%H%M%S")
date_str = process_date.strftime("%Y-%m-%d")

# CRT CSV
with open(f'crt_history_{date_str}_shift{current_shift}_{timestamp_str}.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Machine', 'Start Time', 'End Time', 'Duration (min)', 'Operator Touch Events'])
    for crt in crt_list:
        touch_events_str = ','.join([t.strftime("%Y-%m-%d %H:%M:%S.%f") for t in crt['touch_events']])
        writer.writerow([
            crt['machine'],
            crt['start'].strftime("%Y-%m-%d %H:%M:%S.%f"),
            crt['end'].strftime("%Y-%m-%d %H:%M:%S.%f"),
            f"{crt['duration']:.2f}",
            f"[{touch_events_str}]"
        ])

# NTT CSV
with open(f'ntt_history_{date_str}_shift{current_shift}_{timestamp_str}.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Machine', 'Start Time', 'End Time', 'Duration (min)', 'Parent CRT Start Time'])
    for crt in crt_list:
        for ntt in crt['ntts']:
            writer.writerow([
                crt['machine'],
                ntt['start'].strftime("%Y-%m-%d %H:%M:%S.%f"),
                ntt['end'].strftime("%Y-%m-%d %H:%M:%S.%f"),
                f"{ntt['duration']:.2f}",
                ntt['parent'].strftime("%Y-%m-%d %H:%M:%S.%f")
            ])

# KPI CSV
with open(f'kpi_summary_{date_str}_shift{current_shift}_{timestamp_str}.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Machine', 'Shift', 'MTTR (min)', 'Max CRT (min)', 'Max NTT (min)', 'Total NTT (min)', '% No-Touch Time'])
    writer.writerow([
        'mc17',
        current_shift,
        f"{kpi['mttr']:.2f}",
        f"{kpi['max_crt']:.2f}",
        f"{kpi['max_ntt']:.2f}",
        f"{kpi['total_ntt']:.2f}",
        f"{kpi['percent_no_touch']:.2f}%"
    ])

conn.close()
