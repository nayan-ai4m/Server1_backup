import time
from datetime import datetime
import psycopg2
from pycomm3 import LogixDriver

# ---------------- PLC and DB CONFIG ----------------
PLC_IP = "141.141.143.51"
DB_CONFIG = {
    "dbname": "hul",
    "user": "postgres",
    "password": "ai4m2024",
    "host": "localhost",
    "port": 5432,
}

# ---------------- TAG MAPPINGS ----------------
mc_status_tags = {
    "mc25": "T3_4[8].DN",
    "mc26": "T3_4[9].DN",
    "mc27": "T3_4[10].DN",
    "mc28": "T3_4[11].DN",
    "mc29": "T3_4[12].DN",
    "mc30": "T3_4[13].DN",
}

status_columns = {
    "mc25": "status",
    "mc26": "status",
    "mc27": "status",
    "mc28": "state",
    "mc29": "state",
    "mc30": "status",
}

cld_columns = {
    "mc25": ("cld_count_a", "cld_count_b", "cld_count_c"),
    "mc26": ("cld_count_a", "cld_count_b", "cld_count_c"),
    "mc27": ("cld_count_a", "cld_count_b", "cld_count_c"),
    "mc28": ("cld_a", "cld_b", "cld_c"),
    "mc29": ("cld_a", "cld_b", "cld_c"),
    "mc30": ("cld_count_a", "cld_count_b", "cld_count_c"),
}

shift_tags = {
    "A": {
        "mc25": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(64, 72)],
        "mc26": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(72, 80)],
        "mc27": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(80, 88)],
        "mc28": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(88, 96)],
        "mc29": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(96, 104)],
        "mc30": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(104, 112)],
    },
    "B": {
        "mc25": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(192, 200)],
        "mc26": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(200, 208)],
        "mc27": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(208, 216)],
        "mc28": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(216, 224)],
        "mc29": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(224, 232)],
        "mc30": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(232, 240)],
    },
    "C": {
        "mc25": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(320, 328)],
        "mc26": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(328, 336)],
        "mc27": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(336, 344)],
        "mc28": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(344, 352)],
        "mc29": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(352, 360)],
        "mc30": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(360, 368)],
    },
}

# ---------------- HELPER FUNCTIONS ----------------
def get_last_non_null(cursor, table, columns):
    """Fetch last non-null row values from table"""
    col_list = ', '.join(columns)
    cursor.execute(
        f"SELECT {col_list} FROM {table} WHERE {columns[0]} IS NOT NULL "
        f"ORDER BY timestamp DESC LIMIT 1"
    )
    result = cursor.fetchone()
    if result:
        return dict(zip(columns, result))
    return {col: None for col in columns}


def insert_machine_data(cursor, table, status_col, cld_cols, status_value, cld_a, cld_b, cld_c):
    """Insert non-null machine data"""
    if None in (status_value, cld_a, cld_b, cld_c):
        print(f"⚠️  Skipping {table} — missing data")
        return False

    cursor.execute(
        f"""
        INSERT INTO {table} (timestamp, {status_col}, {cld_cols[0]}, {cld_cols[1]}, {cld_cols[2]})
        VALUES (%s, %s, %s, %s, %s)
        """,
        (datetime.now(), status_value, cld_a, cld_b, cld_c),
    )
    return True


# ---------------- MAIN LOOP ----------------
def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()

    with LogixDriver(PLC_IP) as plc:
        print(f"✅ Connected to PLC {PLC_IP}")

        try:
            while True:
                # ---- Read Machine Status ----
                status_results = plc.read(*mc_status_tags.values())
                status_map = {}
                for (table, tag), res in zip(mc_status_tags.items(), status_results):
                    if res.error is None:
                        # 1 = running, 0 = stopped (as per your logic)
                        status_map[table] = 1 if res.value is False else 0
                    else:
                        print(f"❌ {table.upper()} ({tag}) → Error: {res.error}")
                        status_map[table] = None

                # ---- Read Hourly Production ----
                cld_totals = {mc: {"A": 0, "B": 0, "C": 0} for mc in mc_status_tags.keys()}
                for shift, mc_tags in shift_tags.items():
                    for mc, tags in mc_tags.items():
                        res_list = plc.read(*tags)
                        if all(r.error is None for r in res_list):
                            cld_totals[mc][shift] = sum(r.value for r in res_list)
                        else:
                            print(f"⚠️ {mc.upper()} shift {shift} → Error in hourly read")
                            cld_totals[mc][shift] = None

                # ---- Insert Valid Data ----
                for mc, status_value in status_map.items():
                    # Fetch last valid non-null CLD if new one is None
                    cld_a, cld_b, cld_c = (
                        cld_totals[mc]["A"],
                        cld_totals[mc]["B"],
                        cld_totals[mc]["C"],
                    )

                    if None in (cld_a, cld_b, cld_c):
                        last = get_last_non_null(cursor, mc, cld_columns[mc])
                        cld_a = cld_a or last[cld_columns[mc][0]]
                        cld_b = cld_b or last[cld_columns[mc][1]]
                        cld_c = cld_c or last[cld_columns[mc][2]]

                    # Insert only if we have all data
                    inserted = insert_machine_data(
                        cursor,
                        mc,
                        status_columns[mc],
                        cld_columns[mc],
                        status_value,
                        cld_a,
                        cld_b,
                        cld_c,
                    )

                    if inserted:
                        print(
                            f"✅ {mc.upper()} → Status:{status_value}, "
                            f"A:{cld_a} B:{cld_b} C:{cld_c}"
                        )

                time.sleep(0.3)

        except KeyboardInterrupt:
            print("🛑 Stopped by user")
        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    main()

