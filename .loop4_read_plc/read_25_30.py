import time
from datetime import datetime
import psycopg2
from pycomm3 import LogixDriver

# PLC IP
PLC_IP = "141.141.143.51"

# Database connection details
DB_CONFIG = {
    "dbname": "hul",
    "user": "postgres",
    "password": "ai4m2024",
    "host": "localhost",
    "port": 5432,
}

# Status Tags mapping
mc_status_tags = {
    "mc25": "T3_4[8].DN",
    "mc26": "T3_4[9].DN",
    "mc27": "T3_4[10].DN",
    "mc28": "T3_4[11].DN",
    "mc29": "T3_4[12].DN",
    "mc30": "T3_4[13].DN",
}

# Column mapping for status/state
status_columns = {
    "mc25": "status",
    "mc26": "status",
    "mc27": "status",
    "mc28": "state",
    "mc29": "state",
    "mc30": "status",
}

# Column mapping for CLD counts
cld_columns = {
    "mc25": ("cld_count_a", "cld_count_b", "cld_count_c"),
    "mc26": ("cld_count_a", "cld_count_b", "cld_count_c"),
    "mc27": ("cld_count_a", "cld_count_b", "cld_count_c"),
    "mc28": ("cld_a", "cld_b", "cld_c"),
    "mc29": ("cld_a", "cld_b", "cld_c"),
    "mc30": ("cld_count_a", "cld_count_b", "cld_count_c"),
}

# Hourly Production Tags
shift_tags = {
    "A": {  # Shift A (indices 64–111)
        "mc25": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(64, 72)],
        "mc26": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(72, 80)],
        "mc27": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(80, 88)],
        "mc28": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(88, 96)],
        "mc29": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(96, 104)],
        "mc30": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(104, 112)],
    },
    "B": {  # Shift B (indices 192–239)
        "mc25": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(192, 200)],
        "mc26": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(200, 208)],
        "mc27": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(208, 216)],
        "mc28": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(216, 224)],
        "mc29": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(224, 232)],
        "mc30": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(232, 240)],
    },
    "C": {  # Shift C (indices 320–367)
        "mc25": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(320, 328)],
        "mc26": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(328, 336)],
        "mc27": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(336, 344)],
        "mc28": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(344, 352)],
        "mc29": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(352, 360)],
        "mc30": [f"LOOP3_4_HOURLYDATA.LOOP3_4[{i}]" for i in range(360, 368)],
    },
}


def insert_machine_data(cursor, table, status_col, cld_cols, status_value, cld_a, cld_b, cld_c):
    """Insert machine status + CLD counts into table"""
    cursor.execute(
        f"""
        INSERT INTO {table} (timestamp, {status_col}, {cld_cols[0]}, {cld_cols[1]}, {cld_cols[2]})
        VALUES (%s, %s, %s, %s, %s)
        """,
        (datetime.now(), status_value, cld_a, cld_b, cld_c),
    )


def main():
    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()

    with LogixDriver(PLC_IP) as plc:
        print(f"Connected to PLC {PLC_IP}")

        try:
            while True:
                # ---- Machine Status ----
                status_results = plc.read(*mc_status_tags.values())
                status_map = {}
                for (table, tag), res in zip(mc_status_tags.items(), status_results):
                    if res.error is None:
                        status_map[table] = 1 if res.value is False else 0
                    else:
                        status_map[table] = None
                        print(f"{table.upper()} ({tag}) → Error: {res.error}")

                # ---- Hourly Production (Shift Totals) ----
                cld_totals = {mc: {"A": 0, "B": 0, "C": 0} for mc in mc_status_tags.keys()}

                for shift, mc_tags in shift_tags.items():
                    for mc, tags in mc_tags.items():
                        res_list = plc.read(*tags)
                        if all(r.error is None for r in res_list):
                            total = sum(r.value for r in res_list)
                            cld_totals[mc][shift] = total
                        else:
                            cld_totals[mc][shift] = None  # Set to None if any tag read fails
                            print(f"{mc.upper()} shift {shift} → Error in hourly read")

                # ---- Insert into DB ----
                for mc, status_value in status_map.items():
                    # Check if status and all CLD totals are not None
                    if (
                        status_value is not None
                        and cld_totals[mc]["A"] is not None
                        and cld_totals[mc]["B"] is not None
                        and cld_totals[mc]["C"] is not None
                    ):
                        insert_machine_data(
                            cursor,
                            mc,
                            status_columns[mc],
                            cld_columns[mc],
                            status_value,
                            cld_totals[mc]["A"],
                            cld_totals[mc]["B"],
                            cld_totals[mc]["C"],
                        )
                        print(
                            f"{mc.upper()} → Status: {status_value}, "
                            f"A:{cld_totals[mc]['A']} B:{cld_totals[mc]['B']} C:{cld_totals[mc]['C']}"
                        )
                    else:
                        print(
                            f"{mc.upper()} → Skipped insertion due to NULL data: "
                            f"Status: {status_value}, "
                            f"A:{cld_totals[mc]['A']}, B:{cld_totals[mc]['B']}, C:{cld_totals[mc]['C']}"
                        )

                time.sleep(0.30)

        except KeyboardInterrupt:
            print("Stopped by user")

        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    main()
