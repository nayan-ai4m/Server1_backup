import json
import os
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
import traceback
from database import get_db, close_db
from psycopg2 import sql

# === Load TP Description ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TP_DESCRIPTION_FILE = os.path.join(BASE_DIR, "touchpoints.json")

with open(TP_DESCRIPTION_FILE, "r") as f:
    raw_descriptions = json.load(f)
    TP_DETAILS = {
        key.lower(): {
            "title": value.get("title", ""),
            "instruction": value.get("instruction", ""),
            "hasVisible": value.get("hasVisible")
        }
        for item in raw_descriptions for key, value in item.items()
    }

# === Table Mapping Logic ===
def get_loop4_table_name(machine_id: str) -> str:
    machine_id = machine_id.lower().replace(" ", "")

    predefined_tables = {
        "tpmc": "tpmc_tp_status_loop4",
        "press": "press_tp_status_loop4",
        "ce": "case_erector_tp_status_loop4",
        "ckwr": "check_weigher_tp_status_loop4",
        "highbay" : "highbay_tp_status_loop4"
       
    }

    if machine_id in predefined_tables:
        return predefined_tables[machine_id]
    
    if machine_id.startswith("mc") and machine_id[2:].isdigit():
        mc_num = int(machine_id[2:])
        if 25 <= mc_num <= 30:
            return f"{machine_id}_tp_status"

    raise HTTPException(status_code=400, detail="Unsupported machine ID for Loop 4")

# === Fetch Active TP Data ===
def fetch_tp_status_data(machine_id: str) -> List[Dict[str, Any]]:
    table_name = get_loop4_table_name(machine_id)
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        # Fetch TP columns
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,)
        )
        columns_data = cursor.fetchall()
        tp_keys = [col[0] for col in columns_data if col[0].startswith("tp")]

        if not tp_keys:
            raise HTTPException(status_code=404, detail="No TP columns found")

        # WHERE clause to get active TPs
        where_clauses = [
            sql.SQL("({} ->> 'active')::int = 1").format(sql.Identifier(col))
            for col in tp_keys
        ]
        where_sql = sql.SQL(" OR ").join(where_clauses)

        query = sql.SQL("SELECT * FROM {} WHERE {}").format(
            sql.Identifier(table_name),
            where_sql
        )
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        result = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            for tp_key in tp_keys:
                tp_data = row_dict.get(tp_key)
                if isinstance(tp_data, dict) and tp_data.get("active") == 1:
                    result.append({
                        "id": tp_key,
                        "uuid": tp_data.get("uuid"),
                        "filepath": tp_data.get("filepath"),
                        "timestamp": tp_data.get("timestamp"),
                        "color_code": tp_data.get("color_code"),
                        "title": TP_DETAILS.get(tp_key, {}).get("title", ""),
                        "instruction": TP_DETAILS.get(tp_key, {}).get("instruction", ""),
                        "hasVisible": TP_DETAILS.get(tp_key, {}).get("hasVisible")
                    })

        result.sort(key=lambda x: x.get("timestamp"), reverse=True)
        return result

    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching TP data")

    finally:
        close_db(conn, cursor)

# === Mark TP Inactive by UUID ===
def mark_tp_inactive(machine_id: str, target_uuid: str, updated_time: Optional[str] = None) -> Dict[str, Any]:
    table_name = get_loop4_table_name(machine_id)
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        # Get TP columns
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,)
        )
        tp_columns = [row[0] for row in cursor.fetchall() if row[0].startswith("tp")]

        for tp_col in tp_columns:
            cursor.execute(
                sql.SQL("SELECT {col} FROM {table} LIMIT 1").format(
                    col=sql.Identifier(tp_col),
                    table=sql.Identifier(table_name)
                )
            )
            row = cursor.fetchone()
            if not row:
                continue

            data = row[0]
            if data and isinstance(data, dict) and data.get("uuid") == target_uuid:
                data["active"] = 0
                if updated_time:
                    data["updated_time"] = updated_time

                cursor.execute(
                    sql.SQL("UPDATE {table} SET {col} = %s").format(
                        table=sql.Identifier(table_name),
                        col=sql.Identifier(tp_col)
                    ),
                    (json.dumps(data),)
                )
                conn.commit()
                print(f"TP column {tp_col} for machine {machine_id} marked inactive.")
                return {"status": "success", "column": tp_col, "machine": machine_id}

        raise HTTPException(status_code=404, detail="UUID not found in any TP column")

    except Exception as e:
        print(f"Error updating uuid {target_uuid} in {table_name}: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error marking TP inactive")

    finally:
        close_db(conn, cursor)
