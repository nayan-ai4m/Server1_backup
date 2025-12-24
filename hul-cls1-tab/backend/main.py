from fastapi import FastAPI, HTTPException, Request, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
from typing import List, Optional
from datetime import datetime, timedelta
import traceback
import psycopg2
import asyncio
import uvicorn
import os
from tpoints_events import fetch_tp_status_data, mark_tp_inactive
from database import get_db, close_db  

app = FastAPI()


origins = ["*"]  
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------- MODELS ----------------------------
class MachineStatus(BaseModel):
    id: str
    color: Optional[str] = None
    active: Optional[str] = None
    status: Optional[str] = None


# ---------------------------- ROUTES ----------------------------

@app.get("/tp-points")
async def get_tp_status(machine_id: str = Query(...,pattern=r"^(mc(25|26|27|28|29|30)|tpmc|highbay|press|ce|ckwr)$")):
    print(machine_id)
    try:
        data = fetch_tp_status_data(machine_id)
        print(data)
        return data
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": traceback.format_exc()}
        )



@app.get("/machine-status-loop4", response_model=List[MachineStatus])
def get_machine_status_loop4():
    try:
        conn, cursor = get_db()
        cursor.execute("""
            SELECT 
                machine_status::json->>'id' AS id,
                machine_status::json->>'color' AS color,
                machine_status::json->>'active' AS active,
                machine_status::json->>'status' AS status
            FROM overview_tp_status_loop4
        """)
        rows = cursor.fetchall()
        result = [
            {"id": row[0], "color": row[1], "active": row[2], "status": row[3]}
            for row in rows
        ]
        return result
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": traceback.format_exc()}
        )
    finally:
        close_db(conn, cursor)


@app.post("/clear-notification-loop4")
async def clear_notification(payload: dict):
    try:
        machine = payload["machine"]
        uuid = payload["uuid"]
        updated_time = payload.get("updated_time")

        result = mark_tp_inactive(machine, uuid, updated_time)

        print("Cleared Notification:", payload)
        return {"status": "success", "cleared_id": payload["id"], "updated": result}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": traceback.format_exc()}
        )


# ---------------------------- ENTRY POINT ----------------------------

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
