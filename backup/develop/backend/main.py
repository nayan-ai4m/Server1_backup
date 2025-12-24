from fastapi import FastAPI, HTTPException,Request,Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import json
import traceback
import uvicorn
from fastapi.responses import JSONResponse,StreamingResponse
from datetime import datetime,timedelta
from typing import List,Dict
from fastapi import WebSocket
from fastapi import WebSocketDisconnect
import asyncio
from psycopg2.extras import RealDictCursor  

with open('db_config.json', 'r') as config_file:
    config = json.load(config_file)

DATABASE = config['database']
DATABASE2 = config['database1']


app = FastAPI()
origins = ["*"]  
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


active_connections = {}
offline_notifications: Dict[str, List[Dict[str, any]]] = {}


def get_db():
    try:
        conn = psycopg2.connect(**DATABASE)
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        print(f"Error connecting to main database: {e}")
        print(traceback.format_exc())
        return None, None

def get_device_db():
    try:
        conn2 = psycopg2.connect(**DATABASE2)
        cursor2 = conn2.cursor()
        return conn2, cursor2
    except Exception as e:
        print(f"Error connecting to secondary database: {e}")
        print(traceback.format_exc())
        return None, None

def close_db(conn, cursor):
    """ Close the main database connection and cursor. """
    if cursor:
        cursor.close()
    if conn:
        conn.close()

def close_device_db(conn2, cursor2):
    """ Close the secondary database connection and cursor. """
    if cursor2:
        cursor2.close()
    if conn2:
        conn2.close()


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class NotificationQueue:
    def __init__(self, max_age_hours=72):
        self.queues: Dict[str, List[Dict]] = {}
        self.max_age = max_age_hours

    def add_notification(self, user_id: str, notification: Dict):
        """Add a notification to a user's queue with timestamp"""
        if user_id not in self.queues:
            self.queues[user_id] = []
        
        notification['queued_at'] = datetime.now()
        self.queues[user_id].append(notification)
        self._cleanup_old_notifications(user_id)

    def get_notifications(self, user_id: str) -> List[Dict]:
        """Retrieve and clear notifications for a user"""
        if user_id not in self.queues:
            return []
        
        self._cleanup_old_notifications(user_id)
        notifications = self.queues[user_id]
        self.queues[user_id] = []
        return notifications

    def _cleanup_old_notifications(self, user_id: str):
        """Remove notifications older than max_age"""
        if user_id not in self.queues:
            return
        
        current_time = datetime.now()
        self.queues[user_id] = [
            notif for notif in self.queues[user_id]
            if (current_time - notif['queued_at']) < timedelta(hours=self.max_age)
        ]


notification_queue = NotificationQueue()

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        self.active_connections[user_id].append(websocket)

    def disconnect(self, websocket: WebSocket, user_id: str):
        self.active_connections[user_id].remove(websocket)
        if not self.active_connections[user_id]:
            del self.active_connections[user_id]

    async def send_personal_notification(self, user_id: str, message: Dict[str, any]):
        if user_id in self.active_connections:
            for connection in self.active_connections[user_id]:
                await connection.send_json(message)

manager = ConnectionManager()





#Notification Logic

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    try:
        await manager.connect(websocket, user_id)
        if user_id in offline_notifications:
            for notification in offline_notifications[user_id]:
                await websocket.send_json(notification)
            del offline_notifications[user_id]

        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket, user_id)

@app.post("/assign-task/")
async def assign_task(request: Request):
    try:
        data = await request.json()
        assignee = data.get('assignee')
        task_description = data.get('task_description')
        event_id = data.get('event_id')
        alert_type = data.get('alert_type')

        if not all([assignee, task_description, event_id]):
            return JSONResponse(
                content={"error": "Missing required fields"},
                status_code=400
            )


        conn,cursor = get_db()
        if not conn or not cursor:
            return JSONResponse(
                content={"error": "Database connection failed"},
                status_code=500
            )

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:

                query = """
                UPDATE event_table
                SET
                    assigned_to = %s,
                    action = 'Assigned',
                    remark = %s,
                    event_type = %s,
                    alert_type = %s,
                    timestamp = %s
                WHERE event_id = %s
                RETURNING *
                """

                unique_timestamp = datetime.now()
                cursor.execute(
                    query,
                    (
                        assignee,
                        task_description,
                        task_description,
                        alert_type,
                        unique_timestamp,
                        event_id,
                    )
                )


                updated_record = cursor.fetchone()
                conn.commit()

                notification = {
                    "type": "assignment",
                    "event_id": event_id,
                    "task_description": task_description,
                    "assigned_at": unique_timestamp.isoformat(),
                    "alert_type": alert_type,
                    "assignee": assignee
                }


                if assignee in manager.active_connections:

                    await manager.send_personal_notification(assignee, notification)
                else:

                    if assignee not in offline_notifications:
                        offline_notifications[assignee] = []
                    offline_notifications[assignee].append(notification)


                if updated_record:
                    serializable_record = {}
                    for key, value in updated_record.items():
                        if isinstance(value, datetime):
                            serializable_record[key] = value.isoformat()
                        else:
                            serializable_record[key] = value

                    return JSONResponse(
                        content={
                            "message": "Task assigned successfully",
                            "assigned_record": serializable_record,
                            "notification": notification
                        },
                        status_code=200
                    )
                else:
                    return JSONResponse(
                        content={"error": f"No event found with ID: {event_id}"},
                        status_code=404
                    )

        except Exception as e:
            conn.rollback()
            print(f"Database error: {e}")
            return JSONResponse(
                content={"error": "Database update failed", "details": str(e)},
                status_code=500
            )
        finally:

            conn.close()

    except Exception as e:
        print(f"Unexpected error: {e}")
        return JSONResponse(
            content={"error": "Unexpected server error", "details": str(e)},
            status_code=500
        )


@app.get("/notifications/{user_id}")
async def get_pending_notifications(user_id: str):
    print("user id --->>>", user_id)
    if user_id in offline_notifications:
        notifications = offline_notifications[user_id]
        print("notification", notifications)
        del offline_notifications[user_id]
        return JSONResponse(content={"notifications": notifications}, status_code=200)
    return JSONResponse(content={"notifications": []}, status_code=200)

def fetch_loop3_data():
    query = """
    SELECT last_update, primary_tank_level, secondary_tank_level, bulk_settling_time,
           ega, cld_production, taping_machine, case_erector,plantair,planttemperature
    FROM loop3_overview 
    ORDER BY last_update DESC
    LIMIT 1
    """
    
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Main database connection error")

    try:
        cursor.execute(query)
        result = cursor.fetchone()
    except Exception as e:
        print(f"Error fetching data from loop3_overview: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching data from loop3_overview")
    finally:
        close_db(conn, cursor)

    if result:
        return {
            "last_update": result[0],
            "primary_tank_level": result[1],
            "secondary_tank_level": result[2],
            "bulk_settling_time": result[3],
            "ega": result[4],
            "cld_production": result[5],
            "taping_machine": result[6],
            "case_erector": result[7],
            "plantair":result[8],
            "planttemperature":result[9]
        }
    return {}


def fetch_loop4_data():
    query = """
    SELECT last_update, primary_tank_level, secondary_tank_level, bulk_settling_time,
           ega, cld_production, taping_machine, case_erector 
    FROM loop4_overview
    ORDER BY last_update DESC
    LIMIT 1
    """
    
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Secondary database connection error")

    try:
        cursor.execute(query)
        result = cursor.fetchone()
    except Exception as e:
        print(f"Error fetching data from loop4_overview: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching data from loop4_overview")
    finally:
        close_db(conn, cursor)

    if result:
        return {
            "last_update": result[0],
            "primary_tank_level": result[1],
            "secondary_tank_level": result[2],
            "bulk_settling_time": result[3],
            "ega": result[4],
            "cld_production": result[5],
            "taping_machine": result[6],
            "case_erector": result[7]
        }
    return {}
"""
machine table data  
"""


def fetch_machine_data():
    query = """
    SELECT 
        last_update,
        machines
    FROM {table_name}
    ORDER BY last_update DESC
    LIMIT 1
    """
    
    data = {}
    for table_name in ["loop3_overview", "loop4_overview"]:
        conn, cursor = get_db()
        if not conn or not cursor:
            raise HTTPException(status_code=500, detail="Database connection error")
        
        try:
            cursor.execute(query.format(table_name=table_name))
            result = cursor.fetchone()
            
            if result:
                last_update, machines_json = result
                
                if isinstance(machines_json, str):
                    machines_data = json.loads(machines_json)
                elif isinstance(machines_json, dict):
                    machines_data = machines_json
                else:
                    raise TypeError("machines_json is neither a string nor a dictionary")
                data[table_name] = {
                    "last_update": last_update,
                    "machines": machines_data
                }
                
        except Exception as e:
            print(f"Error fetching data from {table_name}: {e}")
            print(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error fetching data from {table_name}")
        finally:
            close_db(conn, cursor)
    
    return data
"""
"""
def fetch_device_graph_data(limit=24):
    """Fetch telemetry data for a device from the database."""
    conn2, cursor2 = get_device_db()  
    try:
        query = """
        WITH ranked_data AS (
            SELECT
                device_id,
                timestamp,
                x_rms_vel,
                y_rms_vel,
                z_rms_vel,
                spl_db,
                temp_c,
                label,
                ROW_NUMBER() OVER (PARTITION BY label ORDER BY timestamp DESC) AS row_num
            FROM public.api_timeseries
        )
        SELECT
            device_id,
            timestamp,
            x_rms_vel,
            y_rms_vel,
            z_rms_vel,
            spl_db,
            temp_c,
            label
        FROM ranked_data
        WHERE row_num = 1
        ORDER BY timestamp DESC, label DESC
        LIMIT %s;
        """
        cursor2.execute(query, (limit,))
        result = cursor2.fetchall()
        print("Device Data:", result)
        
        if result:
            data = [
                {
                    "device_id": row[0],
                    "timestamp": row[1],
                    "x_rms_vel": row[2],
                    "y_rms_vel": row[3],
                    "z_rms_vel": row[4],
                    "spl_db": row[5],
                    "temp_c": row[6],
                    "label": row[7]
                } for row in result
            ]
        else:
            data = []

        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
    finally:
        close_db(conn2, cursor2)  


"""
Last 5 mins record fetch
"""
def fetch_last_5mins_data(machine_name: list = None, title_condition: str = ""):
    """Fetch the last 50 telemetry records, filtered by machine numbers and label contents."""
    conn2, cursor2 = get_device_db()
    try:
        if machine_name:
            machine_conditions = " OR ".join([f"label ILIKE '%{num}%'" for num in machine_name])
            query = f"""
            WITH ranked_data AS (
                SELECT
                    x_rms_vel,
                    y_rms_vel,
                    z_rms_vel,
                    label,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY label ORDER BY timestamp DESC) AS row_num
                FROM
                    api_timeseries
                WHERE
                    ({machine_conditions}) -- Dynamically include the machine numbers
                    AND (label ILIKE '%Vertical%' OR label ILIKE '%Horizontal%') -- Include labels with Vertical or Horizontal
                    {title_condition}  -- Apply the title filter if provided
            )
            SELECT
                x_rms_vel,
                y_rms_vel,
                z_rms_vel,
                label,
                timestamp
            FROM
                ranked_data
            WHERE
                row_num <= 50
            ORDER BY
                label, timestamp DESC;
            """
        else:
            query = f"""
            WITH ranked_data AS (
                SELECT
                    x_rms_vel,
                    y_rms_vel,
                    z_rms_vel,
                    label,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY label ORDER BY timestamp DESC) AS row_num
                FROM
                    api_timeseries
                WHERE
                    label ILIKE '%Vertical%' OR label ILIKE '%Horizontal%' -- Only Vertical or Horizontal labels
                    {title_condition}  -- Apply the title filter if provided
            )
            SELECT
                x_rms_vel,
                y_rms_vel,
                z_rms_vel,
                label,
                timestamp
            FROM
                ranked_data
            WHERE
                row_num <= 50
            ORDER BY
                label, timestamp DESC;
            """
        cursor2.execute(query)

        result = cursor2.fetchall()

        if result:
            data = [
                {
                    "x_rms_vel": row[0],
                    "y_rms_vel": row[1],
                    "z_rms_vel": row[2],
                    "label": row[3],
                    "timestamp": row[4],
                }
                for row in result
            ]
        else:
            data = []

        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching telemetry data: {str(e)}")
    finally:
        close_db(conn2, cursor2)

"""
Event  
"""
def fetch_today_events():
    today = datetime.now().date()
    query = """
    SELECT * FROM event_table  ORDER BY timestamp DESC
    
    """
    #WHERE DATE(event_date) = %s
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query, (today,))
        result = cursor.fetchall()
        print("Result",result)
    except Exception as e:
        print(f"Error fetching data from event_table: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching data from event_table")
    finally:
        close_db(conn, cursor)

    events = [
        {
            "timestamp":row[0],
            "event_id": row[1],  
            "zone": row[2],
            "camera_id": row[3],
            "assigned_to":row[4],
            "action":row[5],
            "remark":row[6],
            "resolution_time":row[7],
            "event_type":row[9],
            "alert_type": row[10]
            
        }
        for row in result
    ]
    return events

def fetch_latest_event():
    query = """SELECT * FROM event_table
    WHERE assigned_to IS NULL
    ORDER BY timestamp DESC
    LIMIT 1"""
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        result = cursor.fetchone()  
        
        
        if result is None:
            
            count_cursor = conn.cursor()
            try:
                count_cursor.execute("SELECT COUNT(*) FROM event_table")
                total_events = count_cursor.fetchone()[0]
                
                if total_events > 0:
                    return {"message": "All events are assigned"}
                else:
                    return {"message": "No events found"}
            finally:
                count_cursor.close()  
        else:
        
            event = {
                "timestamp": result[0],
                "event_id": result[1],
                "zone": result[2],
                "camera_id": result[3],
                "assigned_to": result[4],
                "action": result[5],
                "remark": result[6],
                "resolution_time": result[7],
                "event_type": result[8],  
                "alert_type": result[9]   
            }
            return event
    except Exception as e:
        print(f"Error fetching data from event_table: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching data from event_table")
    finally:
        close_db(conn, cursor)

def fetch_machine_setpoints_data(machine_id):
    # machine_columns = [f"mc{i}" for i in range(17, 31) if i not in (23, 24)]
    # column_names = ", ".join(machine_columns)  # Join column names for the query
    
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    machine_data = {}

    try:
        
        cursor.execute(f"SELECT {machine_id} FROM cls1_setpoints LIMIT 1")
        result = cursor.fetchone()
        
        if result: 
            machine_data = result[0] if result[0] is not None else "No data available"
        else:
            raise HTTPException(status_code=404, detail="No data available in cls1_setpoints")
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from cls1_setpoints")
    finally:
        close_db(conn, cursor)

    return machine_data

def fetch_recent_machine_setpoints(machine_id):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    last_5_values = []
    try:
        
        query = f"""
            SELECT {machine_id}
            FROM cls1_setpoints
            WHERE {machine_id} IS NOT NULL
            ORDER BY row_number() OVER () DESC  -- Emulate recency
            LIMIT 5
        """
        cursor.execute(query)
        results = cursor.fetchall()
        for result in results:
            last_5_values.append(result[0])
        last_5_values.reverse()

        if not last_5_values:
            raise HTTPException(status_code=404, detail="No recent data available in cls1_setpoints")

    except Exception as e:
        print(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from cls1_setpoints")
    finally:
        close_db(conn, cursor)

    return last_5_values

def fetch_horizontal_sealer_graph(machine_id):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        
        query = f"""
            SELECT 
                {machine_id}->'time_bucket' AS time_bucket,
                {machine_id}->'hor_front1' AS hor_sealer_front1,
                {machine_id}->'hor_rear1' AS hor_sealer_rear1
            FROM historical_graphs 
            WHERE {machine_id} IS NOT NULL;
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            raise HTTPException(status_code=404, detail="No data available in historical_graph")

        historical_data = {
            "time_bucket": [],
            "hor_sealer_front1": [],
            "hor_sealer_rear1": []
        }

        for row in results:
            historical_data["time_bucket"].extend(row[0])  
            historical_data["hor_sealer_front1"].extend(row[1]) 
            historical_data["hor_sealer_rear1"].extend(row[2])  

        # print("Historical Data:", historical_data)
        return historical_data

    except Exception as e:
        print(f"Error fetching historical data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from historical_graph")
    finally:
        close_db(conn, cursor)


def fetch_hopper_data(machine_id):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        query = f"""
            SELECT 
                {machine_id}->'time_bucket' AS time_bucket,
                {machine_id}->'hopper_1' AS hopper_1,
                {machine_id}->'hopper_2' AS hopper_2
            FROM historical_graphs 
            WHERE {machine_id} IS NOT NULL;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        if not results:
            raise HTTPException(status_code=404, detail="No hopper data available")

        hopper_data = {
            "time_bucket": [],
            "hopper_1": [],
            "hopper_2": []
        }

        for row in results:
            hopper_data["time_bucket"].extend(row[0])  
            hopper_data["hopper_1"].extend(row[1])  
            hopper_data["hopper_2"].extend(row[2])  

        # print("Hopper Data:", hopper_data)
        return hopper_data

    except Exception as e:
        print(f"Error fetching hopper data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from historical_graphs")
    finally:
        close_db(conn, cursor)


def fetch_vertical_sealer_full_graph(machine_id):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:

        front_columns = ", ".join([f"{machine_id}->>'front{i}' AS front{i}" for i in range(1, 14)])
        rear_columns = ", ".join([f"{machine_id}->>'rear{i}' AS rear{i}" for i in range(1, 14)])

        query = f"""
            SELECT 
                {machine_id}->'time_bucket' AS time_bucket,
                {front_columns},
                {rear_columns}
            FROM historical_graphs 
            WHERE {machine_id} IS NOT NULL;
        """

        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            raise HTTPException(status_code=404, detail="No data available in historical_graph")
        vertical_data = {
            "time_bucket": [],
            **{f"front{i}": [] for i in range(1, 14)},
            **{f"rear{i}": [] for i in range(1, 14)}
        }

    
        for row in results:
            vertical_data["time_bucket"].append(row[0])  

            for i in range(1, 14):
            
                try:
                    front_values = json.loads(row[i]) if row[i] else []
                    rear_values = json.loads(row[13 + i]) if row[13 + i] else []
                except json.JSONDecodeError:
                    front_values = rear_values = []

        
                vertical_data[f"front{i}"].extend(front_values)
                vertical_data[f"rear{i}"].extend(rear_values)

        # print("Full Vertical Data:", vertical_data)
        return vertical_data

    except Exception as e:
        print(f"Error fetching full vertical data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from historical_graph")
    finally:
        close_db(conn, cursor)


subscribers = {}

@app.post("/close-task/")
async def close_task(request: Request):
    data = await request.json()
    print("data", data)  
    event_id = data.get("event_id")  
    close_time = data.get("closeTime")
    action_remarks = data.get("actionRemarks")  

    if close_time is None:
        return JSONResponse(content={"error": "Missing close time"}, status_code=400)
    if event_id is None:
        return JSONResponse(content={"error": "Missing event ID"}, status_code=400)

    conn, cursor = get_db()
    if not conn or not cursor:
        return JSONResponse(content={"error": "Database connection error"}, status_code=500)

    query = """
    UPDATE event_table
    SET action = 'Closed', resolution_time = CURRENT_DATE + %s::time, remark = %s
    WHERE event_id = %s
    """
    cursor.execute(query, (close_time, action_remarks,event_id))
    conn.commit()
    close_db(conn, cursor)
    for subscriber in app.subscribers:
        await subscribers[subscriber].put({
            "type": "closure",
            "eventId": event_id,
            "closeTime": close_time,
            "actionRemarks": action_remarks
        })

    return JSONResponse(content={"message": "Task closed successfully"})
"""
@app.post("/assign-task/")
async def assign_task(request: Request):
    data = await request.json()
    assignee = data.get("assignee")
    task_description = data.get("task_description")
    event_id = data.get("event_id")  
    alert_type = data.get("alert_type")

    if not assignee or not task_description or not event_id:
        return JSONResponse(content={"error": "Missing assignee, task description, or event ID"}, status_code=400)

    conn, cursor = get_db()
    if not conn or not cursor:
        return JSONResponse(content={"error": "Database connection error"}, status_code=500)
    query = 
    UPDATE event_table
    SET assigned_to = %s, action = 'Assigned', remark = %s, event_type =  %s, alert_type = %s, timestamp = %s
    WHERE event_id = %s


    unique_timestamp = datetime.now()

    cursor.execute(query, (assignee, task_description, task_description, alert_type, unique_timestamp, event_id))
    conn.commit()
    close_db(conn, cursor)

    if assignee in subscribers:
        await subscribers[assignee].put({
            "type": "assignment",
            "assignee": assignee,
            "taskDescription": task_description
        })

    return JSONResponse(content={"message": "Task updated and notification sent if user is connected"})
"""

@app.get("/sse/notifications/")
async def sse_notifications(username: str):
    print(username)
    async def event_stream():
        queue = asyncio.Queue()
        subscribers[username] = queue

        try:
            while True:
                message = await queue.get()
                yield f"data: {json.dumps(message)}\n\n"
        except asyncio.CancelledError:
            print(traceback.format_exc())
            subscribers.pop(username, None)
    
    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/loopoverview")
async def get_loop_overview():
    try:
        loop3_data = fetch_loop3_data()
        loop4_data = fetch_loop4_data()

        response = {
            "loop3": loop3_data,
            "loop4": loop4_data
        }
        # print("Response Loop Overview", response)
        return response
    except Exception as e:
        print(f"Error in /loopoverview endpoint: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching loop overview data")
    
@app.get("/machinetabledata")
async def get_machine_table_data():
    try:
        machine_data = fetch_machine_data()
        response = {
            "loop3": machine_data.get("loop3_overview", {}),
            "loop4": machine_data.get("loop4_overview", {})
        }
        # print("Response Machine Table Data", response)
        return response
    except Exception as e:
        print(f"Error in /machinetabledata endpoint: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching machine table data")

@app.get("/fetch-device-data")
async def fetch_device_data(request: Request, recent: bool = True):
    """
    Fetch the most recent record for a given device. 
    If no recent data is found, fetch the last 100 records.
    """
    try:
        # Fetch the most recent or last 100 records
        if recent:
            data = fetch_device_graph_data(limit=24)
            if not data:
                data = fetch_device_graph_data(limit=24)
        else:
            data = fetch_device_graph_data(limit=24)

        if not data:
            raise HTTPException(status_code=404, detail="No data found for the given device.")

        final_response = [
            {
                "machine_id": item["label"],
                "x_rms_vel": item["x_rms_vel"],
                "y_rms_vel": item["y_rms_vel"],
                "z_rms_vel": item["z_rms_vel"],
                "spl_db": item["spl_db"],
                "temp_c": item["temp_c"]
            } for item in data
        ]

        return JSONResponse(content={"data": final_response})

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

"""
@app.get("/fetch-last-5mins-data")
async def fetch_last_5mins_data_endpoint(request: Request):

    #Fetch the telemetry data for the last 5 minutes.

    try:
        # Fetch data for the last 5 minutes
        data = fetch_last_5mins_data()

        if not data:
            raise HTTPException(status_code=404, detail="No data found for the last 5 minutes.")

        # Create the final JSON structure with the machine label
        final_response = [
            {
                "machine_id": item["label"],
                "x_rms_vel": item["x_rms_vel"],
                "y_rms_vel": item["y_rms_vel"],
                "z_rms_vel": item["z_rms_vel"],
                "spl_db": item["spl_db"],
                "temp_c": item["temp_c"]
            } for item in data
        ]

        return JSONResponse(content={"data": final_response})

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))    

"""

@app.get("/fetch-last-5mins-data")
async def fetch_last_5mins_data_endpoint(request: Request, machine_name: str = None, title: str = None):
    """
    Fetch telemetry data for the last 5 minutes, optionally filtered by machine numbers and title.
    """
    try:
        machine_name_list = machine_name.split(",") if machine_name else None

        title_condition = ""
        if title:
            title_condition = f" AND label ILIKE '%{title}%'"

        data = fetch_last_5mins_data(machine_name_list, title_condition)

        if not data:
            raise HTTPException(status_code=404, detail="No data found for the given parameters.")

        final_response = [
            {
                "machine_id": item["label"],
                "timestamp": item["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                "x_rms_vel": item["x_rms_vel"],
                "y_rms_vel": item["y_rms_vel"],
                "z_rms_vel": item["z_rms_vel"],
            }
            for item in data
        ]

        return JSONResponse(content={"data": final_response})

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/get_latest_event")
async def get_latest_event():
    try:
        event = fetch_latest_event()
        return {"event": event}
    except HTTPException as e:
        raise e  
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Unexpected error fetching the latest event")

@app.get("/get_today_events")
async def get_today_events():
    try:
        events = fetch_today_events()
        return {"events": events}
    except HTTPException as e:
        raise e  
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Unexpected error fetching today's events")
    
@app.get("/machine_setpoints")
async def get_machine_setpoints(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    data = fetch_machine_setpoints_data(machine_id)
    return data

@app.get("/sealar_front_temperature")
async def get_sealar_front_temperature(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    data = fetch_recent_machine_setpoints(machine_id)
    # print("data",data)
    return JSONResponse(content=data)

"""
@app.get("/horizontal_sealer_graph")
async def get_horizontal_graph(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    data = fetch_horizontal_sealer_graph(machine_id)
    # print("hor",data)
    return data
@app.get("/verticle_sealer_graph")
async def get_verticle_sealer_graph(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    data = fetch_vertical_sealer_full_graph(machine_id)
    # print("ver",data)
    return data
@app.get("/hopper_level_graph")
async def get_machine_setpoints(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
    data = fetch_hopper_data(machine_id)
    # print("verticle_sealer_graph",data)
    return data
"""

@app.put("/update_setpoint")
async def update_setpoint(machine_id: str, key: str, value: str):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")
    
    machine_column = machine_id.lower()  # Ensure the machine ID is lowercase for consistency
    # print("machine_column:",machine_column)
    if machine_column not in [f"mc{i}" for i in range(17, 31) if i not in (23, 24)]:
        raise HTTPException(status_code=400, detail="Invalid machine ID")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)

