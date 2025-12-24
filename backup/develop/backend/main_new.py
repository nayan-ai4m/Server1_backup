from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg2
import json
import traceback
import uvicorn
import asyncio
from datetime import datetime
from typing import Dict

# Load database configuration
with open('db_config.json', 'r') as config_file:
    config = json.load(config_file)

DATABASE = config['database']
DATABASE1 = config['database1']
class Database:
    @staticmethod
    def get_connection():
        """ Returns a new database connection and cursor. """
        try:
            conn = psycopg2.connect(**DATABASE)
            return conn, conn.cursor()
        except Exception as e:
            print(f"Error connecting to database: {e}")
            print(traceback.format_exc())
            return None, None

    @staticmethod
    def close_connection(conn, cursor):
        """ Closes the database connection and cursor. """
        if cursor:
            cursor.close()
        if conn:
            conn.close()
     

    @staticmethod
    def get_device_db():
        """ Returns a new database connection and cursor. """
        try:
            conn = psycopg2.connect(**DATABASE1)
            return conn, conn.cursor()
        except Exception as e:
            print(f"Error connecting to database: {e}")
            print(traceback.format_exc())
            return None, None

class NotificationManager:
    subscribers: Dict[str, asyncio.Queue] = {}

    @classmethod
    async def notify_subscribers(cls, username: str, message: dict):
        if username in cls.subscribers:
            await cls.subscribers[username].put(message)

class EventManager:
    @staticmethod
    async def close_task(data: dict):
        event_id = data.get("event_id")
        close_time = data.get("closeTime")
        action_remark = data.get("actionRemarks")
        if close_time is None or event_id is None:
            raise HTTPException(status_code=400, detail="Missing close time or event ID")

        conn, cursor = Database.get_connection()
        if not conn or not cursor:
            raise HTTPException(status_code=500, detail="Database connection error")

        try:
            query = """
            UPDATE event_table
            SET action = 'Closed', resolution_time = CURRENT_DATE + %s::time,
            remark = %s
            WHERE event_id = %s
            """
            cursor.execute(query, (close_time,action_remark,event_id))
            conn.commit()

            # Notify subscribers
            await NotificationManager.notify_subscribers(event_id, {
                "type": "closure",
                "eventId": event_id,
                "closeTime": close_time
            })

            return {"message": "Task closed successfully"}
        finally:
            Database.close_connection(conn, cursor)

    @staticmethod
    async def assign_task(data: dict):
        assignee = data.get("assignee")
        task_description = data.get("task_description")
        event_id = data.get("event_id")
        alert_type = data.get("alert_type")

        if not assignee or not task_description or not event_id:
            raise HTTPException(status_code=400, detail="Missing assignee, task description, or event ID")

        conn, cursor = Database.get_connection()
        if not conn or not cursor:
            raise HTTPException(status_code=500, detail="Database connection error")

        try:
            query = """
            UPDATE event_table
            SET assigned_to = %s, action = 'Assigned', remark = %s, event_type = %s, alert_type = %s, timestamp = %s
            WHERE event_id = %s
            """
            unique_timestamp = datetime.now()
            cursor.execute(query, (assignee, task_description, task_description, alert_type, unique_timestamp, event_id))
            conn.commit()

            # Notify subscribers
            await NotificationManager.notify_subscribers(assignee, {
                "type": "assignment",
                "assignee": assignee,
                "taskDescription": task_description
            })

            return {"message": "Task updated and notification sent if user is connected"}
        finally:
            Database.close_connection(conn, cursor)

class App:
    def __init__(self):
        self.app = FastAPI()
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=False,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        self.add_routes()

    def add_routes(self):
        @self.app.post("/close-task/")
        async def close_task(request: Request):
            data = await request.json()
            return await EventManager.close_task(data)

        @self.app.post("/assign-task/")
        async def assign_task(request: Request):
            data = await request.json()
            return await EventManager.assign_task(data)

        @self.app.get("/sse/notifications/")
        async def sse_notifications(username: str):
            queue = asyncio.Queue()
            NotificationManager.subscribers[username] = queue

            try:
                while True:
                    message = await queue.get()
                    yield f"data: {json.dumps(message)}\n\n"
            except asyncio.CancelledError:)
                NotificationManager.subscribers.pop(username, None)
            finally:
                # Ensure the subscriber is removed upon disconnection
                NotificationManager.subscribers.pop(username, None)
                print(f"Removed {username} from subscribers")

        @self.app.get("/hopper_level_graph")
        async def get_hopper_level_graph(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
            data = await self.fetch_hopper_data(machine_id)
            return data
    
        @self.app.get("/verticle_sealer_graph")
        async def get_verticle_sealer_graph(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
            data = await self.fetch_vertical_sealer_full_graph(machine_id)
            return data
        
        @self.app.get("/horizontal_sealer_graph")
        async def get_horizontal_graph(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
            data = await self.fetch_horizontal_sealer_graph(machine_id)
            return data
        
        @self.app.get("/sealar_front_temperature")
        async def get_sealer_front_temperature(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
            data = await self.fetch_recent_machine_setpoints(machine_id)
            return JSONResponse(content=data)
        
        @self.app.get("/machine_setpoints")
        async def get_machine_setpoints(machine_id: str = Query(..., pattern="^mc(17|18|19|20|21|22|25|26|27|28|29|30)$")):
            data = await self.fetch_machine_setpoints_data(machine_id)
            return JSONResponse(content=data)
        
        @self.app.get("/get_today_events")
        async def get_today_events():
            try:
                events = await self.fetch_today_events()
                return {"events": events}
            except HTTPException as e:
                raise e  # Pass along the HTTPException with status code
            except Exception as e:
                print(f"Unexpected error: {e}")
                print(traceback.format_exc())
                raise HTTPException(status_code=500, detail="Unexpected error fetching today's events")
    
        @self.app.get("/fetch-device-data")
        async def fetch_device_data(request: Request, recent: bool = True):
            """
            Fetch the most recent record for a given device. 
            If no recent data is found, fetch the last 100 records.
            """
            try:
                # Fetch the most recent or fallback to last records
                data = await self.fetch_device_graph_data(limit=12)

                if recent and not data:
                    data = await self.fetch_device_graph_data(limit=100)

                if not data:
                    raise HTTPException(status_code=404, detail="No data found for the given device.")

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

            except HTTPException as e:
                raise e  # Pass along the HTTPException with status code
            except Exception as e:
                print(f"Unexpected error: {e}")
                print(traceback.format_exc())
                raise HTTPException(status_code=500, detail="Unexpected error while fetching device data.")
    
        @self.app.get("/machinetabledata")
        async def get_machine_table_data():
            try:
                machine_data = await self.fetch_machine_data()
                response = {
                    "loop3": machine_data.get("loop3_overview", {}),
                    "loop4": machine_data.get("loop4_overview", {})
                }
                return JSONResponse(content=response)
            except HTTPException as e:
                raise e  # Pass along the HTTPException with status code
            except Exception as e:
                print(f"Error in /machinetabledata endpoint: {e}")
                print(traceback.format_exc())
                raise HTTPException(status_code=500, detail="Error fetching machine table data")

        @self.app.get("/loopoverview")
        async def get_loop_overview():
            try:
                loop_data = await self.fetch_loop_data()
                return JSONResponse(content=loop_data)
            except HTTPException as e:
                raise e  # Pass along the HTTPException with status code
            except Exception as e:
                print(f"Error in /loopoverview endpoint: {e}")
                print(traceback.format_exc())
                raise HTTPException(status_code=500, detail="Error fetching loop overview data")

    async def fetch_hopper_data(self, machine_id: str):
        conn, cursor = Database.get_connection()
        if not conn or not cursor:
            raise HTTPException(status_code=500, detail="Database connection error")

        try:
            # Construct the query to fetch time_bucket, hopper_1, and hopper_2 from the JSONB column
            query = f"""
                SELECT 
                    {machine_id}->'time_bucket' AS time_bucket,
                    {machine_id}->'hopper_1' AS hopper_1,
                    {machine_id}->'hopper_2' AS hopper_2
                FROM historical_graphs 
                WHERE {machine_id} IS NOT NULL;
            """

            # Execute the query
            cursor.execute(query)
            results = cursor.fetchall()

            # Check if results are empty
            if not results:
                raise HTTPException(status_code=404, detail="No hopper data available")

            # Format the data into a structured response
            hopper_data = {
                "time_bucket": [],
                "hopper_1": [],
                "hopper_2": []
            }

            for row in results:
                hopper_data["time_bucket"].extend(row[0])  # Assuming row[0] is a list
                hopper_data["hopper_1"].extend(row[1])     # Assuming row[1] is a list
                hopper_data["hopper_2"].extend(row[2])     # Assuming row[2] is a list

            return hopper_data

        except Exception as e:
            print(f"Error fetching hopper data: {e}")
            raise HTTPException(status_code=500, detail="Error fetching data from historical_graphs")
        finally:
            Database.close_connection(conn, cursor)

    async def fetch_vertical_sealer_full_graph(self, machine_id: str):
        conn, cursor = Database.get_connection()
        if not conn or not cursor:
            raise HTTPException(status_code=500, detail="Database connection error")

        try:
            # Construct the query dynamically for front and rear columns
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

            # Execute the query
            cursor.execute(query)
            results = cursor.fetchall()

            # Check if results are empty
            if not results:
                raise HTTPException(status_code=404, detail="No data available in historical_graph")

            # Initialize the structured data response
            vertical_data = {
                "time_bucket": [],
                **{f"front{i}": [] for i in range(1, 14)},
                **{f"rear{i}": [] for i in range(1, 14)}
            }

            # Populate the structured data
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

            return vertical_data

        except Exception as e:
            print(f"Error fetching full vertical data: {e}")
            raise HTTPException(status_code=500, detail="Error fetching data from historical_graph")
        finally:
            Database.close_connection(conn, cursor)

    async def fetch_horizontal_sealer_graph(self, machine_id: str):
        conn, cursor = Database.get_connection()
        if not conn or not cursor:
            raise HTTPException(status_code=500, detail="Database connection error")

        try:
            # Construct the query to fetch time_bucket, hor_sealer_front1, and hor_sealer_rear1
            query = f"""
                SELECT 
                    {machine_id}->'time_bucket' AS time_bucket,
                    {machine_id}->'hor_front1' AS hor_sealer_front1,
                    {machine_id}->'hor_rear1' AS hor_sealer_rear1
                FROM historical_graphs 
                WHERE {machine_id} IS NOT NULL;
            """

            # Execute the query
            cursor.execute(query)
            results = cursor.fetchall()

            # Check if results are empty
            if not results:
                raise HTTPException(status_code=404, detail="No data available in historical_graph")

            # Format the data into a structured response
            historical_data = {
                "time_bucket": [],
                "hor_sealer_front1": [],
                "hor_sealer_rear1": []
            }

            for row in results:
                historical_data["time_bucket"].extend(row[0])  # Assuming row[0] is a list
                historical_data["hor_sealer_front1"].extend(row[1])  # Assuming row[1] is a list
                historical_data["hor_sealer_rear1"].extend(row[2])  # Assuming row[2] is a list

            return historical_data

        except Exception as e:
            print(f"Error fetching historical data: {e}")
            raise HTTPException(status_code=500, detail="Error fetching data from historical_graph")
        finally:
            Database.close_connection(conn, cursor)

    async def fetch_recent_machine_setpoints(self, machine_id: str):
        conn, cursor = Database.get_connection()
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

            # Process each result and add to the last_5_values list
            for result in results:
                last_5_values.append(result[0])

            # Reverse to show data in ascending order (oldest first)
            last_5_values.reverse()

            if not last_5_values:
                raise HTTPException(status_code=404, detail="No recent data available in cls1_setpoints")

        except Exception as e:
            print(f"Error fetching data: {e}")
            raise HTTPException(status_code=500, detail="Error fetching data from cls1_setpoints")
        finally:
            Database.close_connection(conn, cursor)

        return last_5_values

    async def fetch_machine_setpoints_data(self, machine_id: str):
        conn, cursor = Database.get_connection()
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
            Database.close_connection(conn, cursor)

        return machine_data

    async def fetch_today_events(self):
        today = datetime.now().date()
        query = """
        SELECT * FROM event_table ORDER BY timestamp DESC
        """
        
        conn, cursor = Database.get_connection()
        if not conn or not cursor:
            raise HTTPException(status_code=500, detail="Database connection error")

        try:
            cursor.execute(query)
            result = cursor.fetchall()
            print("Result", result)
        except Exception as e:
            print(f"Error fetching data from event_table: {e}")
            print(traceback.format_exc())
            raise HTTPException(status_code=500, detail="Error fetching data from event_table")
        finally:
            Database.close_connection(conn, cursor)

        events = [
            {
                "timestamp": row[0],
                "event_id": row[1],
                "zone": row[2],
                "camera_id": row[3],
                "assigned_to": row[4],
                "action": row[5],
                "remark": row[6],
                "resolution_time": row[7],
                "event_type": row[9],
                "alert_type": row[10]
            }
            for row in result
        ]
        return events

    async def fetch_device_graph_data(limit=12):
        """Fetch telemetry data for a device from the database."""
        conn, cursor = Database.get_device_db()
        if not conn or not cursor:
            raise HTTPException(status_code=500, detail="Database connection error")

        try:
            query = """
            SELECT device_id, timestamp, x_rms_vel, y_rms_vel, z_rms_vel, spl_db, temp_c, label
            FROM deviceid_timeseries
            ORDER BY timestamp DESC
            LIMIT %s;
            """
            cursor.execute(query, (limit,))
            result = cursor.fetchall()

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
            ] if result else []

            return data

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
        finally:
            Database.close_connection(conn, cursor)

    async def fetch_machine_data(self):
        query = """
        SELECT 
            last_update,
            machines
        FROM {table_name}
        ORDER BY last_update DESC
        LIMIT 1
        """
        
        data = {}
        table_names = ["loop3_overview", "loop4_overview"]

        for table_name in table_names:
            conn, cursor = Database.get_db()
            if not conn or not cursor:
                raise HTTPException(status_code=500, detail="Database connection error")
            
            try:
                cursor.execute(query.format(table_name=table_name))
                result = cursor.fetchone()
                
                if result:
                    last_update, machines_json = result
                    machines_data = self.parse_machines_data(machines_json)
                    
                    # Structure the response data
                    data[table_name] = {
                        "last_update": last_update,
                        "machines": machines_data
                    }
                    
            except Exception as e:
                print(f"Error fetching data from {table_name}: {e}")
                print(traceback.format_exc())
                raise HTTPException(status_code=500, detail=f"Error fetching data from {table_name}")
            finally:
                Database.close_db(conn, cursor)
        
        return data
    
    async def fetch_loop_data():
        loop_overviews = {
            "loop3": fetch_loop_data_helper("loop3_overview"),
            "loop4": fetch_loop_data_helper("loop4_overview")
        }
        return loop_overviews

def parse_machines_data(machines_json):
        """Parse the machines JSON string or return the dictionary."""
        if isinstance(machines_json, str):
            return json.loads(machines_json)  # Parse the JSON string
        elif isinstance(machines_json, dict):
            return machines_json  # Already a dictionary
        else:
            raise TypeError("machines_json is neither a string nor a dictionary")

def fetch_loop_data_helper(table_name):
    query = f"""
    SELECT last_update, primary_tank_level, secondary_tank_level, bulk_settling_time,
           ega, cld_production, taping_machine, case_erector, plantair, planttemperature
    FROM {table_name}
    ORDER BY last_update DESC
    LIMIT 1
    """
    
    conn, cursor = Database.get_connection()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            # Adjust the unpacking based on the columns present in the table
            response_data = {
                "last_update": result[0],
                "primary_tank_level": result[1],
                "secondary_tank_level": result[2],
                "bulk_settling_time": result[3],
                "ega": result[4],
                "cld_production": result[5],
                "taping_machine": result[6],
                "case_erector": result[7],
            }
            if table_name == "loop3_overview":
                response_data.update({
                    "plantair": result[8],
                    "planttemperature": result[9]
                })
            return response_data
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error fetching data from {table_name}")
    finally:
        Database.close_connection(conn, cursor)

    return {}
app_instance = App()
app = app_instance.app

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)

