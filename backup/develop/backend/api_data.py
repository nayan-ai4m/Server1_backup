import psycopg2
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

app = FastAPI()

class DeviceManager:
    def __init__(self):
        pass

    def get_db(self):
        """
        Establish a new database connection using hardcoded credentials.
        """
        try:
            # Replace these with your actual PostgreSQL connection details
            conn = psycopg2.connect(
                dbname="sensor_db",
                user="postgres",
                password="ai4m2024",
                host="192.168.4.11",
                port="5432"
            )
            cursor = conn.cursor()
            return conn, cursor
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

    def close_db(self, conn, cursor):
        """Close the database connection and cursor."""
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    def fetch_device_data(self, limit=1):
        """Fetch telemetry data for a device from the database."""
        conn, cursor = self.get_db()  # Establish the database connection
        try:
            query = """
            SELECT device_id, timestamp, x_rms_vel, y_rms_vel, z_rms_vel, spl_db, temp_c, label
            FROM deviceid_timeseries
            ORDER BY timestamp DESC
            LIMIT %s;
            """
            cursor.execute(query, (limit,))
            result = cursor.fetchall()

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
            self.close_db(conn, cursor)  # Ensure the database connection is closed

device_manager = DeviceManager()

@app.get("/fetch-device-data")
async def fetch_device_data(request: Request, recent: bool = True):
    """
    Fetch the most recent record for a given device. 
    If no recent data is found, fetch the last 100 records.
    """
    try:
        # Fetch the most recent or last 100 records
        if recent:
            data = device_manager.fetch_device_data(limit=12)
            if not data:
                data = device_manager.fetch_device_data(limit=100)
        else:
            data = device_manager.fetch_device_data(limit=100)

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

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
