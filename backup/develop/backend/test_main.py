import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime
import time
from fastapi.middleware.cors import CORSMiddleware
from status_data import color_codes, predictions_status_data, live_status_data, vertical_sealer_data

DATABASE = {
    'dbname': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': '100.93.62.9',
    'port': '5432'
}
app = FastAPI()
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],  
    allow_headers=["*"], 
)

class MachineData:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.predictions_status_data = predictions_status_data

    def get_db_connection(self):
        """Establish and return the database connection and cursor."""
        self.conn = psycopg2.connect(**DATABASE)
        self.cursor = self.conn.cursor()

    def close_db_connection(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    # def get_latest_machine_data(self, table_num):
    #     """Retrieve the latest data for a given machine table."""
    #     query = f"""
    #     SELECT 
    #         timestamp,
    #         mc{table_num}_ver_sealer_front_1_temp, mc{table_num}_ver_sealer_front_2_temp,
    #         mc{table_num}_ver_sealer_front_3_temp, mc{table_num}_ver_sealer_front_4_temp,
    #         mc{table_num}_ver_sealer_front_5_temp, mc{table_num}_ver_sealer_front_6_temp,
    #         mc{table_num}_ver_sealer_front_7_temp, mc{table_num}_ver_sealer_front_8_temp,
    #         mc{table_num}_ver_sealer_front_9_temp, mc{table_num}_ver_sealer_front_10_temp,
    #         mc{table_num}_ver_sealer_front_11_temp, mc{table_num}_ver_sealer_front_12_temp,
    #         mc{table_num}_ver_sealer_front_13_temp, mc{table_num}_ver_sealer_rear_1_temp,
    #         mc{table_num}_ver_sealer_rear_2_temp, mc{table_num}_ver_sealer_rear_3_temp,
    #         mc{table_num}_ver_sealer_rear_4_temp, mc{table_num}_ver_sealer_rear_5_temp,
    #         mc{table_num}_ver_sealer_rear_6_temp, mc{table_num}_ver_sealer_rear_7_temp,
    #         mc{table_num}_ver_sealer_rear_8_temp, mc{table_num}_ver_sealer_rear_9_temp,
    #         mc{table_num}_ver_sealer_rear_10_temp, mc{table_num}_ver_sealer_rear_11_temp,
    #         mc{table_num}_ver_sealer_rear_12_temp, mc{table_num}_ver_sealer_rear_13_temp,
    #         mc{table_num}_hor_sealer_front_1_temp, mc{table_num}_hor_sealer_rear_1_temp,
    #         mc{table_num}_hopper_1_level, mc{table_num}_hopper_2_level, mc{table_num}_piston_stroke_length,
    #         mc{table_num}_status
    #     FROM mc{table_num}_data
    #     ORDER BY timestamp DESC LIMIT 1;
    #     """
    #     try:
    #         self.cursor.execute(query)
    #         data = self.cursor.fetchone()
    #         print(data)
    #         if data:
    #             # Convert datetime to string
    #             data = list(data)
    #             data[0] = data[0].isoformat() if isinstance(data[0], datetime) else data[0]
    #             return data
    #         return None
    #     except Exception as e:
    #         self.conn.rollback()  # Rollback the transaction
    #         print(f"Error fetching data: {e}")
    #         return None
    
    def get_latest_prediction(self, table_num):
        """Retrieve the latest prediction data for a given machine table and return as a dictionary."""
        query = f"""
        SELECT mc{table_num}_hopper_1_level, mc{table_num}_hopper_2_level, mc{table_num}_pulling_servo_current,
            mc{table_num}_hor_sealer_current, mc{table_num}_hor_sealer_position, mc{table_num}_cam_position, 
            mc{table_num}_status, mc{table_num}_status_code, mc{table_num}_hor_pressure 
        FROM mc{table_num}_data 
        ORDER BY timestamp DESC LIMIT 1;
        """
        
        try:
            self.cursor.execute(query)
            prediction = self.cursor.fetchone()
            
            if prediction:
                # Define the keys for the dictionary
                keys = [
                    f'mc{table_num}_hopper_1_level',
                    f'mc{table_num}_hopper_2_level',
                    f'mc{table_num}_pulling_servo_current',
                    f'mc{table_num}_hor_sealer_current',
                    f'mc{table_num}_hor_sealer_position',
                    f'mc{table_num}_cam_position',
                    f'mc{table_num}_status',
                    f'mc{table_num}_status_code',
                    f'mc{table_num}_hor_pressure'
                ]
                
                # Create a dictionary by zipping the keys with the fetched prediction values
                prediction_dict = {key: value for key, value in zip(keys, prediction)}
                
                return prediction_dict
            
            return None  # Return None if no data is fetched

        except Exception as e:
            self.conn.rollback()  # Rollback in case of error
            print(f"Error fetching prediction data: {e}")
            return None

    
    def get_latest_machine_data(self, table_num):
        """Retrieve the latest data for a given machine table."""
        query = f"""
        SELECT 
            timestamp,
            mc{table_num}_ver_sealer_front_1_temp, mc{table_num}_ver_sealer_front_2_temp,
            mc{table_num}_ver_sealer_front_3_temp, mc{table_num}_ver_sealer_front_4_temp,
            mc{table_num}_ver_sealer_front_5_temp, mc{table_num}_ver_sealer_front_6_temp,
            mc{table_num}_ver_sealer_front_7_temp, mc{table_num}_ver_sealer_front_8_temp,
            mc{table_num}_ver_sealer_front_9_temp, mc{table_num}_ver_sealer_front_10_temp,
            mc{table_num}_ver_sealer_front_11_temp, mc{table_num}_ver_sealer_front_12_temp,
            mc{table_num}_ver_sealer_front_13_temp, mc{table_num}_ver_sealer_rear_1_temp,
            mc{table_num}_ver_sealer_rear_2_temp, mc{table_num}_ver_sealer_rear_3_temp,
            mc{table_num}_ver_sealer_rear_4_temp, mc{table_num}_ver_sealer_rear_5_temp,
            mc{table_num}_ver_sealer_rear_6_temp, mc{table_num}_ver_sealer_rear_7_temp,
            mc{table_num}_ver_sealer_rear_8_temp, mc{table_num}_ver_sealer_rear_9_temp,
            mc{table_num}_ver_sealer_rear_10_temp, mc{table_num}_ver_sealer_rear_11_temp,
            mc{table_num}_ver_sealer_rear_12_temp, mc{table_num}_ver_sealer_rear_13_temp,
            mc{table_num}_hor_sealer_front_1_temp, mc{table_num}_hor_sealer_rear_1_temp,
            mc{table_num}_hopper_1_level, mc{table_num}_hopper_2_level, mc{table_num}_piston_stroke_length,
            mc{table_num}_status
        FROM mc{table_num}_data
        ORDER BY timestamp DESC LIMIT 1;
        """
        try:
            self.cursor.execute(query)
            data = self.cursor.fetchone()
            if data:
                # Dynamically create the keys based on the table number and column names
                keys = [
                    'timestamp',
                    f'mc{table_num}_ver_sealer_front_1_temp', f'mc{table_num}_ver_sealer_front_2_temp',
                    f'mc{table_num}_ver_sealer_front_3_temp', f'mc{table_num}_ver_sealer_front_4_temp',
                    f'mc{table_num}_ver_sealer_front_5_temp', f'mc{table_num}_ver_sealer_front_6_temp',
                    f'mc{table_num}_ver_sealer_front_7_temp', f'mc{table_num}_ver_sealer_front_8_temp',
                    f'mc{table_num}_ver_sealer_front_9_temp', f'mc{table_num}_ver_sealer_front_10_temp',
                    f'mc{table_num}_ver_sealer_front_11_temp', f'mc{table_num}_ver_sealer_front_12_temp',
                    f'mc{table_num}_ver_sealer_front_13_temp', f'mc{table_num}_ver_sealer_rear_1_temp',
                    f'mc{table_num}_ver_sealer_rear_2_temp', f'mc{table_num}_ver_sealer_rear_3_temp',
                    f'mc{table_num}_ver_sealer_rear_4_temp', f'mc{table_num}_ver_sealer_rear_5_temp',
                    f'mc{table_num}_ver_sealer_rear_6_temp', f'mc{table_num}_ver_sealer_rear_7_temp',
                    f'mc{table_num}_ver_sealer_rear_8_temp', f'mc{table_num}_ver_sealer_rear_9_temp',
                    f'mc{table_num}_ver_sealer_rear_10_temp', f'mc{table_num}_ver_sealer_rear_11_temp',
                    f'mc{table_num}_ver_sealer_rear_12_temp', f'mc{table_num}_ver_sealer_rear_13_temp',
                    f'mc{table_num}_hor_sealer_front_1_temp', f'mc{table_num}_hor_sealer_rear_1_temp',
                    f'mc{table_num}_hopper_1_level', f'mc{table_num}_hopper_2_level', f'mc{table_num}_piston_stroke_length',
                    f'mc{table_num}_status'
                ]
                
                # Create a dictionary using the keys and the data from the database
                data_dict = {key: value for key, value in zip(keys, data)}
                
                # Convert the timestamp to ISO format
                if isinstance(data_dict['timestamp'], datetime):
                    data_dict['timestamp'] = data_dict['timestamp'].isoformat()
                
                return data_dict
            return None
        except Exception as e:
            self.conn.rollback()  # Rollback the transaction
            print(f"Error fetching data: {e}")
            return None

    def fetch_vertical_sealer_data(self, table_num):
        """Fetch vertical sealer data."""
        return self.get_latest_machine_data(table_num)

    def get_pulling_roller_data(self, table_num):
        """Retrieve pulling roller data for a given machine table."""
        query = f"""
        SELECT * FROM mc{table_num}_pulling_roller_data
        ORDER BY timestamp DESC LIMIT 1;
        """
        try:
            self.cursor.execute(query)
            data = self.cursor.fetchone()
            if data:
                data = list(data)
                data[0] = data[0].isoformat() if isinstance(data[0], datetime) else data[0]
                return data
            return None
        except Exception as e:
            self.conn.rollback()  # Rollback the transaction
            print(f"Error fetching pulling roller data: {e}")
            return None
        def fetch_prediction_data(self, table_num):
            """Fetch vertical sealer data."""
            return self.get_latest_prediction(table_num)
        def update_predictions_status(self):
            while True:
                time.sleep(0.5)
                conn, cursor = self.get_db()
                try:
                    latest_prediction = self.get_latest_prediction(table_num)
                    print(latest_prediction)
                    if latest_prediction:
                        if (
                            latest_prediction[3] > 8
                            and latest_prediction[4] < 45
                            and latest_prediction[5] < 113
                        ):
                            self.predictions_status_data.update(
                                {
                                    "sealant_leakage": {
                                        "status": "Possibility",
                                        "color": self.color_codes["Possibility"],
                                    }
                                }
                            )
                        elif latest_prediction[6] == 0 or latest_prediction[7] != 1000:
                            self.predictions_status_data.update(
                                {
                                    "sealant_leakage": {
                                        "status": "Occured",
                                        "color": self.color_codes["Occured"],
                                    }
                                }
                            )

                        elif (latest_prediction[5] > 125 and latest_prediction[5] < 195) and (latest_prediction[8] < 4.2):
                            self.predictions_status_data.update(
                                {
                                    "sealant_leakage": {
                                        "status": "Possibility Horizontal Sealing Pressure Drop",
                                        "color": self.color_codes["Possibility"],
                                    }
                                }
                            )

                        
                        else:
                            self.predictions_status_data.update(
                                {
                                    "sealant_leakage": {
                                        "status": "None",
                                        "color": self.color_codes["None"],
                                    }
                                }
                        )
                        
                        self.predictions_status_data.update(
                            {
                                #"sealant_leakage": {"status": "None", "color": "#00FF00"},
                                "laminate_pulling": {"status": "None", "color": "#00FF00"},
                                "laminate_jamming": {"status": "None", "color": "#00FF00"},
                                "hopper_level": {
                                    "value": latest_prediction[0],
                                    "color": "#FF0000",
                                },
                                "laminate_cof_variation": {
                                    "value": latest_prediction[1],
                                    "color": "#FF0000",
                                },
                                "pulling_roller_current_variation": {
                                    "value": latest_prediction[2],
                                    "color": "#FF0000",
                                },
                            }
                        )
                finally:
                    self.close_db(conn, cursor)

# # FastAPI app
# app = FastAPI()

@app.on_event("startup")
async def startup_event():
    """Event to run on startup."""
    global machine_data
    machine_data = MachineData()
    machine_data.get_db_connection()
    # machine_data.update_predictions_status()
    # machine_data.get_latest_prediction()
    
@app.on_event("shutdown")
async def shutdown_event():
    """Event to run on shutdown."""
    machine_data.close_db_connection()


@app.get("/machines/latest/{table_num}")
async def get_latest_machine_data(table_num: int):
    """Endpoint to get the latest machine data."""
    latest_data = machine_data.get_latest_prediction(table_num)
    if latest_data:
        return JSONResponse(content=latest_data)
    raise HTTPException(status_code=404, detail="No data available")

# @app.get("/machines/live_data/{table_num}")
# async def get_live_status(table_num: int):
#     """Endpoint to get the live machine data."""
#     latest_data = machine_data.get_machine_live_status(table_num)
#     if latest_data:
#         return JSONResponse(content=latest_data)
#     raise HTTPException(status_code=404, detail="No data available")

@app.get("/machines/ver-sealer/{table_num}")
async def get_vertical_sealer_data(table_num: int = 17):
    """Endpoint to get vertical sealer data."""
    vertical_sealer_data = machine_data.fetch_vertical_sealer_data(table_num)
    if vertical_sealer_data:
        # print(vertical_sealer_data)
        return JSONResponse(content=vertical_sealer_data)
    raise HTTPException(status_code=404, detail="No data available")

@app.get("/machines/hopper-level/{table_num}")
async def get_hopper_level_data(table_num: int):
    """Endpoint to get hopper level data."""
    latest_hopper_data = machine_data.get_hopper_level_data(table_num)
    if latest_hopper_data:
        return JSONResponse(content=latest_hopper_data)
    raise HTTPException(status_code=404, detail="No data available")

@app.get("/machines/pulling-roller/{table_num}")
async def get_pulling_roller_data(table_num: int):
    """Endpoint to get pulling roller data."""
    pulling_roller_data = machine_data.get_pulling_roller_data(table_num)
    if pulling_roller_data:
        return JSONResponse(content=pulling_roller_data)
    raise HTTPException(status_code=404, detail="No data available")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
