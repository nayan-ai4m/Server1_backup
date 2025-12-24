import json
import time  # for simulating continuous data fetching
import psycopg2
from fastapi import FastAPI, HTTPException,Query, Request, WebSocketDisconnect
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta
import traceback
import re
from typing import List
import time 
from fastapi import WebSocket

# Load database configuration from a JSON file
with open('db_config.json', 'r') as config_file:
    config = json.load(config_file)

DATABASE = config['database']  # Database connection details
 
DATABASE2 = config['database1']
app = FastAPI()

# CORS middleware to allow requests from any origin
origins = ["*"]  # Change this to trusted origins in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

class StatusManager:
    def __init__(self):
        self.color_codes = {}  # Initialize as needed

    def get_db(self):
        """ Establish a new database connection and return it. """
        conn = psycopg2.connect(**DATABASE)
        cursor = conn.cursor()
        return conn, cursor
    
    def get_device_db(self):
        """ Establish a new database connection and return it. """
        conn2 = psycopg2.connect(**DATABASE2)
        cursor2 = conn2.cursor()
        return conn2, cursor2

    def close_db(self, conn, cursor):
        """ Close the database connection and cursor. """
        cursor.close()
        conn.close()

    def close_device_db(self, conn2, cursor2):
        """ Close the database connection and cursor. """
        cursor2.close()
        conn2.close()

    def get_machine_table_range(self):
        """Return the range of machine table names (mc17 to mc22)."""
        return range(17, 23) 

    def determine_shift(self):
        """Determine the current shift based on the current time."""
        current_time = datetime.now().time()
        if current_time >= datetime.strptime("07:00", "%H:%M").time() and current_time < datetime.strptime("15:00", "%H:%M").time():
            return 'A', 'cld_count_a'
        elif current_time >= datetime.strptime("15:00", "%H:%M").time() and current_time < datetime.strptime("23:00", "%H:%M").time():
            return 'B', 'cld_count_b'
        else:
            return 'C', 'cld_count_c'

    def determine_shift(self):
        """Determine the current shift based on the current time."""
        current_time = datetime.now().time()
        if current_time >= datetime.strptime("07:00", "%H:%M").time() and current_time < datetime.strptime("15:00", "%H:%M").time():
            return 'A', 'cld_count_a'
        elif current_time >= datetime.strptime("15:00", "%H:%M").time() and current_time < datetime.strptime("23:00", "%H:%M").time():
            return 'B', 'cld_count_b'
        else:
            return 'C', 'cld_count_c'
    # def fetch_latest_cld_production(self, machine_ids: list, cld_count_column: str):
    #     """
    #     Fetch the sum of the latest cld_count for each machine in the loop based on machine IDs and shift column.
    #     Returns the total cld_production for the loop.
    #     """
    #     conn, cursor = self.get_db()  # Get DB connection
    #     try:
    #         total_cld_production = 0
    #         current_time = datetime.now().time()
    #         # Loop through the machine tables in the given list
    #         for machine_id, column_name in machine_ids:
    #             query = f"""
    #             SELECT SUM({column_name}) 
    #             FROM (
    #                 SELECT {column_name}
    #                 FROM mc{machine_id}
    #                 ORDER BY timestamp DESC
    #                 LIMIT 1
    #             ) AS latest_records;
    #             """
    #             cursor.execute(query)
    #             result = cursor.fetchone()
                
    #             # Add the sum for this machine to the total production
    #             total_cld_production += result[0] if result[0] is not None else 0
            
    #         return total_cld_production

    #     finally:
    #         self.close_db(conn, cursor)
    def get_shift_and_column(self,current_time):
        """
        Determine the current shift based on the provided time and return the corresponding shift ID and CLD count column.
        
        :param current_time: The current time (should be a datetime.time() object).
        :return: A tuple containing the shift ID and the corresponding CLD count column.
        """
        if current_time >= datetime.strptime("07:00", "%H:%M").time() and current_time < datetime.strptime("15:00", "%H:%M").time():
            return 'A' 
        elif current_time >= datetime.strptime("15:00", "%H:%M").time() and current_time < datetime.strptime("23:00", "%H:%M").time():
            return 'B' 
        else:
            return 'C' 
   
    def fetch_latest_cld_production(self, machine_ids: list):
        """
        Fetch the sum of the latest cld_count for each machine based on the most recent entry for each machine ID.
        Returns the total cld_production for the loop.
        
        :param machine_ids: List of tuples where each tuple contains machine_id and cld_count_column (e.g., [(1, 'cld_count_a'), (2, 'cld_count_b')]).
        """
        current_time = datetime.now().time()  # Get current time
        shift = self.get_shift_and_column(current_time)  # Determine current shift
        
        conn, cursor = self.get_db()  # Get DB connection
        try:
            total_cld_production = 0
            
            # Loop through the machine tuples in the given list
            for machine_id, cld_count_column in machine_ids:
                print("Processing machine_id:", machine_id, "with column:", cld_count_column)
                
                # SQL query to get the latest entry for each machine (based on timestamp)
                query = f"""
                SELECT {cld_count_column}
                FROM mc{machine_id}
                ORDER BY timestamp DESC
                LIMIT 1;
                """
                
                cursor.execute(query)
                result = cursor.fetchone()

                # Add the latest cld_count value for this machine to the total production
                if result is not None:
                    if(result[0] is not None):
                        total_cld_production += result[0]
                    else:
                        total_cld_production += 0  
                else:
                    total_cld_production += 0
            
            return total_cld_production

        finally:
            self.close_db(conn, cursor)
    def fetch_cld_production(self):
        """Fetch and return cld production for loop3 and loop4."""
        
        # Machine IDs and column names for loop3 (mc17 to mc22 have 'cld_count' column)
        loop3_machine_ids = [(machine_id, 'cld_count') for machine_id in range(17, 23)]
        loop3_cldproduction = self.fetch_latest_cld_production(loop3_machine_ids)
        print("loop",loop3_cldproduction)

        # Determine the current shift and get the corresponding column name
        shift, shift_column = self.determine_shift()

        # Machine IDs and columns for loop4
        loop4_machine_ids = [
            (25, shift_column),  # mc25 uses shift-specific column
            (26, shift_column),  # mc26 uses shift-specific column
            (27, shift_column),  # mc27 uses shift-specific column
            (30, shift_column),  # mc30 uses shift-specific column
            (28, 'cld'),         # mc28 uses 'cld'
            (29, 'cld')          # mc29 uses 'cld'
        ]
        loop4_cldproduction = self.fetch_latest_cld_production(loop4_machine_ids)
        print(loop4_cldproduction)

        # Return JSON-like output
        return {
            "loop3_cldproduction": loop3_cldproduction,
            "loop4_cldproduction": loop4_cldproduction
        }

    
    def fetch_machine_hopper_level_data(self, machine_num=None):
        """
        Fetch the latest hopper levels for specific machines. Handle different column names for different machines.
        Machines mc17 to mc22 have hopper_1_level and hopper_2_level.
        Machine mc27 has hopper_level_right and hopper_level_left.
        Machines mc28, mc29, mc30 have no hopper levels and should return blank.
        
        :param machine_num: The specific machine number (e.g., 17 for 'mc17') to fetch data for. If None, fetch data for all machines.
        """
        table_range = range(17, 31)  # From mc17 to mc30
        machine_data = {}
        result = None
        # If a specific machine_num is provided, narrow the search to that machine
        if machine_num:
            table_range = [machine_num]  # Use the numeric machine number directly

        for i in table_range:
            table_name = f"mc{i}"  # Generate table name dynamically (e.g., mc17, mc18)

            try:
                conn, cursor = self.get_db()

                # Handle column names based on the machine number
                if 17 <= i <= 22:
                    query = f"""
                    SELECT hopper_1_level, hopper_2_level 
                    FROM {table_name}
                    ORDER BY timestamp DESC 
                    LIMIT 1;
                    """
                    cursor.execute(query)
                    result = cursor.fetchone()

                    if result:
                        machine_data[table_name] = {
                            "hopper_1_level": result[0],
                            "hopper_2_level": result[1]
                        }

                elif i == 27:
                    query = f"""
                    SELECT hopper_level_right, hopper_level_left 
                    FROM {table_name}
                    ORDER BY timestamp DESC 
                    LIMIT 1;
                    """
                    cursor.execute(query)
                    result = cursor.fetchone()

                    if result:
                        machine_data[table_name] = {
                            "hopper_level_right": result[0],
                            "hopper_level_left": result[1]
                        }

                elif i in [28, 29, 30]:
                    # Machines 28, 29, 30 have no hopper levels, return blank data
                    machine_data[table_name] = {
                        "hopper_1_level": "",
                        "hopper_2_level": ""
                    }

                # If no result found, fetch the last 100 records (only for mc17 to mc22 and mc27)
                if not result and i != 27 and i in range(17, 23):
                    query_last_100 = f"""
                    SELECT hopper_1_level, hopper_2_level
                    FROM {table_name}
                    ORDER BY timestamp DESC 
                    LIMIT 100;
                    """
                    cursor.execute(query_last_100)
                    result_last_100 = cursor.fetchall()

                    if result_last_100:
                        machine_data[table_name] = {
                            "last_100_records": [
                                {"hopper_1_level": row[0], "hopper_2_level": row[1]} 
                                for row in result_last_100
                            ]
                        }

                self.close_db(conn, cursor)

            except Exception as e:
                print(f"Error fetching data from {table_name}: {e}")
                self.close_db(conn, cursor)

        return machine_data

    
    # def fetch_vertical_sealer_data(self, machine_id=None):
    #     """
    #     Fetch vertical sealer temperature data from a specific machine (e.g., 'mc17') or all machines ('mc17' to 'mc30').
        
    #     The column names differ for machines mc17 to mc22 and mc25 to mc30, so this function handles that accordingly.
        
    #     :param machine_id: The specific machine (e.g., 'mc17') to fetch data for. If None, fetch data for all machines.
    #     """
    #     # Define machine ranges and column configurations
    #     mc17_to_mc22_columns = {
    #         "front": [f"ver_sealer_front_{i}_temp" for i in range(1, 14)],
    #         "rear": [f"ver_sealer_rear_{i}_temp" for i in range(1, 14)],
    #     }
        
    #     mc25_to_mc30_columns = {
    #         "front": ["verticalsealerfront_1"],
    #         "rear": ["verticalsealerrear_1"],
    #     }
        
    #     # Handle machine range dynamically
    #     table_range = range(17, 23) if machine_id is None else []

    #     vertical_data = {}

    #     if machine_id:
    #         try:
    #             table_number = machine_id  # Extract number from 'mcXX' format
    #             table_range = [table_number]  # Fetch data for the specific machine
    #         except ValueError:
    #             return {"error": "Invalid machine_id format. Use 'mcXX' format (e.g., 'mc17')."}

    #     for i in table_range:
    #         table_name = f"mc{i}"  # Generate table name dynamically (e.g., mc17, mc18, etc.)

    #         try:
    #             conn, cursor = self.get_db()

    #             # Use different queries for machines mc17-mc22 and mc25-mc30
    #             if 17 <= i <= 22:
    #                 columns = mc17_to_mc22_columns
    #             elif 25 <= i <= 30:
    #                 columns = mc25_to_mc30_columns
    #             else:
    #                 continue  # Skip invalid machine IDs

    #             # Construct SQL query for the specific machine
    #             query = f"""
    #             SELECT {', '.join(columns['front'] + columns['rear'])}
    #             FROM {table_name}
    #             ORDER BY timestamp DESC
    #             LIMIT 1;
    #             """
    #             cursor.execute(query)
    #             result = cursor.fetchone()

    #             if result:
    #                 vertical_data[table_name] = {
    #                     **{columns['front'][j - 1]: result[j - 1] for j in range(1, len(columns['front']) + 1)},
    #                     **{columns['rear'][j - 1]: result[len(columns['front']) + j - 1] for j in range(1, len(columns['rear']) + 1)}
    #                 }
    #             else:
    #                 # If no data is found, fetch last 100 records
    #                 query_last_100 = f"""
    #                 SELECT {', '.join(columns['front'] + columns['rear'])}
    #                 FROM {table_name}
    #                 WHERE timestamp = (SELECT MAX(timestamp) FROM {table_name})
    #                 ORDER BY timestamp DESC
    #                 LIMIT 100;
    #                 """
    #                 cursor.execute(query_last_100)
    #                 result_last_100 = cursor.fetchall()

    #                 if result_last_100:
    #                     vertical_data[table_name] = {
    #                         "last_100_records": [
    #                             {
    #                                 **{columns['front'][j - 1]: row[j - 1] for j in range(1, len(columns['front']) + 1)},
    #                                 **{columns['rear'][j - 1]: row[len(columns['front']) + j - 1] for j in range(1, len(columns['rear']) + 1)}
    #                             }
    #                             for row in result_last_100
    #                         ]
    #                     }
    #                 else:
    #                     vertical_data[table_name] = {"error": "No data found in the last 100 records"}

    #             self.close_db(conn, cursor)

    #         except Exception as e:
    #             print(f"Error fetching vertical sealer data from {table_name}: {e}")
    #             self.close_db(conn, cursor)

    #     return vertical_data

    ################
    
    def fetch_machine_hopper_level_graph_data(self, machine_num=None, last_timestamp=None):
        """
        Fetch the latest hopper levels for specific machines with handling of different column names per machine.
        
        Machines mc17 to mc22 have hopper_1_level and hopper_2_level.
        Machine mc27 has hopper_level_right and hopper_level_left.
        Machines mc28, mc29, mc30 have no hopper levels and should return blank.

        :param machine_num: The specific machine number (e.g., 17 for 'mc17') to fetch data for. If None, fetch data for all machines.
        :param last_timestamp: The last timestamp to fetch data from. If None, fetch the most recent data.
        :return: A dictionary with machine names as keys and hopper level data as values.
        """
        table_range = range(17, 31)  # Machines mc17 to mc30
        machine_data = {}

        # If a specific machine_num is provided, limit the range to that machine only
        if machine_num:
            table_range = [machine_num]

        for i in table_range:
            table_name = f"mc{i}"  # Generate the table name dynamically (e.g., mc17, mc18)
            result = None

            try:
                conn, cursor = self.get_db()

                # Handle machines with hopper_1_level and hopper_2_level (mc17 to mc22)
                if 17 <= i <= 22:
                    query = f"""
                        SELECT hopper_1_level, hopper_2_level, timestamp
                        FROM {table_name}
                        WHERE timestamp > %s ORDER BY timestamp DESC LIMIT 1;
                    """ if last_timestamp else f"""
                        SELECT hopper_1_level, hopper_2_level, timestamp
                        FROM {table_name}
                        ORDER BY timestamp DESC LIMIT 100;
                    """
                    cursor.execute(query, (last_timestamp,) if last_timestamp else ())
                    result = cursor.fetchall()

                    if result:
                        machine_data[table_name] = [
                            {
                                "hopper_1_level": row[0],
                                "hopper_2_level": row[1],
                                "timestamp": row[2].isoformat()  # Real timestamp here
                            } for row in result
                        ]

                # Handle machine mc27 with hopper_level_right and hopper_level_left
                elif i == 27:
                    query = f"""
                        SELECT hopper_level_right, hopper_level_left, timestamp
                        FROM {table_name}
                        WHERE timestamp > %s ORDER BY timestamp DESC LIMIT 1;
                    """ if last_timestamp else f"""
                        SELECT hopper_level_right, hopper_level_left, timestamp
                        FROM {table_name}
                        ORDER BY timestamp DESC LIMIT 100;
                    """
                    cursor.execute(query, (last_timestamp,) if last_timestamp else ())
                    result = cursor.fetchall()

                    if result:
                        machine_data[table_name] = [
                            {
                                "hopper_level_right": row[0],
                                "hopper_level_left": row[1],
                                "timestamp": row[2].isoformat()  # Real timestamp here
                            } for row in result
                        ]

                # Handle machines mc28, mc29, mc30 with no hopper data (return empty values)
                elif i in [28, 29, 30]:
                    machine_data[table_name] = {
                        "hopper_1_level": None,
                        "hopper_2_level": None,
                        "timestamp": None  # Indicate no timestamp available
                    }

                self.close_db(conn, cursor)

            except Exception as e:
                print(f"Error fetching data from {table_name}: {e}")
                self.close_db(conn, cursor)

        return machine_data

    ################
    def fetch_horizontal_sealer_data(self,machine_id=None):
        try:
            """
            Fetch horizontal sealer temperature data from a specific machine (e.g., 'mc17') or all machines ('mc17' to 'mc30').
            
            The column names differ for machines mc17 to mc22 and mc25 to mc30, so this function handles that accordingly.
            
            :param machine_id: The specific machine (e.g., 'mc17') to fetch data for. If None, fetch data for all machines.
            """
            # Define machine ranges and column configurations
            mc17_to_mc22_columns = {
                #"front": [f"hor_sealer_front_{i}_temp" for i in range(1, 14)],
                "front": ["hor_sealer_front_1_temp"], 
                "rear": [f"hor_sealer_rear_{i}_temp" for i in range(1, 14)],
                "pressure": ["hor_pressure"] 
            }
            
            

            mc25_to_mc30_columns = {
                "front": ["hor_sealer_front"],
                "rear": ["hor_sealer_rear"],
                "pressure": ["hor_pressure"] 
            }
            print("machine idL",machine_id)
            if(machine_id == 25 or machine_id == 26):
                mc25_to_mc30_columns["pressure"] = ["hor_pressure"]
            
            if machine_id is not None and machine_id >= 28:
                mc25_to_mc30_columns["pressure"]= ["horizontal_pressure"]
            
            # Handle machine range dynamically
            table_range = range(17, 31) if machine_id is None else []

            vertical_data = {}

            if machine_id:
                try:
                    table_number = machine_id  # Extract number from 'mcXX' format
                    table_range = [table_number]  # Fetch data for the specific machine
                except ValueError:
                    return {"error": "Invalid machine_id format. Use 'mcXX' format (e.g., 'mc17')."}

            for i in table_range:
                table_name = f"mc{i}"  # Generate table name dynamically (e.g., mc17, mc18, etc.)

                try:
                    conn, cursor = self.get_db()
 
                    # Use different queries for machines mc17-mc22 and mc25-mc30
                    if 17 <= i <= 22:
                        columns = mc17_to_mc22_columns
                    elif 25 <= i <= 30:
                        columns = mc25_to_mc30_columns
                    else:
                        continue  # Skip invalid machine IDs

                    # Construct SQL query for the specific machine
                    query = f"""
                    SELECT {', '.join(columns['front'] + columns['rear']  + columns['pressure'] + ["TO_CHAR(timestamp, 'HH24:MI:SS') AS formatted_time"])}
                    FROM {table_name}
                    ORDER BY timestamp DESC
                    LIMIT 1;
                    """
                    cursor.execute(query)
                    result = cursor.fetchone()

                    if result:
                        vertical_data[table_name] = {
                            **{columns['front'][j - 1]: result[j - 1] for j in range(1, len(columns['front']) + 1)},
                            **{columns['rear'][j - 1]: result[len(columns['front']) + j - 1] for j in range(1, len(columns['rear']) + 1)},
                            "pressure": result[-2],  # Get pressure value
                            "time": result[-1]  # Get formatted timestamp 
                        }
                        avg_query = f"""
                        SELECT 
                            AVG(COALESCE({', '.join(columns['front'])}, 0)) AS avg_front_temp,
                            AVG(COALESCE({', '.join(columns['rear'])}, 0)) AS avg_rear_temp
                        FROM {table_name};
                        """
                        cursor.execute(avg_query)
                        avg_result = cursor.fetchone()

                        if avg_result:
                            vertical_data[table_name]["avg_front_temp"] = avg_result[0]  # Average front temp
                            vertical_data[table_name]["avg_rear_temp"] = avg_result[1]  # Average rear temp
                    else:
                        # If no data is found, fetch last 100 records
                        query_last_100 = f"""
                        SELECT {', '.join(columns['front'] + columns['rear'])}
                        FROM {table_name}
                        WHERE timestamp = (SELECT MAX(timestamp) FROM {table_name})
                        ORDER BY timestamp DESC
                        LIMIT 100;
                        """
                        cursor.execute(query_last_100)
                        result_last_100 = cursor.fetchall()

                        if result_last_100:
                            vertical_data[table_name] = {
                                "last_100_records": [
                                    {
                                        **{columns['front'][j - 1]: row[j - 1] for j in range(1, len(columns['front']) + 1)},
                                        **{columns['rear'][j - 1]: row[len(columns['front']) + j - 1] for j in range(1, len(columns['rear']) + 1)},
                                        "pressure": result[-1] 
                                    }
                                    for row in result_last_100
                                ]
                            }
                        else:
                            vertical_data[table_name] = {"error": "No data found in the last 100 records"}

                    self.close_db(conn, cursor)

                except Exception as e:
                    print(f"Error fetching horizontal sealer data from {table_name}: {e}")
                    self.close_db(conn, cursor)


            return vertical_data
        except Exception as e:
            print(traceback.format_exc())  # For debugging purposes
            raise HTTPException(status_code=500, detail=str(e))
    ###############################################################
    def fetch_horizontal_sealer_graph_data(self, machine_id=None, last_timestamp=None):
        try:
            """
            Fetch horizontal sealer temperature data from a specific machine (e.g., 'mc17') or all machines ('mc17' to 'mc30').

            The column names differ for machines mc17 to mc22 and mc25 to mc30, so this function handles that accordingly.

            :param machine_id: The specific machine (e.g., 'mc17') to fetch data for. If None, fetch data for all machines.
            :param last_timestamp: The last timestamp to fetch new data points.
            """
            # Define machine ranges and column configurations
            mc17_to_mc22_columns = {
                "front": ["hor_sealer_front_1_temp"], 
                "rear": [f"hor_sealer_rear_{i}_temp" for i in range(1, 10)],
                "pressure": ["hor_pressure"] 
            }

            mc25_to_mc30_columns = {
                "front": ["hor_sealer_front"],
                "rear": ["hor_sealer_rear"],
                "pressure": ["hor_pressure"] 
            }

            vertical_data = {}

            # Handle machine ID and table range dynamically
            table_range = [i for i in range(17, 31) if i not in [23, 24]] if machine_id is None else [machine_id]

            for i in table_range:
                table_name = f"mc{i}"

                try:
                    conn, cursor = self.get_db()

                    # Select column configuration based on the machine ID
                    columns = mc17_to_mc22_columns if 17 <= i <= 22 else mc25_to_mc30_columns

                    # Fetch the last 100 records initially
                    last_100_query = f"""
                    SELECT {', '.join(columns['front'] + columns['rear'] + columns['pressure'] + ["timestamp"])}
                    FROM {table_name}
                    ORDER BY timestamp DESC
                    LIMIT 100;
                    """
                    cursor.execute(last_100_query)
                    result_last_100 = cursor.fetchall()
                    # arr_data = []
                    if result_last_100: 
                        vertical_data[table_name] = [{
                                    **{columns['front'][j - 1]: row[j - 1] for j in range(1, len(columns['front']) + 1)},
                                    **{columns['rear'][j - 1]: row[len(columns['front']) + j - 1] for j in range(1, len(columns['rear']) + 1)},
                                    "pressure": row[-2],
                                    "timestamp": row[-1]
                                }
                                for row in result_last_100
                               ]

                        
                    # vertical_data[table_name] = arr_data
                    # If a last timestamp is provided, fetch the latest data point
                    if last_timestamp:
                        new_data_query = f"""
                        SELECT {', '.join(columns['front'] + columns['rear'] + columns['pressure'] + ["TO_CHAR(timestamp, 'HH24:MI:SS') AS formatted_time"])}
                        FROM {table_name}
                        WHERE timestamp > %s
                        ORDER BY timestamp ASC
                        LIMIT 1;
                        """
                        cursor.execute(new_data_query, (last_timestamp,))
                        new_data = cursor.fetchone()

                        if new_data:
                            vertical_data[table_name]["new_data_point"] = {
                                **{columns['front'][j - 1]: new_data[j - 1] for j in range(1, len(columns['front']) + 1)},
                                **{columns['rear'][j - 1]: new_data[len(columns['front']) + j - 1] for j in range(1, len(columns['rear']) + 1)},
                                "pressure": new_data[-2],
                                "time": new_data[-1]
                            }

                    self.close_db(conn, cursor)

                except Exception as e:
                    print(f"Error fetching horizontal sealer data from {table_name}: {e}")
                    self.close_db(conn, cursor)

            return vertical_data

        except Exception as e:
            print(traceback.format_exc())  # For debugging purposes
            raise HTTPException(status_code=500, detail=str(e))

    ###############################################################
    def fetch_loop_overview(self) -> dict:
        """Fetch primary and secondary tank data from the loop3_sku and loop4_sku tables."""
        try:
            conn, cursor = self.get_db()
            cldproduction =self.fetch_cld_production()
            # print("Loop overview inside",cldproduction)
            loop3_cldproduction_count = cldproduction["loop3_cldproduction"]
            loop4_cldproduction_count = cldproduction["loop4_cldproduction"]
            tank_data = {}

            # Fetching from loop3_sku
            query_loop3 = """
                SELECT primary_tank, secondary_tank, ht_transfer_ts, level
                FROM loop3_sku
                ORDER BY ht_transfer_ts DESC
                LIMIT 1;
            """
            cursor.execute(query_loop3)
            data_loop3 = cursor.fetchone()
        
            if data_loop3:
                # Assuming ht_transfer_time is in HH:MM:SS format
                ht_transfer_time = datetime.strptime(data_loop3[2], "%H:%M:%S").time() if data_loop3[2] else None
            
                if ht_transfer_time:
                    current_time = datetime.now()
                    # Combine the current date with the transfer time to avoid date mismatch
                    ht_transfer_datetime = datetime.combine(current_time.date(), ht_transfer_time)
                
                    # Calculate the time difference
                    time_difference = current_time - ht_transfer_datetime
                
                    # Extract hours, minutes, and seconds from the time difference
                    total_seconds = time_difference.total_seconds()
                    hours, remainder = divmod(total_seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    time_difference_str = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"  # Format as HH:mm:ss
                else:
                    time_difference_str = "--"
                primary_tank_value = data_loop3[0] / 100 * 10
                secondary_tank_value = data_loop3[1] / 100 * 10

                tank_data["loop3"] = {
                   "primary_tank": round((primary_tank_value),2),
                    "secondary_tank": round((secondary_tank_value),2),
                    "ht_transfer_ts": data_loop3[2] if data_loop3[2] else "--",  # Set to '--' if no timestamp
                    "level": data_loop3[3],
                    "bulk_settling_time": time_difference_str,
                     "cld_production":loop3_cldproduction_count
                }
            else:
                tank_data["loop3"] = {
                    "primary_tank": "00",
                    "secondary_tank": "00",
                    "ht_transfer_ts": "00",
                    "level": "00",
                    "bulk_settling_time": "00",
                    "cld_production": "00"
                }

            # Fetching from loop4_sku
            query_loop4 = """
                SELECT primary_tank, secondary_tank, ht_transfer_ts, level
                FROM loop4_sku
                ORDER BY ht_transfer_ts DESC
                LIMIT 1;
            """
            cursor.execute(query_loop4)
            data_loop4 = cursor.fetchone()
            # print("data lloop 4",data_loop4)
            if data_loop4:
                # Assuming ht_transfer_time is in HH:MM:SS format
                ht_transfer_time = datetime.strptime(data_loop4[2], "%H:%M:%S").time() if data_loop4[2] else None
            
                if ht_transfer_time:
                    current_time = datetime.now()
                    # Combine the current date with the transfer time to avoid date mismatch
                    ht_transfer_datetime = datetime.combine(current_time.date(), ht_transfer_time)
                
                    # Calculate the time difference
                    time_difference = current_time - ht_transfer_datetime
                
                    # Extract hours, minutes, and seconds from the time difference
                    total_seconds = time_difference.total_seconds()
                    hours, remainder = divmod(total_seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    time_difference_str = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"  # Format as HH:mm:ss
                else:
                    time_difference_str = "--"
                primary_tank_value = data_loop4[0] / 100 * 10
                secondary_tank_value = data_loop4[1] / 100 * 10
                tank_data["loop4"] = {
                    "primary_tank": round((primary_tank_value),2),
                    "secondary_tank": round((secondary_tank_value),2),
                    "ht_transfer_ts": data_loop4[2] if data_loop4[2] else "--",  # Set to '--' if no timestamp
                    "level": data_loop4[3],
                    "bulk_settling_time": time_difference_str,
                    "cld_production": loop4_cldproduction_count
                }
            else:
                tank_data["loop4"] = {
                    "primary_tank": "00",
                    "secondary_tank": "00",
                    "ht_transfer_ts": "00",
                    "level": "00",
                    "bulk_settling_time": "00",
                    "cld_production": "00"
                }

            return tank_data

        except Exception as e:
            print(traceback.format_exc())
            return {"error": f"Error fetching data: {e}"}
    
        finally:
            self.close_db(conn, cursor)
            
    def fetch_machine_status_and_cld_data(self):
        """Fetch the latest record of status_code and cld_count for all machines (mc17 to mc22). 
        If no data is found, skip the machine.
        """
        table_range = range(17, 23)  # From mc17 to mc22
        machine_data = {}

        for i in table_range:
            table_name = f"mc{i}"  # Generate table name dynamically (e.g., mc17, mc18)
            
            try:
                conn, cursor = self.get_db()

                # Query to fetch the latest status_code and cld_count
                query = f"""
                SELECT status, cld_count 
                FROM {table_name}
                ORDER BY timestamp DESC 
                LIMIT 1;
                """
                cursor.execute(query)
                result = cursor.fetchone()

                if result:
                    # Populate machine data only if a record is found
                    machine_data[table_name] = {
                        "status": result[0],
                        "cld_count": result[1]
                    }

                # Close the database connection for the current machine table
                self.close_db(conn, cursor)

            except Exception as e:
                print(f"Error fetching data from {table_name}: {e}")
                self.close_db(conn, cursor)

        return machine_data
    
    def fetch_vertical_sealer_data(self, machine_id=None):
        try:
            """
            Fetch vertical sealer temperature data from a specific machine (e.g., 'mc17') or all machines ('mc17' to 'mc30').
            
            The column names differ for machines mc17 to mc22 and mc25 to mc30, so this function handles that accordingly.
            
            :param machine_id: The specific machine (e.g., 'mc17') to fetch data for. If None, fetch data for all machines.
            """
            # Define machine ranges and column configurations
            mc17_to_mc22_columns = {
                "front": [f"ver_sealer_front_{i}_temp" for i in range(1, 14)],
                "rear": [f"ver_sealer_rear_{i}_temp" for i in range(1, 14)],
                "pressure": ["ver_pressure"] 
            }
            
            

            mc25_to_mc30_columns = {
                "front": ["verticalsealerfront_1"],
                "rear": ["verticalsealerrear_1"],
                "pressure": ["vertical_pressure"] 
            }
            print("machine idL",machine_id)
            if(machine_id == 25 or machine_id == 26):
                mc25_to_mc30_columns["pressure"] = ["ver_pressure"]
            
            if(machine_id >= 28):
                mc25_to_mc30_columns["pressure"]= ["vertical_pressure"]
            
            # Handle machine range dynamically
            table_range = range(17, 31) if machine_id is None else []

            vertical_data = {}

            if machine_id:
                try:
                    table_number = machine_id  # Extract number from 'mcXX' format
                    table_range = [table_number]  # Fetch data for the specific machine
                except ValueError:
                    return {"error": "Invalid machine_id format. Use 'mcXX' format (e.g., 'mc17')."}

            for i in table_range:
                table_name = f"mc{i}"  # Generate table name dynamically (e.g., mc17, mc18, etc.)

                try:
                    conn, cursor = self.get_db()

                    # Use different queries for machines mc17-mc22 and mc25-mc30
                    if 17 <= i <= 22:
                        columns = mc17_to_mc22_columns
                    elif 25 <= i <= 30:
                        columns = mc25_to_mc30_columns
                    else:
                        continue  # Skip invalid machine IDs

                    # Construct SQL query for the specific machine
                    query = f"""
                    SELECT {', '.join(columns['front'] + columns['rear']  + columns['pressure'] + ["TO_CHAR(timestamp, 'HH24:MI:SS') AS formatted_time"])}
                    FROM {table_name}
                    ORDER BY timestamp DESC
                    LIMIT 1;
                    """
                    cursor.execute(query)
                    result = cursor.fetchone()

                    if result:
                        vertical_data[table_name] = {
                            **{columns['front'][j - 1]: result[j - 1] for j in range(1, len(columns['front']) + 1)},
                            **{columns['rear'][j - 1]: result[len(columns['front']) + j - 1] for j in range(1, len(columns['rear']) + 1)},
                            "pressure": result[-2],  # Get pressure value
                            "time": result[-1]  # Get formatted timestamp 
                        }
                        avg_query = f"""
                        SELECT 
                            AVG(COALESCE({', '.join(columns['front'])}, 0)) AS avg_front_temp,
                            AVG(COALESCE({', '.join(columns['rear'])}, 0)) AS avg_rear_temp
                        FROM {table_name};
                        """
                        cursor.execute(avg_query)
                        avg_result = cursor.fetchone()

                        if avg_result:
                            vertical_data[table_name]["avg_front_temp"] = avg_result[0]  # Average front temp
                            vertical_data[table_name]["avg_rear_temp"] = avg_result[1]  # Average rear temp
                    else:
                        # If no data is found, fetch last 100 records
                        query_last_100 = f"""
                        SELECT {', '.join(columns['front'] + columns['rear'])}
                        FROM {table_name}
                        WHERE timestamp = (SELECT MAX(timestamp) FROM {table_name})
                        ORDER BY timestamp DESC
                        LIMIT 100;
                        """
                        cursor.execute(query_last_100)
                        result_last_100 = cursor.fetchall()

                        if result_last_100:
                            vertical_data[table_name] = {
                                "last_100_records": [
                                    {
                                        **{columns['front'][j - 1]: row[j - 1] for j in range(1, len(columns['front']) + 1)},
                                        **{columns['rear'][j - 1]: row[len(columns['front']) + j - 1] for j in range(1, len(columns['rear']) + 1)},
                                        "pressure": result[-1] 
                                    }
                                    for row in result_last_100
                                ]
                            }
                        else:
                            vertical_data[table_name] = {"error": "No data found in the last 100 records"}

                    self.close_db(conn, cursor)

                except Exception as e:
                    print(f"Error fetching vertical sealer data from {table_name}: {e}")
                    self.close_db(conn, cursor)


            return vertical_data
        except Exception as e:
            print(traceback.format_exc())  # For debugging purposes
            raise HTTPException(status_code=500, detail=str(e))
    
    # def fetch_sealer_data_loop3_loop4(self, machine_ids, cld_count_column, loop_name):
    #     """Fetch status, speed, and CLD count for a given list of machines."""
    #     loop_data = {}
    #     conn, cursor = self.get_db()  # Corrected to use self.get_db()

    #     for machine_id in machine_ids:
    #         table_name = f"mc{machine_id}"
    #         try:
    #             if loop_name == "loop3":
    #                 # For loop3, take default speed value as 100 and select cld_count
    #                 query = f"""
    #                 SELECT status,  100 AS speed,cld_count
    #                 FROM {table_name}
    #                 ORDER BY timestamp DESC
    #                 LIMIT 1;
    #                 """
    #             else:
    #                 # For loop4, select the cld_count_count based on the current shift
    #                 query = f"""
    #                 SELECT status, speed, {cld_count_column}
    #                 FROM {table_name}
    #                 ORDER BY timestamp DESC
    #                 LIMIT 1;
    #                 """
                
    #             cursor.execute(query)
    #             result = cursor.fetchone()
    #             # print("result",result)
    #             if result:
    #                 status, speed, cld_count = result
    #                 loop_data[table_name]= {
    #                     "status": status,
    #                     "speed": speed,
    #                     "cld_count": cld_count
    #                 }
    #             else:
    #                 loop_data[table_name] = {
    #                     "status": "--",
    #                     "speed": "--",
    #                     "cld_count": "--"
    #                 }
    #         except Exception as e:
    #             loop_data[table_name] = {"error": f"Error fetching data: {e}"}

    #     self.close_db(conn, cursor)  

    #     return loop_data


    def fetch_sealer_data_loop3_loop4(self, machine_ids, cld_count_column, loop_name):
        """Fetch status, speed, and CLD count for a given list of machines."""
        loop_data = {}
        conn, cursor = self.get_db()  # Corrected to use self.get_db()

        for machine_id in machine_ids:
            table_name = f"mc{machine_id}"
            try:
                if loop_name == "loop3":
                    # For loop3, take default speed value as 100 and select cld_count
                    query = f"""
                    SELECT status, 100 AS speed, cld_count
                    FROM {table_name}
                    ORDER BY timestamp DESC
                    LIMIT 1;
                    """
                else:
                    # For loop4, set speed based on machine ID
                    default_speed = 120 if machine_id in [25, 26] else 100
                    query = f"""
                    SELECT status, {default_speed} AS speed, {cld_count_column}
                    FROM {table_name}
                    ORDER BY timestamp DESC
                    LIMIT 1;
                    """
                
                cursor.execute(query)
                result = cursor.fetchone()
                
                if result:
                    status, speed, cld_count = result
                    loop_data[table_name] = {
                        "status": status,
                        "speed": speed,
                        "cld_count": cld_count
                    }
                else:
                    loop_data[table_name] = {
                        "status": "--",
                        "speed": "--",
                        "cld_count": "--"
                    }
            except Exception as e:
                loop_data[table_name] = {"error": f"Error fetching data: {e}"}

        self.close_db(conn, cursor)  

        return loop_data

#######
    def fetch_machine_status_data_loop3_loop4(self, machine_ids, loop_name):
        """Fetch status for a given list of machines."""
        loop_data = []
        conn, cursor = self.get_db()

        for machine_id in machine_ids:
            machine_name = f"M{machine_id}"  # Update the machine name format
            try:
                # Use the same query logic for both loops
                query = f"""
                SELECT status
                FROM mc{machine_id}
                ORDER BY timestamp DESC
                LIMIT 1;
                """
                
                cursor.execute(query)
                result = cursor.fetchone()
                print(result)
                if result:
                    status_value = result[0]
                    # Map status values to human-readable text and color
                    status = "Running" if status_value == 1.0 else "Stopped"
                    color = "#00FF00" if status_value == 1.0 else "#FF0000"
                    
                    # Add machine status data to the loop_data list
                    loop_data.append({
                        "id": machine_name,  # Send the machine name as "M{id}"
                        "status": status,
                        "color": color
                    })
                else:
                    loop_data.append({
                        "id": machine_name,
                        "status": "--",  # Default when no status found
                        "color": "#808080"  # Gray for unknown status
                    })
            except Exception as e:
                loop_data.append({
                    "id": machine_name,
                    "status": "Error",
                    "color": "#FF0000",  # Red for errors
                    "error": str(e)
                })

        self.close_db(conn, cursor)

        return loop_data


    def fetch_front_temperature_data(self, machine_num=None):
        """
        Fetch the latest 100 front temperatures for specific machines. 
        Handle different column names for different machine groups.
        Machines mc17 to mc22 have ver_sealer_front_X_temp columns.
        Machines mc25 to mc30 have verticalsealerfront_1 columns.
        
        :param machine_num: The specific machine number (e.g., 17 for 'mc17') to fetch data for. If None, fetch data for all machines.
        """
        mc17_to_mc22_columns = [f"ver_sealer_front_{i}_temp" for i in range(1, 14)]
        mc25_to_mc30_columns = ["verticalsealerfront_1"]

        table_range = range(17, 31)  # Machines mc17 to mc30
        machine_data = {}

        # If a specific machine_num is provided, narrow the search to that machine
        if machine_num:
            table_range = [machine_num]  # Use the numeric machine number directly

        for i in table_range:
            table_name = f"mc{i}"  # Generate table name dynamically (e.g., mc17, mc18, etc.)
            result = None

            try:
                conn, cursor = self.get_db()

                # Use different queries for machines mc17-mc22 and mc25-mc30
                if 17 <= i <= 22:
                    columns = mc17_to_mc22_columns
                elif 25 <= i <= 30:
                    columns = mc25_to_mc30_columns
                else:
                    continue  # Skip invalid machine IDs

                # Construct SQL query to fetch last 100 records
                query = f"""
                SELECT {', '.join(columns)}, TO_CHAR(timestamp, 'HH24:MI:SS') AS formatted_time
                FROM {table_name}
                ORDER BY timestamp DESC 
                LIMIT 100;
                """
                cursor.execute(query)
                result_last_100 = cursor.fetchall()

                if result_last_100:
                    machine_data[table_name] = {
                        "last_100_records": [
                            {
                                **{columns[j]: row[j] for j in range(len(columns))},  # Map temperature columns
                                "time": row[-1]  # Add the formatted timestamp
                            }
                            for row in result_last_100
                        ]
                    }

                self.close_db(conn, cursor)

            except Exception as e:
                print(f"Error fetching data from {table_name}: {e}")
                self.close_db(conn, cursor)

        return machine_data


    def fetch_rear_temperature_data(self, machine_num=None):
        """
        Fetch the latest 100 rear temperatures for specific machines. 
        Handle different column names for different machine groups.
        Machines mc17 to mc22 have ver_sealer_rear_X_temp columns.
        Machines mc25 to mc30 have verticalsealerrear_1 columns.
        
        :param machine_num: The specific machine number (e.g., 17 for 'mc17') to fetch data for. If None, fetch data for all machines.
        """
        mc17_to_mc22_columns = [f"ver_sealer_rear_{i}_temp" for i in range(1, 14)]
        mc25_to_mc30_columns = ["verticalsealerrear_1"]

        table_range = range(17, 31)  # Machines mc17 to mc30
        machine_data = {}

        # If a specific machine_num is provided, narrow the search to that machine
        if machine_num:
            table_range = [machine_num]  # Use the numeric machine number directly

        for i in table_range:
            table_name = f"mc{i}"  # Generate table name dynamically (e.g., mc17, mc18, etc.)
            result = None

            try:
                conn, cursor = self.get_db()

                # Use different queries for machines mc17-mc22 and mc25-mc30
                if 17 <= i <= 22:
                    columns = mc17_to_mc22_columns
                elif 25 <= i <= 30:
                    columns = mc25_to_mc30_columns
                else:
                    continue  # Skip invalid machine IDs

                # Construct SQL query to fetch last 100 records
                query = f"""
                SELECT {', '.join(columns)}, TO_CHAR(timestamp, 'HH24:MI:SS') AS formatted_time
                FROM {table_name}
                ORDER BY timestamp DESC 
                LIMIT 100;
                """
                cursor.execute(query)
                result_last_100 = cursor.fetchall()

                if result_last_100:
                    machine_data[table_name] = {
                        "last_100_records": [
                            {
                                **{columns[j]: row[j] for j in range(len(columns))},  # Map temperature columns
                                "time": row[-1]  # Add the formatted timestamp
                            }
                            for row in result_last_100
                        ]
                    }

                self.close_db(conn, cursor)

            except Exception as e:
                print(f"Error fetching data from {table_name}: {e}")
                self.close_db(conn, cursor)

        return machine_data
    
    def fetch_device_data(self, limit=12):
        """Fetch telemetry data for a device from the database."""
        conn2, cursor2 = self.get_device_db()  # Establish the database connection
        try:
            query = """
            SELECT device_id, timestamp, x_rms_vel, y_rms_vel, z_rms_vel, spl_db, temp_c, label
            FROM deviceid_timeseries
            ORDER BY timestamp DESC
            LIMIT %s;
            """
            cursor2.execute(query, (limit,))
            result = cursor2.fetchall()

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
            self.close_db(conn2, cursor2)  # Ensure the database connection is closed
 


status_manager = StatusManager()

def fetch_vertical_sealer_data_by_machine_id_task(machine_id: int):
    """Task to fetch vertical sealer temperature data for a specific machine continuously."""
    while True:
        try:
            vertical_data = status_manager.fetch_vertical_sealer_data_by_machine_id(machine_id)
            yield f"{json.dumps({'data': vertical_data})}\n\n"
            time.sleep(5)  # Fetch every 5 seconds

        except Exception as e:
            yield f"data: {{'error': '{str(e)}'}}\n\n"
            break

def fetch_machine_status_and_cld():
    while True:
        try:
            machine_data = status_manager.fetch_machine_status_and_cld_data()
            yield f"{json.dumps({'data': machine_data})}\n\n"
            time.sleep(5)  # Fetch every 5 seconds

        except Exception as e:
            yield f"data: {{'error': '{str(e)}'}}\n\n"
            break

def fetch_hopper_data_task():
    """ Task to fetch machine hopper data continuously. """
    while True:
        try:
            machine_data = status_manager.fetch_machine_hopper_level_data()
            yield f"data: {json.dumps(machine_data)}\n\n"
            time.sleep(5)  # Fetch every 5 seconds

        except Exception as e:
            yield f"data: {{'error': '{str(e)}'}}\n\n"
            break

def fetch_vertical_sealer_data_task():
    """Task to fetch vertical sealer temperature data continuously."""
    while True:
        try:
            vertical_data = status_manager.fetch_vertical_sealer_data()
            yield f"data: {json.dumps(vertical_data)}\n\n"
            time.sleep(5)  # Fetch every 5 seconds

        except Exception as e:
            yield f"data: {{'error': '{str(e)}'}}\n\n"
            break

def fetch_horizontal_sealer_data_task():
    """Task to fetch vertical sealer temperature data continuously."""
    while True:
        try:
            horizontal_data = status_manager.fetch_horizontal_sealer_data()
            yield f"data: {json.dumps(horizontal_data)}\n\n"
            time.sleep(5)  # Fetch every 5 seconds

        except Exception as e:
            yield f"data: {{'error': '{str(e)}'}}\n\n"
            break


def fetch_loop_data_task():
    """ Task to fetch loop overview data continuously. """
    while True:
        try:
            tank_data = status_manager.fetch_loop_overview()
            yield f"data: {json.dumps(tank_data)}\n\n"
            time.sleep(5)  # Fetch every 5 seconds

        except Exception as e:
            yield f"data: {{'error': '{str(e)}'}}\n\n"
            break


@app.get("/hopper-levels")
def get_hopper_levels(machine_id: str):
    try:
        # Extract the numeric part from the machine_id (e.g., 'M17' becomes '17')
        machine_num = int(machine_id.replace('M', ''))

        machine_data = status_manager.fetch_machine_hopper_level_data(machine_num)
        if not machine_data:
            raise HTTPException(status_code=404, detail="No hopper level data found.")
        return JSONResponse(content=machine_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# @app.get("/hopper-levels/")
# def get_hopper_levels(machine_id: int = Query(description="Machine ID (e.g., 17 for mc17)", example=17)):
#     print(machine_id,"***")
#     try:
#         machine_data = status_manager.fetch_machine_hopper_level_data(machine_id)
       
#         if not machine_data:
#             raise HTTPException(status_code=404, detail="No hopper level data found.")
#         return JSONResponse(content=machine_data)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


@app.get("/vertical-sealers")
def get_vertical_sealers():
    try:
        vertical_data = status_manager.fetch_vertical_sealer_data()
        if not vertical_data:
            raise HTTPException(status_code=404, detail="No vertical sealer data found.")
        return JSONResponse(content=vertical_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/horizontal-sealers")
def get_horizontal_sealers():
    try:
        horizontal_data = status_manager.fetch_horizontal_sealer_data()
        if not horizontal_data:
            raise HTTPException(status_code=404, detail="No horizontal sealer data found.")
        return JSONResponse(content=horizontal_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/loop-overview")
def get_loop_overview():
    try:
        tank_data = status_manager.fetch_loop_overview()
        if not tank_data:
            raise HTTPException(status_code=404, detail="No loop overview data found.")
        return JSONResponse(content=tank_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
  
@app.get("/machine-status")
def get_machine_status():
    # Fetch data for loop3 and loop4
    loop3_data = status_manager.fetch_machine_status_data_loop3_loop4(machine_ids=range(17, 23), loop_name="loop3")
    loop4_data = status_manager.fetch_machine_status_data_loop3_loop4(machine_ids=range(25, 31), loop_name="loop4")
    machine_status_data = loop3_data + loop4_data
    # print("Machine Status", machine_status_data)
    return JSONResponse(content=machine_status_data)
@app.get("/machine-table-data")
def get_loop_overview():
    try:
        # Fetch data for loop3 machines (mc17 to mc22)
        loop3_data = status_manager.fetch_sealer_data_loop3_loop4(machine_ids=range(17, 23), cld_count_column='cld_count', loop_name="loop3")
        
        # Determine shift for loop4 machines (mc25 to mc30)
        shift, cld_count_column = status_manager.determine_shift()
        print("Shift",shift,"cld",cld_count_column)
        # Fetch data for loop4 machines (mc25 to mc30)
        loop4_data = status_manager.fetch_sealer_data_loop3_loop4(machine_ids=[25,26,27,30], cld_count_column=cld_count_column, loop_name="loop4")
        loop4_data_2_machine = status_manager.fetch_sealer_data_loop3_loop4(machine_ids=[28,29], cld_count_column="cld", loop_name="loop4")
        
        print("Loop 4 machine table call",loop4_data_2_machine)
        # Create loop3 array with latest cld_count
        loop3_array = [
            {
                "machine": f"mc{machine_id}",
                "status": loop3_data.get(f"mc{machine_id}", {}).get("status", "--"),
                "speed": loop3_data.get(f"mc{machine_id}", {}).get("speed", "--"),
                "cld_count": loop3_data.get(f"mc{machine_id}", {}).get("cld_count", "--")
            }
            for machine_id in range(17, 23)
        ]

        # Create loop4 array with latest 
        shift, cld_count_column = status_manager.determine_shift()
        print("Shift",shift,"cld",cld_count_column)
        # Fetch data for loop4 machines (mc25 to mc30)
        loop4_data = status_manager.fetch_sealer_data_loop3_loop4(machine_ids=[25,26,27,30], cld_count_column=cld_count_column, loop_name="loop4")
        loop4_data_2_machine = status_manager.fetch_sealer_data_loop3_loop4(machine_ids=[28,29], cld_count_column="cld", loop_name="loop4")
        
        print("Loop 4 machine table call",loop4_data_2_machine)
        # Create loop3 array with latest cld_count
        loop3_array = [
            {
                "machine": f"mc{machine_id}",
                "status": loop3_data.get(f"mc{machine_id}", {}).get("status", "--"),
                "speed": loop3_data.get(f"mc{machine_id}", {}).get("speed", "--"),
                "cld_count": loop3_data.get(f"mc{machine_id}", {}).get("cld_count", "--")
            }
            for machine_id in range(17, 23)
        ]

        loop4_array = []
        for machine_id in range(25, 31):
            jsonObj = { "machine": f"mc{machine_id}"}
            if machine_id not in [25, 26, 27, 30]:  # Machines that use shift-specific columns
                jsonObj["status"] = loop4_data_2_machine.get(f"mc{machine_id}", {}).get("status", "--")
                jsonObj["speed"] = loop4_data_2_machine.get(f"mc{machine_id}", {}).get("speed", "--")
                jsonObj["cld_count"] = loop4_data_2_machine.get(f"mc{machine_id}", {}).get("cld_count", "--")
            else:
                jsonObj["status"] = loop4_data.get(f"mc{machine_id}", {}).get("status", "--")
                jsonObj["speed"] = loop4_data.get(f"mc{machine_id}", {}).get("speed", "--")
                jsonObj["cld_count"] = loop4_data.get(f"mc{machine_id}", {}).get("cld_count", "--")
                 
            loop4_array.append(jsonObj)

        combined_data = {
            "loop3": loop3_array,
            "loop4": loop4_array
        }

       

        return JSONResponse(content=combined_data)

    except Exception as e:
        print(traceback.format_exc())  # For debugging purposes
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/vertical-sealer-machine-data")
def get_loop_overview(machine_id: str):
    try:
        # Validate machine_id before fetching data
        machine_num = int(machine_id.replace('M', ''))
        print("machine_num:", machine_num)
        # machine_data = status_manager.fetch_vertical_sealer_data(machine_num)
    # if machine_id in machine_num:
    #     raise HTTPException(status_code=400, detail="Invalid machine ID. Must be between 17 and 22.")
    
    
        machine_data = status_manager.fetch_vertical_sealer_data(machine_num)
        
        # if not data:
        #     raise HTTPException(status_code=404, detail="No loop overview data found.")
        
        return JSONResponse(content=machine_data)

    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/horizontal-sealer-machine-data")
def get_loop_overview(machine_id: str):
    try:
        # Validate machine_id before fetching data
        machine_num = int(machine_id.replace('M', ''))
        print("machine_num:", machine_num)
        # machine_data = status_manager.fetch_vertical_sealer_data(machine_num)
    # if machine_id in machine_num:
    #     raise HTTPException(status_code=400, detail="Invalid machine ID. Must be between 17 and 22.")
    
    
        machine_data = status_manager.fetch_horizontal_sealer_data(machine_num)
        
        # if not data:
        #     raise HTTPException(status_code=404, detail="No loop overview data found.")
        
        return JSONResponse(content=machine_data)

    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
def format_data_hopper(data: List[tuple]) -> List[dict]:
    """
    Format database query results into JSON-serializable format.
    """

    formatted_data = [
        {
            "timestamp": record[0],
            "hopper_level": record[1],
            # "rotational_pulling_current": round(float(record[2]), 2),
        }
        for record in data
    ]
    return formatted_data



@app.get("/hopper_level_data/{machine_num}")
async def get_hopper_level_data(machine_num: str):
    conn, cursor = status_manager.get_db()
    try:
        # Fetch hopper level data for the specified machine number
        machine_num = int(machine_num.replace('M', ''))
        data = status_manager.fetch_machine_hopper_level_graph_data(machine_num)
        if not data:
            raise HTTPException(status_code=404, detail="No data found for the machine")
        
        # Format data
        response_data = data
        return response_data
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from the server")
    finally:
        status_manager.close_db(conn, cursor)

"""
"""
@app.get("/horizontal_sealer_graph_data/{machine_num}")
async def get_horizontal_sealer_graph_data(machine_num: str):
    conn, cursor = status_manager.get_db()
    try:
        # Fetch hopper level data for the specified machine number
        machine_num = int(machine_num.replace('M', ''))
        data = status_manager.fetch_horizontal_sealer_graph_data(machine_num)
        if not data:
            raise HTTPException(status_code=404, detail="No data found for the machine")
        
        # Format data
        response_data = data
        return response_data
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from the server")
    finally:
        status_manager.close_db(conn, cursor)


@app.get("/fetch-device-data")
async def fetch_device_data(request: Request, recent: bool = True):
    """
    Fetch the most recent record for a given device. 
    If no recent data is found, fetch the last 100 records.
    """
    try:
        # Fetch the most recent or last 100 records
        if recent:
            data = status_manager.fetch_device_data(limit=12)
            if not data:
                data = status_manager.fetch_device_data(limit=12)
        else:
            data = status_manager.fetch_device_data(limit=12)

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
    
@app.get("/notifications")
async def get_notifications(use_dummy_data: bool = True):
    try:
        if use_dummy_data:
            # Dummy data for testing
            dummy_notifications = [
                {
                    "timestamp": datetime.now().isoformat(),
                    "event_id": "EVT001",
                    "zone": "Zone A",
                    "camera_id": "CAM01",
                    "assigned_to": "User1",
                    "action": "Investigate",
                    "remark": "High priority",
                },
                {
                    "timestamp": datetime.now().isoformat(),
                    "event_id": "EVT002",
                    "zone": "Zone B",
                    "camera_id": "CAM02",
                    "assigned_to": "User2",
                    "action": "Resolve",
                    "remark": "Medium priority",
                },
                {
                    "timestamp": datetime.now().isoformat(),
                    "event_id": "EVT003",
                    "zone": "Zone C",
                    "camera_id": "CAM03",
                    "assigned_to": "User3",
                    "action": "Monitor",
                    "remark": "Low priority",
                },
            ]
            return dummy_notifications

        # Real database connection
        # conn = status_manager.get_db()
        # query = "SELECT timestamp, event_id, zone, camera_id, assigned_to, action, remark FROM event_table ORDER BY timestamp DESC"
        # rows = await conn.fetch(query)
        # await conn.close()

        # notifications = [
        #     {
        #         "timestamp": row["timestamp"].isoformat(),
        #         "event_id": row["event_id"],
        #         "zone": row["zone"],
        #         "camera_id": row["camera_id"],
        #         "assigned_to": row["assigned_to"],
        #         "action": row["action"],
        #         "remark": row["remark"],
        #     }
        #     for row in rows
        # ]
        # return notifications

    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to retrieve notifications")
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)