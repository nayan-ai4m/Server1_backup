import os
import json
import psycopg2
import pandas as pd
from psycopg2 import sql


####################################################################################################################

# This scriot collects latest 6000 cycles MAE data and calculates thresholds.

# Updates leakage_thresholds.json file in "present working directory".

# Provide table_name of respective machine before execution of script.

#####################################################################################################################



################# Function to collect latest 6K cycles MAE #####################################################

def get_mae_data(table_name):
    """
    Safely fetches MAE columns from the specified table using psycopg2 and returns a pandas DataFrame.
    """
    query = sql.SQL("""
        SELECT mae_seal_current, mae_sealer_position, mae_seal_pressure,
               mae_front_temp, mae_rear_temp
        FROM {}
        ORDER BY timestamp DESC
        LIMIT 6000;
    """).format(sql.Identifier(table_name))

    try:
        # Establish connection
        conn = psycopg2.connect(
            host='localhost',
            dbname='short_data_hul',
            user='postgres',
            password='ai4m2024',
            port='5432'
        )
        # Use cursor to execute and fetch results
        cur = conn.cursor()
        cur.execute(query)

        # Get column names from cursor
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=columns)

        # Close connection
        cur.close()
        conn.close()
        return df

    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure


########################### Function to calculate thresholds ###################################################

def save_leakage_thresholds(MAE_data):
    """
    Computes thresholds, adjusts specific keys, saves to JSON, and prints values.
    """
    thresholds_dict = (MAE_data.mean() + 3 * MAE_data.std()).to_dict()

    for key in ['mae_front_temp', 'mae_rear_temp']:
        if key in thresholds_dict:
            thresholds_dict[key] *= 4

    thresholds_dict = {k: round(v, 5) for k, v in thresholds_dict.items()}

    output_path = os.path.join(os.getcwd(), "leakage_thresholds.json")
    with open(output_path, "w") as f:
        json.dump(thresholds_dict, f, indent=2)

    print("Leakage Thresholds:")
    for key, value in thresholds_dict.items():
        print(f"{key}: {value}")


########################################### Main function ###########################################################

def main(table_name):
    mae_df = get_mae_data(table_name)
    if not mae_df.empty:
        save_leakage_thresholds(mae_df)
    else:
        print("No data returned. Thresholds not saved.")

###############################################################################################################


if __name__ == "__main__":


    ################################## Provide input variables ##############################################

    table_name = "mc17_autoencoder"

    ################################################################################################################
    
    main(table_name)
