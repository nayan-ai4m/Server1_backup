import psycopg2
import pandas as pd
from keras.models import load_model
import pickle
import time
import json
import os

# Load the saved model and scaler
print("Loading model and scaler...")
model = load_model('/home/ai4m/develop/backup/develop/DQ/Seal_quality/MC19/MC19_seal_quality_deploymnet_files/mc19_seal_strength_model3.keras')
with open('/home/ai4m/develop/backup/develop/DQ/Seal_quality/MC19/MC19_seal_quality_deploymnet_files/mc19_seal_strength_scaler3.pkl', 'rb') as f:
    scaler = pickle.load(f)

print("Model and scaler loaded successfully.")

# Database connection parameters for different databases
short_data_conn_params = {
    'dbname': 'short_data_hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432'
}

hul_conn_params = {
    'dbname': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432'
}

# Fetch data from PostgreSQL by spare1
def fetch_data_by_spare1(spare1):
    print(f"Fetching data for spare1: {spare1}")
    conn = psycopg2.connect(**short_data_conn_params)
    cursor = conn.cursor()

    query = f"""
    SELECT hor_sealer_front_1_temp, 
           hor_sealer_rear_1_temp, 
           hor_pressure, 
           hor_sealer_current/10.0 ashor_sealer_current, 
           cam_position, 
           timestamp,
           spare1
    FROM mc19_short_data
    WHERE spare1 = '{spare1}' ORDER BY timestamp DESC LIMIT 1000;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    columns = ['hor_sealer_front_1_temp', 
               'hor_sealer_rear_1_temp', 
               'hor_pressure', 
               'hor_sealer_current', 
               'cam_position', 
               'timestamp',
               'spare1']
    print(f"Data fetched for spare1: {spare1}, rows: {len(rows)}")
    return pd.DataFrame(rows, columns=columns)

# Function to calculate thickness from GSM and density
def calculate_thickness(gsm, density):
    return float(gsm) / density

# Function to calculate the K_effective
def calculate_K_effective(json_path, k_A, k_B, k_C):
    with open(json_path, 'r') as f:
        data = json.load(f)

    # Extract GSM values
    gsm_A = data['Layer3GSM']  # GSM for BOPP
    gsm_B = data['Layer2GSM']  # GSM for MET BOPP
    gsm_C = data['Layer1GSM']  # GSM for LDPE

    # Densities in g/m³
    density_A = 900000  # BOPP
    density_B = 930000  # MET BOPP
    density_C = 925000  # LDPE

    # Calculate thicknesses
    d_A = calculate_thickness(gsm_A, density_A)
    d_B = calculate_thickness(gsm_B, density_B)
    d_C = calculate_thickness(gsm_C, density_C)

    # Calculate K_effective
    K_effective = (d_A / k_A) + (d_B / k_B) + (d_C / k_C)
    print("----------------------------------------------------")
    print(K_effective)
    print("----------------------------------------------------")

    return K_effective

# Define calculate_averages function
def calculate_averages(df, K_effective):
    print(f"Calculating averages for spare1: {df['spare1'].iloc[0]}")
    df['T'] = (df['hor_sealer_front_1_temp'] + df['hor_sealer_rear_1_temp']) / 2
    filtered_df = df[(df['cam_position'] >= 160) & (df['cam_position'] <= 215)]
    
    avg_P = filtered_df['hor_pressure'].mean() if not filtered_df.empty else 0
    avg_Is = filtered_df['hor_sealer_current'].mean() if not filtered_df.empty else 0
    cam_position = df['cam_position'].iloc[-1]
    Ts = 56
    t = 0.160
    A = 0.00516
    d = 0.000352

    E_heat = ((df['T'].mean() - Ts) * A * t) / K_effective
    E_press = avg_P * A * d * 98066.5

    print(f"Averages calculated: T={df['T'].mean()}, P={avg_P}, Is={avg_Is}")
    return pd.DataFrame([{
        'T': df['T'].mean(),
        'Ts': Ts,
        'K': K_effective,
        'Is': avg_Is,
        'P': avg_P,
        'E_heat': E_heat,
        'E_press': E_press,
        'cam_position': cam_position,
        'spare1': df['spare1'].iloc[0]
    }])

# Predict class based on averaged data
def predict_class(averaged_data):
    print(f"Making prediction for spare1: {averaged_data['spare1'].iloc[0]}")
    scaled_input = scaler.transform(averaged_data.drop(columns=['cam_position', 'spare1']))
    prediction = model.predict(scaled_input)
    print(f"Prediction: {prediction}")
    return prediction

# Update the seal quality with the prediction
def update_seal_strength(spare1, prediction):
    print(f"Updating seal strength for spare1: {spare1} with prediction: {prediction}")
    conn = psycopg2.connect(**hul_conn_params)  # Using hul database connection
    cursor = conn.cursor()

    update_query = """UPDATE mc19 SET spare2 = %s WHERE spare1 = %s AND timestamp IN (SELECT timestamp FROM mc19 ORDER BY timestamp DESC LIMIT 100);"""
    cursor.execute(update_query, (float(prediction), spare1))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Seal strength updated for spare1: {spare1}")

# Get the latest spare1
def get_latest_spare1():
    print("Fetching latest spare1...")
    conn = psycopg2.connect(**short_data_conn_params)  # Using short_data_hul database connection
    cursor = conn.cursor()
    query = "SELECT spare1 FROM mc19_short_data ORDER BY timestamp DESC LIMIT 1;"
    cursor.execute(query)
    latest_spare1 = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    print(f"Latest spare1 fetched: {latest_spare1}")
    return latest_spare1

# Main function
def main():
    current_spare1 = None
    json_path = 'GSM.json'

    # Constants for thermal conductivities
    k_A = 0.1  # BOPP
    k_B = 0.15  # MET BOPP
    k_C = 0.33  # LDPE

    # Calculate initial K_effective
    K_effective = calculate_K_effective(json_path, k_A, k_B, k_C)
    last_modified_time = os.path.getmtime(json_path)

    while True:
        latest_spare1 = get_latest_spare1()

        # Check JSON file modifications
        current_time = time.time()
        if current_time - last_modified_time >= 3:
            new_modified_time = os.path.getmtime(json_path)
            if new_modified_time > last_modified_time:
                print("JSON file updated, recalculating K_effective...")
                K_effective = calculate_K_effective(json_path, k_A, k_B, k_C)
                last_modified_time = new_modified_time

        if current_spare1 is None:
            current_spare1 = latest_spare1
            print(f"Initial spare1 set to: {current_spare1}")

        elif latest_spare1 != current_spare1:
            print(f"Processing data for previous spare1: {current_spare1}")

            df = fetch_data_by_spare1(current_spare1)
            if not df.empty:
                averaged_data = calculate_averages(df, K_effective)
                predicted_class = predict_class(averaged_data)
                print(predicted_class, predicted_class[0][0])
                print('---------------------------->')
                update_seal_strength(current_spare1, predicted_class[0][0])
            else:
                print(f"No data found for spare1: {current_spare1}")
            
            current_spare1 = latest_spare1

        time.sleep(0.1)

if __name__ == "__main__":
    main()
