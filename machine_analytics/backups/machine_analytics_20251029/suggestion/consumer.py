from kafka import KafkaConsumer
import json
import numpy as np
import joblib
import psycopg2
import datetime
# Kafka configuration

temperature_model = joblib.load('temperature_model.pkl')
tempertaure_scaler = joblib.load('temperature_scaler.pkl')
pressure_model = joblib.load('pressure_model.pkl')
pressure_scaler = joblib.load('pressure_scaler.pkl')
db_params = {
        'dbname': 'hul',
        'user': 'postgres',
        'password': 'ai4m2024',
        'host': 'localhost',
        'port': '5432'
    }
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()
"""
 {"machine":17,"type":"rear_temp","setpoint":setpoint_rear_temp,"current":rear_temp,"timestamp":str(datetime.datetime.now()),"alert":tpdata}

"""

consumer = KafkaConsumer(
    'suggestion',  # Topic to subscribe to
    bootstrap_servers=['192.168.1.149:9092'],  # Replace with your Kafka broker(s)
    auto_offset_reset='latest',  # Start reading at the earliest message
    enable_auto_commit=True,  # Automatically commit offsets
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON
)

def insert_event(machine,message):
    machine_name = 'mc'+str(machine)
        #fault_message = self.fault_manager.get_message(machine_name, int(fault))
    print(message)
    query = """
            INSERT INTO suggestions("timestamp", machine_number, alert_details, suggestion_details, acknowledge) VALUES (%s, %s, %s, %s, 1);"""
    params = (
            datetime.datetime.now(),
            'mc17',
            '{"tp":62}',
            str(message)
    ) 
    cursor.execute(query,(params))
    conn.commit()


def predict_result(temp, stroke, gsm, amb,scaler,model):
    # Prepare input as a numpy array
    input_data = np.array([[temp, stroke, gsm, amb]])

    # Scale the input data
    input_data_scaled = scaler.transform(input_data)

    # Make prediction
    prediction = model.predict(input_data_scaled)

    return prediction[0]
def predict_result_p(temp, stroke, gsm, amb,pres,scaler,model):
    input_data = np.array([[temp, stroke, gsm, amb,pres]])
    # Scale the input data
    input_data_scaled = scaler.transform(input_data)

    # Make prediction
    prediction = model.predict(input_data_scaled)

    return prediction[0]

"""
{"machine":17,"type":"rear_temp","setpoints":{'s1':stroke1,'s2':stroke2,'front':setpoint_front_temp,'rear':setpoint_rear_temp},"current":{'front':front_temp,'rear':rear_temp},"timestamp":str(datetime.datetime.now()),"alert":tpdata}"""
try:
    ambient_temp = 29.02
    GSM = 59.5
    print("Starting consumer...")
    # Consume messages
    for message in consumer:
        # Access the deserialized JSON data
        try:
            data = message.value
            print(data)
            machine = data['machine']
            alert_type = data['type']
            setpoints = data['setpoints']
            current = data['current']
            if alert_type == 'pressure':
                pressure = current['pressure']
                input_variable = [current['rear'] ,setpoints['s1'] ,GSM, ambient_temp,pressure]
                while True:
                    result = predict_result_p(*input_variable,pressure_scaler,pressure_model)
                    if result != 0:
                        input_variable[1] += 1.0
                        result = predict_result_p(*input_variable,pressure_scaler,pressure_model)
                    else :
                        break
                print("suggested pressure",input_variable[1])
            if alert_type == 'rear_temp':
                input_variable = [current['rear'] ,setpoints['s1'] ,GSM, ambient_temp]
                while True:
                    result = predict_result(*input_variable,temperature_scaler,temperature_model)
                    if result != 0:
                        input_variable[0] += 0.3
                        result = predict_result(*input_variable,temperature_scaler,temperature_model)
                        print(result)
                    else :
                        break

            if alert_type == 'front_temp':
                input_variable = [current['front'] ,setpoints['s1'] ,GSM, ambient_temp]
                while True:
                    result = predict_result(*input_variable,temperature_scaler,temperature_model)
                    if result != 0:
                        input_variable[0] += 0.3
                        result = predict_result(*input_variable,temperature_scaler,temperature_model)
                        print(result)
                    else :
                        break
            print("suggested temp {} ".format(round(input_variable[0],2)))
            """"suggestion": {"HMI_Hor_Sealer_Strk_1": 55, "HMI_Hor_Sealer_Strk_2": 3, "text": "Increase Stroke 1 to 55", "HMI_Hor_Seal_Rear_28": 155, "front_temp": 155}"""
            event = {  "suggestion": {
                "HMI_Hor_Sealer_Strk_1":round(54.9,2),"HMI_Hor_Sealer_Strk_2":round(3),"HMI_Hor_Seal_Front_28":round(158),"HMI_Hor_Seal_Front_27":round(158)
                }
                }
            """
            event = {  "suggestion": {
                "HMI_Hor_Sealer_Strk_1":round(setpoints['s1'],2),"HMI_Hor_Sealer_Strk_2":round(setpoints['s2']),"HMI_Hor_Seal_Front_28":round(input_variable[0],2),"HMI_Hor_Seal_Front_27":round(input_variable[0],2)
                }
                }
            """
            insert_event(machine,json.dumps(event))
            print(data['type'])
        except Exception as e :
            print(e)



except KeyboardInterrupt:
    print("Consumer interrupted")
finally:
    # Close the consumer
    consumer.close()
