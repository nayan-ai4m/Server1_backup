import json
from kafka import KafkaProducer
import datetime

producer = KafkaProducer(bootstrap_servers=['192.168.1.149:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
data = {"machine":17,"type":"front_temp","setpoint":150,"current":149.2,"timestamp":str(datetime.datetime.now())}
data = {"machine":17,"type":"pressure","setpoints":{'s1':stroke1,'s2':stroke2,'front':setpoint_front_temp,'rear':setpoint_rear_temp},"current":{'front':front_temp,'rear':rear_temp,
    'pressure':avg_pressure},"timestamp":str(datetime.datetime.now()),"alert":tpdata}
future = producer.send('suggestion',value=data)
