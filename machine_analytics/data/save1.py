from kafka import KafkaConsumer
import json
import os
import cv2
from datetime import datetime

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'loop3'
SAVE_DIR = '/home/ai4m/develop/data/Bad_Orintation'

# Single RTSP stream (all zones are part of this stream)
VIDEO_SOURCE = 'rtsp://admin:unilever2024@192.168.1.21:554/Streaming/Channels/101'

# Ensure save directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# Track saved object IDs
saved_object_ids = set()

# Open single video capture for the RTSP stream
video_cap = cv2.VideoCapture(VIDEO_SOURCE)

def get_frame_at_timestamp(timestamp):
    if not video_cap.isOpened():
        print(f"Error: Video source not available.")
        return None
    
    video_cap.set(cv2.CAP_PROP_POS_MSEC, timestamp)
    ret, frame = video_cap.read()
    return frame if ret else None

def process_message(message):
    try:
        data = json.loads(message.value.decode('utf-8'))

        # Validate objects
        if 'objects' not in data or not isinstance(data['objects'], list):
            print(f"Invalid 'objects' format: {data.get('objects')}")
            return

        # Parse timestamp
        kafka_timestamp = datetime.strptime(data['@timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        video_timestamp = kafka_timestamp.timestamp() * 1000  # ms for OpenCV

        for obj in data['objects']:
            if not isinstance(obj, str):
                print(f"Skipping malformed object: {obj}")
                continue

            parts = obj.split('|')
            if len(parts) < 9:
                print(f"Incomplete object data: {obj}")
                continue

            object_id = parts[0]
            x_min, y_min, x_max, y_max = map(float, parts[2:6])
            sensor_id = parts[8]  # Extract sensor ID from object (this isn't used here)
            classification = parts[-1]

            # Process 'bad' classifications only
            if classification == 'bad' and object_id not in saved_object_ids:
                saved_object_ids.add(object_id)
                frame = get_frame_at_timestamp(video_timestamp)

                if frame is not None:
                    save_image(frame, object_id, x_min, y_min, x_max, y_max)
                else:
                    print(f"No frame captured for object ID {object_id}")

    except Exception as e:
        print(f"Error processing message: {e}")

def save_image(frame, object_id, x_min, y_min, x_max, y_max):
    cropped_img = frame[int(y_min):int(y_max), int(x_min):int(x_max)]
    if cropped_img.size > 0:
        filename = os.path.join(SAVE_DIR, f"{object_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        cv2.imwrite(filename, cropped_img)
        print(f"Saved image: {filename}")
    else:
        print(f"Skipped empty image for object ID {object_id}")

if __name__ == "__main__":
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKER)
    print("Listening for messages...")

    for message in consumer:
        process_message(message)

    # Release video capture on exit
    video_cap.release()

