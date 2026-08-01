import json
import time
import random
import os
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'iot_bus_data'

print(f"Connecting to Kafka at {KAFKA_BROKER}...")

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka!")
        break
    except Exception as e:
        print(f"Waiting for Kafka to be ready... Error: {e}")
        time.sleep(5)

def generate_bus_data(index):

    is_missing_speed = random.random() < 0.15
    is_missing_gps = random.random() < 0.1

    speed = random.randint(0, 60) if not is_missing_speed else None
    
    latitude = round(10.762622 + random.uniform(-0.05, 0.05), 6) if not is_missing_gps else None
    longitude = round(106.660172 + random.uniform(-0.05, 0.05), 6) if not is_missing_gps else None

    return {
        "vehicle_id": f"bus_SG_{index % 500}",
        "route_id": f"route_{random.randint(1, 100)}",
        "latitude": latitude,
        "longitude": longitude,
        "timestamp": time.time(),
        "speed": speed
    }

if __name__ == "__main__":
    print(f"Starting to send data to topic '{TOPIC_NAME}'...")
    try:
        index = 0
        while True:
            # Gửi dữ liệu giả lập (ví dụ gửi 5 xe mỗi lần lặp)
            for _ in range(5):
                data = generate_bus_data(index)
                producer.send(TOPIC_NAME, data)
                print(f"Sent: {data}")
                index += 1

            producer.flush()
            time.sleep(1) # Mỗi 1 giây gửi 1 batch
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()
