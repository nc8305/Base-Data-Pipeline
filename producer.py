import json
import time
import random
import os
from kafka import KafkaProducer
from datetime import datetime

# Cấu hình Kafka Broker (nếu chạy qua docker-compose thì dùng tên service 'kafka', nếu chạy ngoài thì dùng 'localhost')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'iot_sensor_data'

print(f"Connecting to Kafka at {KAFKA_BROKER}...")
# Cố gắng kết nối tới Kafka
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

def generate_sensor_data(sensor_id):
    # Giả lập đôi khi bị mất dữ liệu (để phục vụ bài toán Imputation của bạn)
    # Tỉ lệ 20% dữ liệu sẽ bị None (khuyết thiếu)
    is_missing_temp = random.random() < 0.2
    is_missing_humidity = random.random() < 0.2

    temperature = round(random.uniform(20.0, 35.0), 2) if not is_missing_temp else None
    humidity = round(random.uniform(40.0, 80.0), 2) if not is_missing_humidity else None

    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": temperature,
        "humidity": humidity
    }

if __name__ == "__main__":
    print(f"Starting to send data to topic '{TOPIC_NAME}'...")
    try:
        while True:
            # Giả lập có 3 sensor
            for sensor_id in ["sensor_1", "sensor_2", "sensor_3"]:
                data = generate_sensor_data(sensor_id)
                producer.send(TOPIC_NAME, data)
                print(f"Sent: {data}")
            
            # Flush để đảm bảo data được gửi đi
            producer.flush()
            time.sleep(2) # Mỗi 2 giây gửi 1 lần
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()
