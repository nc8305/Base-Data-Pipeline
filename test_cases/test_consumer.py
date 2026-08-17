import json
from kafka import KafkaConsumer

def main():
    topic = 'hpc-realtime-alert'
    print(f"Đang lắng nghe cảnh báo từ topic '{topic}'...")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['127.0.0.1:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    try:
        for message in consumer:
            alert = message.value
            print(f"\n[CẢNH BÁO NHẬN ĐƯỢC] lúc {message.timestamp}")
            print(json.dumps(alert, indent=4, ensure_ascii=False))
            
    except KeyboardInterrupt:
        print("\nĐã dừng nhận dữ liệu.")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
