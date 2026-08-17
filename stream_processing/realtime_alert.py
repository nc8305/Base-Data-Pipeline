import os
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time

def parse_data(data_string):
    """
    Chuyển đổi chuỗi JSON từ Kafka thành Tuple:
    (machine_id, cpu_usage, count, timestamp)
    """
    try:
        data = json.loads(data_string)
        
        # Lọc chỉ lấy bản ghi "procstat" (dữ liệu CPU/RAM) từ producer có sẵn
        if data.get("name") != "procstat":
            return ("error", 0.0, 1, "")
            
        # Lấy machine_id từ tags.host
        machine_id = data.get("tags", {}).get("host", "unknown")
        
        # Trong producer.py, cpu_time_system random từ 0.1 đến 1.5. 
        # Nhân 100 để giả lập % CPU usage (dao động từ 10% - 150%)
        cpu_usage = float(data.get("fields", {}).get("cpu_time_system", 0.0)) * 100
        
        # Lấy timestamp
        timestamp = str(data.get("timestamp", ""))
        
        return (machine_id, cpu_usage, 1, timestamp)
    except Exception:
        # Trả về giá trị lỗi để filter ra sau
        return ("error", 0.0, 1, "")

def reduce_cpu(val1, val2):
    """
    Hàm Reduce: Cộng dồn tổng CPU và đếm số lượng bản ghi trong window
    """
    return (
        val1[0],               # machine_id
        val1[1] + val2[1],     # tổng cpu_usage
        val1[2] + val2[2],     # tổng count
        val2[3]                # lấy timestamp của bản ghi cuối cùng
    )

def calculate_avg_and_alert(data):
    """
    Hàm Map cuối: Tính CPU trung bình sau khi Reduce và đánh giá ngưỡng.
    Nếu Avg CPU > 90%, trả về JSON cảnh báo, ngược lại trả về None.
    """
    machine_id, total_cpu, count, last_timestamp = data
    if count == 0:
        return None
    
    avg_cpu = total_cpu / count
    
    # Ngưỡng trượt: Trung bình CPU lớn hơn 90% trong 10 giây (test)
    if avg_cpu > 90.0:
        alert = {
            "machine_id": machine_id,
            "alert_type": "HIGH_CPU_AVERAGE_10_SECS",
            "value": round(avg_cpu, 2),
            "severity": "CRITICAL",
            "timestamp": last_timestamp
        }
        return json.dumps(alert)
    return None

def main():
    # 1. Khởi tạo môi trường Flink
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # (Quan trọng) Tự động lấy đường dẫn tuyệt đối tới file jar
    jar_path = f"file://{os.path.abspath(os.path.join(os.path.dirname(__file__), 'jars', 'flink-sql-connector-kafka.jar'))}"
    env.add_jars(jar_path)

    # 2. Cấu hình Kafka Source
    kafka_props = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'flink_alert_group',
        'auto.offset.reset': 'latest'
    }
    
    source = FlinkKafkaConsumer(
        topics='hpc-raw-metrics', # Đổi sang topic của producer.py
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    
    stream = env.add_source(source)

    # 3. DataStream Pipeline: Map -> Filter -> KeyBy -> Window -> Reduce -> Map -> Filter
    parsed_stream = stream.map(
        parse_data, 
        output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.INT(), Types.STRING()])
    )
    
    # Lọc các dòng bị lỗi JSON
    valid_stream = parsed_stream.filter(lambda x: x[0] != "error")

    # Gom nhóm theo machine_id
    keyed_stream = valid_stream.key_by(lambda x: x[0], key_type=Types.STRING())

    # Cắt Window 10 giây để test nhanh (Tumbling Processing Time Window)
    windowed_stream = keyed_stream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))

    # Tính tổng trong 10 giây
    reduced_stream = windowed_stream.reduce(reduce_cpu)

    # Tính trung bình và xuất cảnh báo (loại bỏ giá trị None)
    alert_stream = reduced_stream.map(
        calculate_avg_and_alert, 
        output_type=Types.STRING()
    ).filter(lambda x: x is not None)

    # 4. Cấu hình Kafka Sink
    sink = FlinkKafkaProducer(
        topic='hpc-realtime-alert',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': '127.0.0.1:9092'}
    )
    
    alert_stream.add_sink(sink)

    # 5. Kích hoạt thực thi
    env.execute("HPC Realtime Alerting Job - 10 Sec Average CPU")

if __name__ == '__main__':
    main()
