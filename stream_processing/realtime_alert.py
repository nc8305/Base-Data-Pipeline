import os
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
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

def calculate_avg(data):
    """
    Hàm Map trung gian: chỉ tính trung bình, không rẽ nhánh định dạng output.
    Trả về (machine_id, avg_cpu, last_timestamp) hoặc None nếu dưới ngưỡng / count=0.
    Cả nhánh Kafka (JSON string) và nhánh ClickHouse (tuple) đều build từ kết quả này,
    để tránh lặp lại logic ngưỡng cảnh báo ở hai nơi.
    """
    machine_id, total_cpu, count, last_timestamp = data
    if count == 0:
        return None

    avg_cpu = total_cpu / count

    # Ngưỡng trượt: Trung bình CPU lớn hơn 90% trong 10 giây (test)
    if avg_cpu > 90.0:
        return (machine_id, round(avg_cpu, 2), last_timestamp)
    return None

def to_alert_json(data):
    """Nhánh Kafka: format alert thành JSON string như cũ (giữ nguyên contract cho test_consumer.py)"""
    machine_id, avg_cpu, last_timestamp = data
    alert = {
        "machine_id": machine_id,
        "alert_type": "HIGH_CPU_AVERAGE_10_SECS",
        "value": avg_cpu,
        "severity": "CRITICAL",
        "timestamp": last_timestamp
    }
    return json.dumps(alert)

def to_alert_tuple(data):
    """Nhánh ClickHouse: format alert thành tuple 5 cột khớp bảng hpc_monitor.realtime_alerts"""
    machine_id, avg_cpu, last_timestamp = data
    return (machine_id, "HIGH_CPU_AVERAGE_10_SECS", avg_cpu, "CRITICAL", last_timestamp)

def main():
    # 1. Khởi tạo môi trường Flink
    env = StreamExecutionEnvironment.get_execution_environment()

    # (Quan trọng) Tự động lấy đường dẫn tuyệt đối tới các file jar cần thiết:
    # - flink-sql-connector-kafka: đọc/ghi Kafka
    # - flink-connector-jdbc: sink JDBC generic
    # - clickhouse-jdbc (shaded): driver JDBC cụ thể cho ClickHouse
    jars_dir = os.path.join(os.path.dirname(__file__), 'jars')
    kafka_jar = f"file://{os.path.abspath(os.path.join(jars_dir, 'flink-sql-connector-kafka.jar'))}"
    jdbc_jar = f"file://{os.path.abspath(os.path.join(jars_dir, 'flink-connector-jdbc-3.1.2-1.18.jar'))}"
    clickhouse_driver_jar = f"file://{os.path.abspath(os.path.join(jars_dir, 'clickhouse-jdbc-0.6.0-patch5-shaded.jar'))}"
    env.add_jars(kafka_jar, jdbc_jar, clickhouse_driver_jar)

    # 2. Cấu hình Kafka Source
    # KAFKA_BROKER mặc định 'kafka:29092' vì Flink giờ chạy TRONG docker-compose network
    # (service name, không phải 127.0.0.1). Muốn chạy tay ngoài host như trước (MiniCluster),
    # set env var KAFKA_BROKER=127.0.0.1:9092 trước khi chạy `python realtime_alert.py`.
    kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:29092')
    kafka_props = {
        'bootstrap.servers': kafka_broker,
        'group.id': 'flink_alert_group',
        'auto.offset.reset': 'latest'
    }

    source = FlinkKafkaConsumer(
        topics='hpc-raw-metrics',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(source)

    # 3. DataStream Pipeline: Map -> Filter -> KeyBy -> Window -> Reduce -> Map (avg + threshold)
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

    # Tính trung bình + áp ngưỡng MỘT LẦN, dùng chung cho cả 2 nhánh sink phía dưới
    avg_stream = reduced_stream.map(
        calculate_avg,
        output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.STRING()])
    ).filter(lambda x: x is not None)

    # 4a. Nhánh Kafka Sink (giữ nguyên hành vi cũ — test_consumer.py vẫn nghe được)
    kafka_alert_stream = avg_stream.map(to_alert_json, output_type=Types.STRING())

    kafka_sink = FlinkKafkaProducer(
        topic='hpc-realtime-alert',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': kafka_broker}
    )
    kafka_alert_stream.add_sink(kafka_sink)

    # 4b. Nhánh ClickHouse JDBC Sink (mới)
    # LƯU Ý: bảng hpc_monitor.realtime_alerts phải được tạo trước bằng DDL riêng,
    # Flink JDBC sink không tự tạo bảng.
    clickhouse_url = os.getenv('CLICKHOUSE_URL', 'jdbc:clickhouse://clickhouse:8123/hpc_monitor')
    clickhouse_user = os.getenv('CLICKHOUSE_USER', 'admin')
    clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', 'adminpassword')

    clickhouse_tuple_stream = avg_stream.map(
        to_alert_tuple,
        output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.STRING()])
    )

    jdbc_sink = JdbcSink.sink(
        "INSERT INTO hpc_monitor.realtime_alerts (machine_id, alert_type, value, severity, alert_timestamp) VALUES (?, ?, ?, ?, ?)",
        type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.STRING()]),
        jdbc_connection_options=JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url(clickhouse_url)
            .with_driver_name("com.clickhouse.jdbc.ClickHouseDriver")
            .with_user_name(clickhouse_user)
            .with_password(clickhouse_password)
            .build(),
        jdbc_execution_options=JdbcExecutionOptions.builder()
            .with_batch_size(1)          # alert cần ghi ngay, không buffer batch lớn
            .with_batch_interval_ms(200)
            .with_max_retries(3)
            .build()
    )
    clickhouse_tuple_stream.add_sink(jdbc_sink)

    # 5. Kích hoạt thực thi
    env.execute("HPC Realtime Alerting Job - 10 Sec Average CPU")

if __name__ == '__main__':
    main()