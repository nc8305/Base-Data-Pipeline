import urllib.request
import os

def main():
    jar_url = "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar"
    # Lấy đường dẫn tuyệt đối để tránh tải nhầm ra ngoài Desktop
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dest_dir = os.path.join(current_dir, "..", "stream_processing", "jars")
    jar_name = "flink-sql-connector-kafka.jar"
    
    # Tạo thư mục jars
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, jar_name)
    
    if os.path.exists(dest_path):
        print(f"File {jar_name} đã tồn tại tại {dest_dir}.")
    else:
        print(f"Đang tải {jar_name}...")
        urllib.request.urlretrieve(jar_url, dest_path)
        print(f"Tải xong! Lưu tại: {dest_path}")

if __name__ == '__main__':
    main()
