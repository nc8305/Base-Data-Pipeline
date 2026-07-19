# Sử dụng Python 3.9 làm base image siêu nhẹ (slim)
FROM python:3.9-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Sao chép file requirements.txt (chứa danh sách thư viện)
COPY requirements.txt .

# Cài đặt các thư viện cần thiết
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ mã nguồn vào thư mục /app trong container
COPY . .

# Lệnh mặc định chạy script gửi dữ liệu IoT
CMD ["python", "ingestion/producer.py"]