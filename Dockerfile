# ---- Stage 1: Builder ----
# Sử dụng Python 3.9 làm base image siêu nhẹ (slim)
FROM python:3.9-slim AS builder

WORKDIR /app

# Tạo virtual environment
RUN python -m venv /opt/venv
# Đảm bảo các lệnh pip và python sẽ sử dụng virtual environment này
ENV PATH="/opt/venv/bin:$PATH"

# Sao chép file requirements.txt (chứa danh sách thư viện)
COPY requirements.txt .

# Cài đặt các thư viện cần thiết
RUN pip install --no-cache-dir -r requirements.txt

# ---- Stage 2: Final image ----
FROM python:3.9-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Sao chép thư viện đã cài đặt từ stage builder (chỉ lấy những gì cần thiết)
COPY --from=builder /opt/venv /opt/venv

# Thiết lập môi trường để sử dụng virtual environment
ENV PATH="/opt/venv/bin:$PATH"

# Sao chép toàn bộ mã nguồn vào thư mục /app trong container
COPY . .

# Lệnh mặc định chạy script gửi dữ liệu IoT
CMD ["python", "ingestion/producer.py"]