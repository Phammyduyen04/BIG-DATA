# Binance Real-Time Streaming Pipeline (Redpanda + MinIO)

Hệ thống thu thập dữ liệu Big Data từ Binance cho **100 đồng tiền điện tử phổ biến nhất** (USDT pairs). Pipeline được thiết kế theo chuẩn doanh nghiệp sử dụng **Redpanda** làm Message Broker và **MinIO** làm Data Lake.

---

## 🏗️ Kiến trúc Pipeline

```text
Binance WebSocket/REST ──► Python Producer ──► Redpanda (Kafka API) ──► Python Consumer ──► MinIO (S3 Storage)
```

1.  **Producer (`main.py`)**: Tự động tìm Top Symbols, fetch dữ liệu lịch sử và stream dữ liệu thực tế.
2.  **Broker (Redpanda)**: Lưu trữ dữ liệu thô trong các Topics theo thời gian thực.
3.  **Consumer (`consumer_to_minio.py`)**: Gom nhóm dữ liệu (Batching) và đẩy lên lưu trữ đám mây (MinIO).

---

## 🛠️ Hạ tầng & Tài khoản

Hệ thống chạy trên Docker Compose. Sau khi khởi động (`docker compose up -d`), bạn có thể truy cập các địa chỉ sau:

| Dịch vụ | Địa chỉ | Tài khoản / Ghi chú |
| :--- | :--- | :--- |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **MinIO API** | [http://localhost:9000](http://localhost:9000) | Endpoint cho Spark/Boto3 |
| **Redpanda Console** | [http://localhost:8080](http://localhost:8080) | Xem trực tiếp message trong topics |
| **Redpanda Broker** | `localhost:19092` | Địa chỉ nội bộ cho code kết nối |

---

## 📂 Lưu trữ tại MinIO (Data Lake)

Dữ liệu được lưu trong bucket `binance/` theo cấu trúc Schema-on-Read, phân cấp như sau:

*   **Klines**: `raw/klines/interval=1m/date=YYYY-MM-DD/symbol=.../hour=HH/part-XXXXX.json`
*   **Order Book**: `raw/depth/date=YYYY-MM-DD/symbol=.../hour=HH/part-XXXXX.json`
*   **Ticker 24h**: `raw/ticker/date=YYYY-MM-DD/symbol=.../hour=HH/part-XXXXX.json`

---

## 🚀 Hướng dẫn khởi chạy

### 1. Chuẩn bị môi trường
Cài đặt thư viện:
```bash
pip install -r requirements.txt
```

Cấu hình API Key trong file `.env`:
```env
BINANCE_API_KEY=your_key
BINANCE_SECRET_KEY=your_secret
TOP_N_SYMBOLS=100
REDPANDA_BROKERS=localhost:19092
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=binance
```

### 2. Khởi động Hạ tầng
```bash
docker compose up -d
```

### 3. Chạy Pipeline
Mở 2 terminal riêng biệt:
*   **Terminal 1 (Producer)**: `python main.py`
*   **Terminal 2 (Consumer)**: `python consumer_to_minio.py`

---

## 🛡️ Các tệp tin chính
*   `main.py`: Entry point chính của Producer.
*   `redpanda_storage.py`: Xử lý việc đẩy dữ liệu vào các Kafka Topics.
*   `consumer_to_minio.py`: Consumer đọc từ Redpanda và ghi vào MinIO.
*   `rest_collector.py` / `ws_collector.py`: Các module lấy dữ liệu từ Binance.
*   `config.py`: Cấu hình tập trung cho toàn bộ hệ thống.

---

## 📊 Bước tiếp theo
Hệ thống đã sẵn sàng để tích hợp với **Apache Spark** để thực hiện các bài toán phân tích Big Data (như tính toán RSI, EMA,...). Dữ liệu trong MinIO có thể được đọc trực tiếp bằng PySpark thông qua giao thức S3A.
