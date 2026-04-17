# Spark Processing - MinIO Kubernetes (Phase 1)

Thư mục này chứa các scripts Spark để xử lý dữ liệu từ MinIO được triển khai trên cụm Kubernetes (K3s).

## 🌍 Kiến trúc kết nối (Giai đoạn 1)
- **Hạ tầng**: Spark (Local/Docker) -> S3A Protocol -> Tailscale -> K8s MinIO NodePort (`100.74.195.110:30900`).
- **Phạm vi**: Chỉ tập trung vào việc xác minh khả năng kết nối và schema dữ liệu, chưa sửa đổi luồng ETL chính.

## ⚙️ Cấu hình môi trường
Mọi thông số kết nối được quản lý tập trung qua file **`spark/.env.processing`**. Hệ thống sẽ tự động nạp cấu hình này thông qua biến môi trường `ENV_FILE`.

## 🚀 Hướng dẫn chạy kiểm thử kết nối

Để tránh lỗi import, bạn nên chạy lệnh `spark-submit` từ **thư mục gốc của project** (root) và truyền các file phụ thuộc qua flag `--py-files`.

### Bước 1: Thiết lập môi trường
```bash
# Trên Windows (PowerShell)
$env:ENV_FILE="spark/.env.processing"

# Trên Linux/macOS
export ENV_FILE=spark/.env.processing
```

### Bước 2: Chạy Script Test
```bash
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
             --py-files config.py,spark/jobs/etl_utils.py \
             spark/jobs/test_read_minio.py
```

## ✅ Tiêu chí thành công
1. **Schema**: Spark in ra đúng các trường dữ liệu (ví dụ: `symbol`, `close_price`, `event_time`).
2. **Count**: Số lượng records lớn hơn 0.
3. **Connectivity**: Log không xuất hiện lỗi `AmazonS3Exception` (403, 404) hoặc `ConnectException`.
