import json
import boto3
import logging
from botocore.client import Config
import config

logger = logging.getLogger(__name__)

def get_s3_client():
    """Tạo S3 client kết nối tới MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{config.MINIO_ENDPOINT}",
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",  # MinIO mặc định
    )

def save_active_symbols(symbols: list[str]):
    """Lưu danh sách symbols lên MinIO làm Source-of-Truth theo chu kỳ 24h."""
    client = get_s3_client()
    data = {
        "symbols": symbols,
        "updated_at": config.now_iso(),
        "version": "v1.4.0"
    }
    try:
        client.put_object(
            Bucket=config.MINIO_BUCKET,
            Key="metadata/active_symbols.json",
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        logger.info(f"[S3] Đã lưu {len(symbols)} symbols vào metadata/active_symbols.json")
    except Exception as e:
        logger.error(f"[S3] Lỗi khi lưu active_symbols.json: {e}")

def load_active_symbols() -> list[str] or None:
    """Tải danh sách symbols từ MinIO nếu còn hiệu lực (24h)."""
    client = get_s3_client()
    try:
        response = client.get_object(
            Bucket=config.MINIO_BUCKET,
            Key="metadata/active_symbols.json"
        )
        data = json.loads(response["Body"].read().decode("utf-8"))
        
        # [TODO] Kiểm tra thời gian hết hạn 24h nếu cần
        # Ở bản v1.4.0, chỉ cần file tồn tại là chúng ta ưu tiên dùng để chống drift
        return data.get("symbols", [])
    except client.exceptions.NoSuchKey:
        logger.warning("[S3] Không tìm thấy metadata/active_symbols.json")
        return None
    except Exception as e:
        logger.error(f"[S3] Lỗi khi tải active_symbols.json: {e}")
        return None
