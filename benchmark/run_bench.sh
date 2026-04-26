#!/usr/bin/env bash
# MinIO Upload Benchmark — entry point (bash)
# Chạy: bash benchmark/run_bench.sh
# Hoặc: bash benchmark/run_bench.sh --datasets 4GB --runs 3

set -euo pipefail

# -- Cấu hình MinIO ------------------------------------------------------------
export MINIO_ENDPOINT="100.74.195.110:30900"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY='Bigdata@MinIO2025'
export MINIO_BUCKET="binance"

DATA_ROOT="d:/HK2_2025-2026/DLL/Project/DATA_SPLIT"
WORKERS=4
RUNS=5
RUN_RETRIES=2     # số lần retry mỗi run nếu bị lỗi
RETRY_DELAY=90    # giây chờ giữa các lần retry (đủ cho MinIO reconnect)

# -- Đường dẫn ----------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# -- Kiểm tra kết nối MinIO --------------------------------------------------
echo ""
echo "[1/2] Kiem tra ket noi MinIO ${MINIO_ENDPOINT} ..."
python -c "
import boto3, sys
from botocore.config import Config
try:
    s3 = boto3.client('s3',
        endpoint_url='http://${MINIO_ENDPOINT}',
        aws_access_key_id='${MINIO_ACCESS_KEY}',
        aws_secret_access_key='${MINIO_SECRET_KEY}',
        config=Config(signature_version='s3v4', connect_timeout=5, read_timeout=10))
    buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    print('  OK - Buckets:', buckets)
except Exception as e:
    print('  FAILED:', e)
    sys.exit(1)
"

echo ""
echo "[2/2] Bat dau benchmark: ${RUNS} runs x dataset(s), workers=${WORKERS}"
echo ""
echo "  Tip: dung --datasets 4GB de chi chay 1 dataset"
echo "  Tip: dung --clean neu muon xoa MinIO truoc (se hoi xac nhan 'yes')"
echo ""

# -- Chạy benchmark -----------------------------------------------------------
# Không có --clean mặc định → an toàn
# Truyền thêm args từ command line (vd: --datasets 4GB --runs 3)
python -u benchmark/bench_upload.py \
    --data-root "$DATA_ROOT" \
    --runs "$RUNS" \
    --workers "$WORKERS" \
    --run-retries "$RUN_RETRIES" \
    --retry-delay "$RETRY_DELAY" \
    "$@"
