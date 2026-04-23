import os
import sys
import json
import logging

# Thêm thư mục hiện tại vào path để import các module ETL
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Tee stdout/stderr ra file — đọc được qua `kubectl cp` kể cả khi Tailscale đứt
_LOG_FILE = "/tmp/etl_run.log"
class _Tee:
    def __init__(self, *streams):
        self._streams = streams
    def write(self, data):
        for s in self._streams:
            s.write(data)
    def flush(self):
        for s in self._streams:
            s.flush()
    def fileno(self):
        return self._streams[0].fileno()

_log_fh = open(_LOG_FILE, "w", buffering=1, encoding="utf-8")
sys.stdout = _Tee(sys.__stdout__, _log_fh)
sys.stderr = _Tee(sys.__stderr__, _log_fh)

from etl_utils import get_spark_session

def preflight_audit(spark, bucket, whitelist_symbols):
    """
    Xác thực nhanh (v1.4.3): Kiểm tra PREFIX-LEVEL thay vì per-symbol.
    Lý do: Per-symbol globStatus = 300 S3 calls ~10 phút qua Tailscale.
    Chính sách: Nếu cả 3 prefix đều rỗng -> Abort. Còn lại -> WARNING và tiếp tục.
    """
    sc = spark.sparkContext
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    conf = sc._jsc.hadoopConfiguration()
    fs = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._gateway.jvm.java.net.URI(f"s3a://{bucket}"), conf
    )

    # Chỉ 3 prefix-level checks thay vì 300 per-symbol calls
    topics = {
        "klines": f"s3a://{bucket}/silver/klines/",
        "trades": f"s3a://{bucket}/silver/trades/",
        "ticker": f"s3a://{bucket}/silver/ticker/",
    }

    print(f"\n[launcher] --- GIAI ĐOẠN 2: PRE-FLIGHT AUDIT (Prefix-Level, Fast) ---")
    print(f"[launcher] Checking {len(topics)} prefixes (3 S3 calls, not {len(whitelist_symbols)*len(topics)})...")

    empty_count = 0
    for topic, prefix_path in topics.items():
        try:
            path_obj = Path(prefix_path)
            exists = fs.exists(path_obj)
            if not exists:
                print(f"[launcher] WARNING: {topic.upper()} prefix not found: {prefix_path}")
                empty_count += 1
            else:
                # Kiểm tra có ít nhất 1 file con không
                iter_status = fs.listStatus(path_obj)
                has_data = iter_status is not None and len(iter_status) > 0
                if has_data:
                    print(f"[launcher] OK: {topic.upper()} prefix exists with data.")
                else:
                    print(f"[launcher] WARNING: {topic.upper()} prefix exists but is EMPTY.")
                    empty_count += 1
        except Exception as e:
            print(f"[launcher] WARNING: {topic.upper()} check failed: {str(e)}")
            empty_count += 1

    if empty_count == len(topics):
        print(f"[launcher] CRITICAL: ALL prefixes are empty or missing. Aborting.")
        spark.stop()
        sys.exit(1)

    print(f"[launcher] PRE-FLIGHT AUDIT PASSED ({len(topics)-empty_count}/{len(topics)} prefixes OK). Proceeding...")
    return True

def main():
    import config
    # --- BOOTSTRAP (Guard against out-of-sync config) ---
    # NOTE: getattr is a safety fallback. Proper config sync is still the primary goal.
    app_version = getattr(config, 'VERSION', 'v1.4.2')
    print("=" * 60)
    print(f" BINANCE SPARK LAUNCHER - {app_version}")
    print("=" * 60)

    # 1. Khởi tạo Spark Session
    spark = get_spark_session()
    
    # --- GIAI ĐOẠN 1: SYMBOL COORDINATION (WHITE-LIST) ---
    whitelist_path = f"s3a://{config.MINIO_BUCKET}/metadata/active_symbols.json"
    print(f"\n[launcher] Đang lấy Whitelist từ: {whitelist_path}")
    
    try:
        # Sử dụng option multiLine=true vì s3_utils lưu file có thụt lề (indent)
        whitelist_df = spark.read.json(whitelist_path, multiLine=True)
        
        if whitelist_df.rdd.isEmpty():
            raise ValueError(f"File {whitelist_path} is empty or invalid.")
            
        whitelist_data = whitelist_df.collect()[0]
        symbols = whitelist_data["symbols"]
        print(f"[launcher] SUCCESS: Đã load {len(symbols)} symbols Whitelist.")
    except Exception as e:
        print(f"[launcher] CRITICAL: Không thể xử lý active_symbols.json.")
        print(f"[launcher] ERROR DETAIL: {str(e)}")
        spark.stop()
        sys.exit(1)

    # --- GIAI ĐOẠN 2: PRE-FLIGHT AUDIT ---
    preflight_audit(spark, config.MINIO_BUCKET, symbols)

    # --- GIAI ĐOẠN 3: ORCHESTRATION ---
    print("\n[launcher] Bắt đầu điều phối toàn bộ Pipeline (run_all.py)...")
    import run_all
    
    try:
        run_all.main()
    except Exception as e:
        print(f"\n[launcher] CRITICAL ERROR: {str(e)}")
        sys.exit(1)
    finally:
        print("\n[launcher] SHUTTING DOWN.")
        spark.stop()

if __name__ == "__main__":
    main()
