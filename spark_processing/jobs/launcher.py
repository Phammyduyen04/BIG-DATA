import os
import sys
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


def preflight_audit_csv(spark, bucket):
    """
    Kiểm tra 3 CSV glob patterns trong MinIO trước khi chạy ETL.
    Chính sách: tất cả 3 đều missing → Abort. Còn lại → WARNING và tiếp tục.
    """
    import config
    sc = spark.sparkContext
    jvm = sc._gateway.jvm
    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem

    prefix = config.PREFIX_CSV_RAW.strip("/")
    globs = {
        "klines": f"s3a://{bucket}/{prefix}/{config.CSV_FILENAME_KLINES}",
        "ticker": f"s3a://{bucket}/{prefix}/{config.CSV_FILENAME_TICKER}",
        "trades": f"s3a://{bucket}/{prefix}/{config.CSV_GLOB_TRADES}",
    }

    print(f"\n[launcher] --- GIAI DOAN 2: PRE-FLIGHT AUDIT (CSV Glob Check) ---")
    print(f"[launcher] Kiem tra {len(globs)} CSV glob patterns trong MinIO ...")

    root_uri = jvm.java.net.URI(f"s3a://{bucket}")
    fs = FileSystem.get(root_uri, sc._jsc.hadoopConfiguration())

    missing_count = 0
    for name, glob_str in globs.items():
        try:
            matched = fs.globStatus(Path(glob_str))
            n = len(matched) if matched else 0
            if n == 0:
                print(f"[launcher] WARNING: {name.upper()} — khong co file match: {glob_str}")
                missing_count += 1
            else:
                print(f"[launcher] OK: {name.upper()} — {n} file(s) match.")
        except Exception as e:
            print(f"[launcher] WARNING: {name.upper()} check failed: {e}")
            missing_count += 1

    if missing_count == len(globs):
        print(f"[launcher] CRITICAL: TAT CA CSV glob deu missing. Aborting.")
        spark.stop()
        sys.exit(1)

    print(f"[launcher] PRE-FLIGHT AUDIT PASSED ({len(globs)-missing_count}/{len(globs)} globs OK). Proceeding...")
    return True


def main():
    import config
    app_version = getattr(config, 'VERSION', 'v1.5.0-csv-bench')
    print("=" * 60)
    print(f" BINANCE SPARK LAUNCHER - {app_version}")
    print("=" * 60)

    # 1. Khởi tạo Spark Session
    spark = get_spark_session()

    # --- GIAI ĐOẠN 2: PRE-FLIGHT AUDIT (CSV) ---
    preflight_audit_csv(spark, config.MINIO_BUCKET)

    # --- GIAI ĐOẠN 3: ORCHESTRATION ---
    print("\n[launcher] Bat dau dieu phoi toan bo Pipeline (run_all.py)...")
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
