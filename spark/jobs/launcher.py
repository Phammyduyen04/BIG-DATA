"""
Launcher: Entrypoint chính cho Spark Job trên Kubernetes.
Nhiệm vụ:
  1. Khởi tạo Spark Session với S3A.
  2. Kiểm tra sự tồn tại của dữ liệu (ví dụ: raw/trades).
  3. Thiết lập biến môi trường để kích hoạt "Reduced Mode" nếu thiếu dữ liệu.
  4. Gọi orchestration logic gốc (run_all.main).
"""
import os
import sys

# Thêm thư mục hiện tại vào path để import các module ETL
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from etl_utils import get_spark_session, emulate_listdir

def main():
    print("=" * 60)
    print("K8S SPARK LAUNCHER: INITIALIZING...")
    print("=" * 60)

    # 1. Khởi tạo Spark Session một lần duy nhất tại Entrypoint
    spark = get_spark_session(app_name="CryptoDW-K8s-Production")
    
    # 2. Kiểm tra sự tồn tại của dữ liệu Trades (Reduced Mode Guard)
    # Lấy bucket từ config hoặc env (etl_utils dùng config.py)
    import config
    trades_path = f"s3a://{config.MINIO_BUCKET}/raw/trades/"
    
    print(f"[launcher] Checking input availability: {trades_path}")
    try:
        # Nếu emulate_listdir không lỗi và trả về list, nghĩa là prefix tồn tại
        items = emulate_listdir(spark, trades_path)
        if not items:
            print("[launcher] WARNING: raw/trades is empty. Enabling REDUCED_MODE.")
            os.environ["SKIP_ETL_TRADES"] = "TRUE"
        else:
            print(f"[launcher] SUCCESS: Found {len(items)} items in raw/trades. Full Mode enabled.")
    except Exception as e:
        print(f"[launcher] WARNING: raw/trades prefix not found or error. Enabling REDUCED_MODE.")
        print(f"[launcher] Error detail: {str(e)}")
        os.environ["SKIP_ETL_TRADES"] = "TRUE"

    # 3. Chuyển hướng tới Orchestration logic của team Processing
    print("\n[launcher] Handing over to run_all.py orchestration...")
    import run_all
    
    try:
        # Lưu ý: run_all.py gốc gọi SparkSession.builder...getOrCreate()
        # Vì chúng ta đã tạo session ở đây, getOrCreate() sẽ trả về đúng session này.
        run_all.main()
    except Exception as e:
        print(f"\n[launcher] CRITICAL ERROR during execution: {str(e)}")
        sys.exit(1)
    finally:
        print("\n[launcher] SHUTTING DOWN SPARK SESSION.")
        spark.stop()

if __name__ == "__main__":
    main()
