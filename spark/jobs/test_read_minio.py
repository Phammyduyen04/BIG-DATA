import os
import sys
from dotenv import load_dotenv
from pyspark.sql import functions as F

# Load config từ file .env.processing (nằm cùng thư mục hoặc trong thư mục spark/)
env_path = os.path.join(os.path.dirname(__file__), "..", ".env.processing")
load_dotenv(dotenv_path=env_path)

# Import helper từ etl_utils (đảm bảo file etl_utils.py có trong path)
sys.path.insert(0, os.path.dirname(__file__))
from etl_utils import get_spark_session

def test_prefix(spark, bucket, prefix, name):
    path = f"s3a://{bucket}/{prefix}*"
    print(f"\n>>> Testing {name} at: {path}")
    try:
        # Đọc dữ liệu JSON Lines
        df = spark.read.json(path)
        
        print(f"[{name}] Schema:")
        df.printSchema()
        
        count = df.count()
        print(f"[{name}] Total records: {count}")
        
        if count > 0:
            print(f"[{name}] Sample data (Top 5):")
            df.show(5, truncate=False)
        else:
            print(f"[{name}] Warning: No records found check your prefix/globbing.")
            
    except Exception as e:
        print(f"[{name}] Error: {str(e)}")

def main():
    bucket = os.environ.get("MINIO_BUCKET", "binance")
    app_name = os.environ.get("SPARK_APP_NAME", "test-read-minio")
    
    # Khởi tạo Spark Session với S3A config
    spark = get_spark_session(app_name)
    spark.sparkContext.setLogLevel("ERROR")
    
    print("="*60)
    print(f"Starting MinIO K8s Connection Test")
    print(f"Endpoint: {os.environ.get('MINIO_ENDPOINT')}")
    print(f"Bucket  : {bucket}")
    print("="*60)

    # Test 3 prefixes theo yêu cầu
    prefixes = {
        "Klines": os.environ.get("MINIO_PREFIX_KLINES", "raw/klines/"),
        "Depth ": os.environ.get("MINIO_PREFIX_DEPTH",  "raw/depth/"),
        "Ticker": os.environ.get("MINIO_PREFIX_TICKER", "raw/ticker/")
    }

    for name, prefix in prefixes.items():
        test_prefix(spark, bucket, prefix, name)

    print("\n" + "="*60)
    print("Test connection to MinIO K8s finished.")
    print("="*60)
    spark.stop()

if __name__ == "__main__":
    main()
