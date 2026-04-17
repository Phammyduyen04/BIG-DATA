import os
import sys
import boto3
from pyspark.sql import functions as F

# Đảm bảo Spark có thể import config.py ở root
sys.path.insert(0, os.getcwd())

import config
from spark.jobs.etl_utils import get_spark_session

def get_first_file(prefix):
    """Sử dụng boto3 để lấy tệp đầu tiên trong prefix nhằm test Spark nhanh nhất"""
    s3 = boto3.client(
        's3',
        endpoint_url=f"http://{config.MINIO_ENDPOINT}",
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        use_ssl=False
    )
    response = s3.list_objects_v2(Bucket=config.MINIO_BUCKET, Prefix=prefix, MaxKeys=1)
    if 'Contents' in response:
        return f"s3a://{config.MINIO_BUCKET}/{response['Contents'][0]['Key']}"
    return None

def test_prefix(spark, prefix, name):
    print(f"\n>>> [DIRECT TEST] Finding first file in {name} (Prefix: {prefix})...")
    specific_path = get_first_file(prefix)
    
    if not specific_path:
        print(f"[{name}] Warning: No files found in this prefix.")
        return

    print(f"[{name}] Found file: {specific_path}. Reading now...")
    try:
        # Đọc chính xác 1 file để lấy Schema và Sample
        df = spark.read.json(specific_path)
        
        print(f"[{name}] SUCCESS! Schema:")
        df.printSchema()
        
        count = df.count()
        print(f"[{name}] Total records in this file: {count}")
        
        print(f"[{name}] Sample Data:")
        df.show(3, truncate=False)
            
    except Exception as e:
        print(f"[{name}] Error reading with Spark: {str(e)}")

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("="*60)
    print(f"MinIO K8s DIRECT ACCESS TEST (Phase 1)")
    print(f"Endpoint: {config.MINIO_ENDPOINT}")
    print("="*60)

    test_cases = [
        ("Klines", config.PREFIX_KLINES),
        ("Depth ", config.PREFIX_DEPTH),
        ("Ticker", config.PREFIX_TICKER)
    ]

    for name, prefix in test_cases:
        test_prefix(spark, prefix, name)

    print("\n" + "="*60)
    print("MinIO K8s Direct Integration Test Finished.")
    print("="*60)
    spark.stop()

if __name__ == "__main__":
    main()
