import boto3
import os
from dotenv import load_dotenv

# Load config từ .env.processing
env_path = os.path.join(os.getcwd(), "spark", ".env.processing")
load_dotenv(dotenv_path=env_path)

def test_minio_boto3():
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    bucket_name = os.getenv("MINIO_BUCKET", "binance")
    
    print(f"Connecting to MinIO at: {endpoint}...")
    
    s3 = boto3.client(
        's3',
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        use_ssl=False
    )
    
    try:
        # Kiểm tra bucket
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Success: Connected to bucket '{bucket_name}'!")
        
        # Liệt kê thử 3 prefix
        prefixes = [
            os.getenv("MINIO_PREFIX_KLINES", "raw/klines/"),
            os.getenv("MINIO_PREFIX_DEPTH", "raw/depth/"),
            os.getenv("MINIO_PREFIX_TICKER", "raw/ticker/")
        ]
        
        for p in prefixes:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=p, MaxKeys=5)
            if 'Contents' in response:
                print(f"  - Prefix '{p}': Found {len(response['Contents'])} files (Test OK)")
            else:
                print(f"  - Prefix '{p}': ⚠ Empty (Check ingest pipeline)")
                
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    test_minio_boto3()
