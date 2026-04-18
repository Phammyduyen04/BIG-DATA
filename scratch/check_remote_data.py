
import os
import sys
import argparse
import boto3
from botocore.config import Config
from dotenv import load_dotenv

# Load config from root .env
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv(dotenv_path)

def get_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT", "100.74.195.110:30900")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    
    # Prefix with http/https
    if not endpoint.startswith("http"):
        prefix = "https://" if secure else "http://"
        endpoint = f"{prefix}{endpoint}"

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4")
    )

def count_objects(client, bucket, prefix):
    count = 0
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            count += page.get("KeyCount", 0)
    except Exception as e:
        print(f"Error listing {prefix}: {e}")
    return count

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", action="store_true", help="Take initial snapshot")
    parser.add_argument("--verify", action="store_true", help="Verify after test")
    args = parser.parse_args()

    bucket = os.getenv("MINIO_BUCKET", "binance")
    prefixes = [
        "raw/klines/",
        "raw/ticker/",
        "raw/trades/"
    ]
    
    client = get_s3_client()
    snapshot_file = "scratch/minio_snapshot.json"
    import json

    if args.snapshot:
        print(f"--- Taking Snapshot of Cluster MinIO ({bucket}) ---")
        counts = {}
        for p in prefixes:
            c = count_objects(client, bucket, p)
            counts[p] = c
            print(f"  {p}: {c} objects")
        
        with open(snapshot_file, "w") as f:
            json.dump(counts, f)
        print(f"Snapshot saved to {snapshot_file}")

    elif args.verify:
        print(f"--- Verifying Cluster MinIO ({bucket}) ---")
        if not os.path.exists(snapshot_file):
            print(f"ERROR: Snapshot file {snapshot_file} not found. Run with --snapshot first.")
            return

        with open(snapshot_file, "r") as f:
            old_counts = json.load(f)

        all_passed = True
        for p in prefixes:
            new_count = count_objects(client, bucket, p)
            old_count = old_counts.get(p, 0)
            diff = new_count - old_count
            
            status = "PASS" if diff > 0 else "FAIL"
            print(f"  {p}: {old_count} -> {new_count} (Diff: +{diff}) [{status}]")
            if diff <= 0:
                all_passed = False

        if all_passed:
            print("\n✅ GLOBAL VERIFICATION PASSED: New data received for all 3 branches.")
        else:
            print("\n❌ GLOBAL VERIFICATION FAILED: Some branches missing new data.")

if __name__ == "__main__":
    main()
