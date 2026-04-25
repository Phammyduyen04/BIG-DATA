"""
T1: Upload local DATA_SPLIT/<dataset>/ to MinIO s3://<bucket>/DATA_SPLIT/<dataset>/
Uses parallel boto3 uploads (ThreadPool 8, 64MB multipart, 8 concurrency).
Returns elapsed seconds (float).
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config


def _upload_file(s3_client, local_path, bucket, s3_key, transfer_cfg):
    s3_client.upload_file(local_path, bucket, s3_key, Config=transfer_cfg)
    return s3_key


def run(cfg, dataset, data_root, workers=8):
    """
    Upload all CSV files from data_root/<dataset>/ → s3://<bucket>/DATA_SPLIT/<dataset>/
    Returns T1 seconds measured with time.monotonic().
    """
    local_dir = os.path.join(data_root, dataset)
    if not os.path.isdir(local_dir):
        raise FileNotFoundError(f"Data dir not found: {local_dir}")

    files = [f for f in os.listdir(local_dir) if f.endswith(".csv")]
    if not files:
        raise ValueError(f"No CSV files found in {local_dir}")

    bucket = cfg["minio_bucket"]
    prefix = f"DATA_SPLIT/{dataset}"

    transfer_cfg = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,
        multipart_chunksize=64 * 1024 * 1024,
        max_concurrency=8,
    )

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{cfg['minio_endpoint']}",
        aws_access_key_id=cfg["minio_access_key"],
        aws_secret_access_key=cfg["minio_secret_key"],
        config=Config(signature_version="s3v4", max_pool_connections=workers * 2),
    )

    total_bytes = sum(os.path.getsize(os.path.join(local_dir, f)) for f in files)
    print(f"[upload] Uploading {len(files)} files ({total_bytes/1e9:.2f} GB) "
          f"→ s3://{bucket}/{prefix}/ ...")

    t_start = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _upload_file, s3,
                os.path.join(local_dir, fname),
                bucket,
                f"{prefix}/{fname}",
                transfer_cfg,
            ): fname
            for fname in files
        }
        done = 0
        for fut in as_completed(futures):
            fut.result()
            done += 1
            if done % 10 == 0 or done == len(files):
                print(f"[upload]   {done}/{len(files)} files uploaded ...")

    # HEAD sentinel: verify last file exists before stopping the clock
    last_key = f"{prefix}/{files[-1]}"
    s3.head_object(Bucket=bucket, Key=last_key)

    t1 = time.monotonic() - t_start
    mb_s = (total_bytes / 1e6) / t1
    print(f"[upload] T1 = {t1:.2f}s  ({mb_s:.1f} MB/s)")
    return t1, len(files), total_bytes
