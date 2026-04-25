"""
Cleanup between benchmark runs.
- Deletes MinIO prefix DATA_SPLIT/<dataset>/ (all CSV files)
- TRUNCATEs all 5 output tables in Postgres with RESTART IDENTITY CASCADE
"""
import boto3
from botocore.config import Config


_TABLES = [
    "dim_symbols",
    "fact_klines",
    "fact_ticker_24h_snapshots",
    "mart_trade_metrics",
    "mart_top_coins",
]


def delete_minio_prefix(endpoint, access_key, secret_key, bucket, prefix):
    """Delete all objects under bucket/prefix (paginated)."""
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    for key in keys:
        s3.delete_object(Bucket=bucket, Key=key)
    print(f"[cleanup]   Deleted {len(keys)} objects from s3://{bucket}/{prefix}")
    return len(keys)


def truncate_postgres(host, port, dbname, user, password):
    import psycopg2
    tables_sql = ", ".join(_TABLES)
    sql = f"TRUNCATE TABLE {tables_sql} RESTART IDENTITY CASCADE"
    conn = psycopg2.connect(host=host, port=port, dbname=dbname,
                            user=user, password=password)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.close()
    print(f"[cleanup]   Truncated {len(_TABLES)} tables in Postgres")


def run(cfg, dataset):
    prefix = f"DATA_SPLIT/{dataset}/"
    print(f"[cleanup] Deleting MinIO prefix: s3://{cfg['minio_bucket']}/{prefix}")
    delete_minio_prefix(
        cfg["minio_endpoint"], cfg["minio_access_key"], cfg["minio_secret_key"],
        cfg["minio_bucket"], prefix
    )
    print(f"[cleanup] Truncating Postgres tables ...")
    truncate_postgres(
        cfg["pg_host"], cfg["pg_port"], cfg["pg_dbname"],
        cfg["pg_user"], cfg["pg_password"]
    )
    print(f"[cleanup] Done.")
