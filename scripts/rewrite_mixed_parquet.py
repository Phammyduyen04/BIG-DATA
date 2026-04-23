"""
Rewrite silver parquet files flagged DRIFT by scan_silver_schemas.py:
cast numeric columns to pa.string() and overwrite the same S3 key.

Usage:
  python scripts/rewrite_mixed_parquet.py --input /tmp/silver_scan.csv --dry-run
  python scripts/rewrite_mixed_parquet.py --input /tmp/silver_scan.csv
  python scripts/rewrite_mixed_parquet.py --input /tmp/silver_scan.csv --topic klines --limit 5

Safety: pkill consumer_to_minio.py trước khi chạy apply mode để tránh race
với writer đang ghi file mới cùng key.
"""

import argparse
import csv
import io
import os
import sys
import time
from collections import Counter

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig
from dotenv import load_dotenv

# Make top-level modules importable when running from anywhere.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from silver_schema import NUMERIC_TO_STRING_FIELDS, TOPIC_LABEL_TO_KAFKA


load_dotenv()

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "binance")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"

STRING_TYPE = pa.string()


def create_s3():
    protocol = "https" if MINIO_SECURE else "http"
    return boto3.client(
        "s3",
        endpoint_url=f"{protocol}://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def rewrite_one(s3, key, kafka_topic, dry_run):
    """Read parquet at key, cast numeric fields to string, overwrite.

    Returns (status, detail) where status is one of:
      REWRITTEN | DRY_RUN | NOOP | READ_ERROR | CAST_ERROR | WRITE_ERROR
    """
    t0 = time.time()
    try:
        body = s3.get_object(Bucket=MINIO_BUCKET, Key=key)["Body"].read()
        table = pq.read_table(io.BytesIO(body))
    except Exception as e:
        return ("READ_ERROR", str(e)[:120])

    numeric_fields = NUMERIC_TO_STRING_FIELDS.get(kafka_topic, frozenset())
    changed_cols = []

    for name in numeric_fields:
        if name not in table.column_names:
            continue
        col = table.column(name)
        if col.type == STRING_TYPE:
            continue
        try:
            new_col = pc.cast(col, STRING_TYPE)
        except Exception as e:
            return ("CAST_ERROR", f"col={name}: {str(e)[:100]}")
        idx = table.column_names.index(name)
        table = table.set_column(idx, name, new_col)
        changed_cols.append(name)

    if not changed_cols:
        return ("NOOP", "all numeric cols already string")

    if dry_run:
        return ("DRY_RUN", f"would cast {changed_cols} rows={table.num_rows}")

    try:
        buf_out = io.BytesIO()
        pq.write_table(table, buf_out, compression="snappy")
        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=buf_out.getvalue(),
            ContentType="application/x-parquet",
        )
    except Exception as e:
        return ("WRITE_ERROR", str(e)[:120])

    return ("REWRITTEN", f"cols={changed_cols} rows={table.num_rows} latency={time.time()-t0:.2f}s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CSV path from scan_silver_schemas.py")
    ap.add_argument("--dry-run", action="store_true", help="Report actions without writing")
    ap.add_argument("--topic", choices=list(TOPIC_LABEL_TO_KAFKA), default=None)
    ap.add_argument("--limit", type=int, default=0,
                    help="Process at most N DRIFT files (0 = no limit)")
    args = ap.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    drift_rows = [r for r in rows if r["status"] == "DRIFT"]
    if args.topic:
        drift_rows = [r for r in drift_rows if r["topic"] == args.topic]
    if args.limit:
        drift_rows = drift_rows[: args.limit]

    print(f"[rewrite] Input {args.input}: {len(drift_rows)} DRIFT rows to process "
          f"(dry_run={args.dry_run}, topic={args.topic or 'all'}, limit={args.limit or 'none'})")

    if not drift_rows:
        print("[rewrite] Nothing to do.")
        return

    s3 = create_s3()
    stats = Counter()

    for i, r in enumerate(drift_rows, 1):
        kafka_topic = TOPIC_LABEL_TO_KAFKA[r["topic"]]
        status, detail = rewrite_one(s3, r["key"], kafka_topic, args.dry_run)
        stats[status] += 1
        print(f"[{i:>4d}/{len(drift_rows)}] {status:<13s} {r['key']}  |  {detail}")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for status, count in stats.items():
        print(f"  {status:<13s} {count:>5d}")

    failed = stats["READ_ERROR"] + stats["CAST_ERROR"] + stats["WRITE_ERROR"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
