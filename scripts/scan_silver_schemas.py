"""
Scan silver/{klines,trades,ticker} parquet files on MinIO, detect schema drift
(OHLCV numeric fields typed as double/float instead of string).

Output: CSV per-file + summary counts. Exit 1 if any DRIFT rows found.
Usage:
  python scripts/scan_silver_schemas.py --out /tmp/silver_scan.csv
  python scripts/scan_silver_schemas.py --topic klines --out /tmp/klines_scan.csv
"""

import argparse
import csv
import io
import os
import sys
from collections import Counter

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig
from dotenv import load_dotenv

# Make top-level modules importable when running from anywhere.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from silver_schema import (
    NUMERIC_TO_STRING_FIELDS,
    SILVER_PREFIX_MAP,
    TOPIC_LABEL_TO_KAFKA,
)


load_dotenv()

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "binance")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"

STRING_TYPE = pa.string()

TOPIC_PREFIXES = {
    label: SILVER_PREFIX_MAP[kafka]
    for label, kafka in TOPIC_LABEL_TO_KAFKA.items()
}


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


def parse_key_parts(key):
    """Extract date/hour/symbol/file from a partitioned object key."""
    out = {"date": "", "hour": "", "symbol": ""}
    for part in key.split("/"):
        if part.startswith("date="):
            out["date"] = part[len("date="):]
        elif part.startswith("hour="):
            out["hour"] = part[len("hour="):]
        elif part.startswith("symbol="):
            out["symbol"] = part[len("symbol="):]
    out["file"] = key.rsplit("/", 1)[-1]
    return out


def iter_parquet_objects(s3, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            if obj["Key"].endswith(".parquet"):
                yield obj


def inspect_one(s3, key, kafka_topic):
    """Return (status, drift_col, drift_type, num_rows, created_by).

    status is one of OK | DRIFT | ERROR. For DRIFT we report the first
    numeric column whose type is not pa.string().
    """
    try:
        body = s3.get_object(Bucket=MINIO_BUCKET, Key=key)["Body"].read()
        buf = io.BytesIO(body)
        meta = pq.read_metadata(buf)
        buf.seek(0)
        schema = pq.read_schema(buf)
    except Exception as e:
        return ("ERROR", "", str(e)[:80], 0, "")

    numeric_fields = NUMERIC_TO_STRING_FIELDS.get(kafka_topic, frozenset())
    schema_names = set(schema.names)
    for field_name in numeric_fields:
        if field_name not in schema_names:
            continue
        ftype = schema.field(field_name).type
        if ftype != STRING_TYPE:
            return ("DRIFT", field_name, str(ftype), meta.num_rows, meta.created_by or "")
    return ("OK", "", "", meta.num_rows, meta.created_by or "")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="CSV output path")
    ap.add_argument("--topic", choices=["klines", "trades", "ticker", "all"], default="all")
    ap.add_argument("--limit", type=int, default=0,
                    help="Inspect at most N files per topic (0 = no limit)")
    args = ap.parse_args()

    s3 = create_s3()
    topics = list(TOPIC_LABEL_TO_KAFKA) if args.topic == "all" else [args.topic]

    rows = []
    summary = {t: Counter() for t in topics}

    for label in topics:
        prefix = TOPIC_PREFIXES[label] + "/"
        kafka_topic = TOPIC_LABEL_TO_KAFKA[label]
        print(f"[scan] {label}: listing s3://{MINIO_BUCKET}/{prefix}", flush=True)

        count = 0
        for obj in iter_parquet_objects(s3, prefix):
            if args.limit and count >= args.limit:
                break
            key = obj["Key"]
            parts = parse_key_parts(key)
            status, drift_col, drift_type, num_rows, created_by = inspect_one(s3, key, kafka_topic)
            rows.append({
                "topic": label,
                "date": parts["date"],
                "hour": parts["hour"],
                "symbol": parts["symbol"],
                "file": parts["file"],
                "key": key,
                "status": status,
                "drift_col": drift_col,
                "drift_type": drift_type,
                "num_rows": num_rows,
                "created_by": created_by,
            })
            summary[label][status] += 1
            count += 1
            if count % 500 == 0:
                print(f"[scan] {label}: {count} files inspected...", flush=True)

        print(f"[scan] {label}: done, {count} files", flush=True)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "topic", "date", "hour", "symbol", "file", "key",
            "status", "drift_col", "drift_type", "num_rows", "created_by",
        ])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n[scan] Wrote {len(rows)} rows to {args.out}\n")
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for label in topics:
        c = summary[label]
        total = sum(c.values())
        print(f"{label:8s} | OK={c['OK']:>5d}  DRIFT={c['DRIFT']:>5d}  ERROR={c['ERROR']:>3d}  (total {total})")

    any_drift = any(summary[t]["DRIFT"] > 0 for t in topics)
    sys.exit(1 if any_drift else 0)


if __name__ == "__main__":
    main()
