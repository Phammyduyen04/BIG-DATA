#!/usr/bin/env python3
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", write_through=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", write_through=True)
"""
Benchmark T1: Đo thời gian upload CSV -> MinIO cho từng dataset size.

Quy trình mỗi run:
  1. Upload DATA_SPLIT/<size>/ -> MinIO DATA_SPLIT/<size>/
  2. Ghi log T1 (time.monotonic)
  3. Xóa prefix trên MinIO
  4. Lặp lại --runs lần

Chạy:
  python bench_upload.py \
      --data-root /home/duyen/DATA_SPLIT \
      --runs 5 \
      [--datasets 100MB 1GB 2GB]   # bỏ qua để tự detect tất cả

Kết quả:
  benchmark/results/upload_runs.csv     (1 dòng / run)
  benchmark/results/upload_summary.csv  (trung bình + std mỗi size)
"""
import argparse
import csv
import datetime
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

BENCH_DIR   = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BENCH_DIR, "results")

RUNS_COLS = [
    "dataset", "run_number", "timestamp_iso",
    "files", "bytes_total", "t1_sec", "throughput_mb_s", "status", "notes",
]
SUMMARY_COLS = [
    "dataset", "runs_ok", "files", "bytes_total",
    "avg_t1_sec", "min_t1_sec", "max_t1_sec", "std_t1_sec",
    "avg_throughput_mb_s",
]


# -- MinIO helpers --------------------------------------------------------------

def _make_s3(cfg):
    return boto3.client(
        "s3",
        endpoint_url=f"http://{cfg['minio_endpoint']}",
        aws_access_key_id=cfg["minio_access_key"],
        aws_secret_access_key=cfg["minio_secret_key"],
        config=Config(
            signature_version="s3v4",
            max_pool_connections=32,
            retries={"max_attempts": 10, "mode": "adaptive"},
        ),
    )


def _upload_one(s3, local_path, bucket, key, transfer_cfg):
    s3.upload_file(local_path, bucket, key, Config=transfer_cfg)
    return key


def upload_dataset(cfg, dataset, data_root, workers=8):
    """Upload DATA_SPLIT/<dataset>/*.csv -> MinIO DATA_SPLIT/<dataset>/. Trả về (t1, n_files, n_bytes)."""
    local_dir = os.path.join(data_root, dataset)
    if not os.path.isdir(local_dir):
        raise FileNotFoundError(f"Không tìm thấy: {local_dir}")

    files = sorted(f for f in os.listdir(local_dir) if f.endswith(".csv"))
    if not files:
        raise ValueError(f"Không có file CSV trong {local_dir}")

    bucket = cfg["minio_bucket"]
    prefix = f"DATA_SPLIT/{dataset}"
    total_bytes = sum(os.path.getsize(os.path.join(local_dir, f)) for f in files)

    transfer_cfg = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,
        multipart_chunksize=64 * 1024 * 1024,
        max_concurrency=8,
    )
    s3 = _make_s3(cfg)

    print(f"  [upload] {len(files)} files  {total_bytes/1e6:.1f} MB  -> s3://{bucket}/{prefix}/")
    t_start = time.monotonic()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _upload_one, s3,
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
            if done % max(1, len(files) // 5) == 0 or done == len(files):
                elapsed = time.monotonic() - t_start
                speed = (total_bytes / 1e6) * (done / len(files)) / elapsed if elapsed > 0 else 0
                print(f"  [upload]   {done}/{len(files)} files  ({speed:.1f} MB/s est.) ...")

    # HEAD fence trên file cuối để đảm bảo flush
    s3.head_object(Bucket=bucket, Key=f"{prefix}/{files[-1]}")

    t1 = time.monotonic() - t_start
    return t1, len(files), total_bytes


def delete_dataset(cfg, dataset):
    """Xóa toàn bộ DATA_SPLIT/<dataset>/ trên MinIO (xóa từng object, tránh lỗi Content-MD5)."""
    bucket = cfg["minio_bucket"]
    prefix = f"DATA_SPLIT/{dataset}/"
    s3 = _make_s3(cfg)

    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    for key in keys:
        s3.delete_object(Bucket=bucket, Key=key)
    print(f"  [delete]  Xóa {len(keys)} objects từ s3://{bucket}/{prefix}")


# -- CSV logging ----------------------------------------------------------------

def append_run(row: dict):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, "upload_runs.csv")
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RUNS_COLS)
        if write_header:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in RUNS_COLS})


def write_summary(all_rows: list):
    """Tính avg/min/max/std cho từng dataset và ghi summary.csv."""
    # group by dataset
    from collections import defaultdict
    groups = defaultdict(list)
    for r in all_rows:
        if r["status"] == "ok":
            groups[r["dataset"]].append(r)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, "upload_summary.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_COLS)
        w.writeheader()
        for dataset, rows in sorted(groups.items()):
            t1s = [float(r["t1_sec"]) for r in rows]
            n_bytes = int(rows[0]["bytes_total"])
            avg_t1 = statistics.mean(t1s)
            w.writerow({
                "dataset":          dataset,
                "runs_ok":          len(rows),
                "files":            rows[0]["files"],
                "bytes_total":      n_bytes,
                "avg_t1_sec":       f"{avg_t1:.2f}",
                "min_t1_sec":       f"{min(t1s):.2f}",
                "max_t1_sec":       f"{max(t1s):.2f}",
                "std_t1_sec":       f"{statistics.stdev(t1s):.2f}" if len(t1s) > 1 else "0.00",
                "avg_throughput_mb_s": f"{(n_bytes/1e6)/avg_t1:.1f}",
            })

    print(f"\n[summary] Đã ghi {path}")
    # In bảng ra stdout
    print(f"\n{'-'*70}")
    print(f"{'Dataset':<12} {'Runs':>4} {'AvgT1(s)':>10} {'MinT1(s)':>10} "
          f"{'MaxT1(s)':>10} {'Std(s)':>8} {'MB/s':>8}")
    print(f"{'-'*70}")
    for dataset, rows in sorted(groups.items()):
        t1s = [float(r["t1_sec"]) for r in rows]
        n_bytes = int(rows[0]["bytes_total"])
        avg_t1 = statistics.mean(t1s)
        print(f"{dataset:<12} {len(rows):>4} {avg_t1:>10.2f} {min(t1s):>10.2f} "
              f"{max(t1s):>10.2f} "
              f"{(statistics.stdev(t1s) if len(t1s)>1 else 0):>8.2f} "
              f"{(n_bytes/1e6)/avg_t1:>8.1f}")
    print(f"{'-'*70}")


# -- CLI ------------------------------------------------------------------------

def load_cfg(env_file=None):
    if env_file:
        load_dotenv(env_file)
    return {
        "minio_endpoint":   os.environ["MINIO_ENDPOINT"],
        "minio_access_key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        "minio_secret_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        "minio_bucket":     os.environ.get("MINIO_BUCKET", "binance"),
    }


def _size_sort_key(name):
    import re
    m = re.match(r"(\d+)(MB|GB)", name.upper())
    if not m:
        return (2, 0, name)
    val, unit = int(m.group(1)), m.group(2)
    return (1, val if unit == "MB" else val * 1024, name)


def detect_datasets(data_root):
    """Tự detect các folder có CSV trong DATA_SPLIT/, sort theo size thực tế."""
    sizes = []
    for name in os.listdir(data_root):
        d = os.path.join(data_root, name)
        if os.path.isdir(d) and any(f.endswith(".csv") for f in os.listdir(d)):
            sizes.append(name)
    return sorted(sizes, key=_size_sort_key)


def clean_all_datasets(cfg):
    """Xóa toàn bộ DATA_SPLIT/ trên MinIO (dùng khi test lại từ đầu)."""
    bucket = cfg["minio_bucket"]
    prefix = "DATA_SPLIT/"
    s3 = _make_s3(cfg)
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    for key in keys:
        s3.delete_object(Bucket=bucket, Key=key)
    print(f"[clean] Xoa {len(keys)} objects duoi s3://{bucket}/{prefix}")
    return len(keys)


def parse_args():
    p = argparse.ArgumentParser(
        description="MinIO upload benchmark (T1).\n"
                    "Mac dinh: chay --runs lan, giu lai du lieu sau lan cuoi cung moi dataset.\n"
                    "Dung --clean truoc khi test lai tu dau.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--data-root",   required=True, help="Path to DATA_SPLIT/ folder")
    p.add_argument("--runs",        type=int, default=5, help="Runs per dataset (default: 5)")
    p.add_argument("--datasets",    nargs="*", default=None,
                   help="Specific datasets (e.g. 100MB 1GB). Omit = auto-detect all")
    p.add_argument("--config",      default=None, help=".env file with MINIO_ENDPOINT etc.")
    p.add_argument("--workers",     type=int, default=8, help="Parallel upload threads (default: 8)")
    p.add_argument("--results-dir", default=None,
                   help="Folder to store CSV logs (default: benchmark/results/)")
    p.add_argument("--clean",       action="store_true",
                   help="Xoa sach toan bo DATA_SPLIT/ tren MinIO truoc khi chay")
    return p.parse_args()


def main():
    args = parse_args()
    cfg  = load_cfg(args.config)

    global RESULTS_DIR
    if args.results_dir:
        RESULTS_DIR = os.path.abspath(args.results_dir)
        os.makedirs(RESULTS_DIR, exist_ok=True)

    # --clean: xóa hết MinIO trước khi bắt đầu
    if args.clean:
        print(f"\n[clean] Xoa toan bo DATA_SPLIT/ tren MinIO truoc khi chay ...")
        n = clean_all_datasets(cfg)
        print(f"[clean] Done. Da xoa {n} objects.\n")

    datasets = args.datasets or detect_datasets(args.data_root)
    if not datasets:
        print(f"Khong tim thay dataset nao trong {args.data_root}")
        sys.exit(1)

    print(f"\n{'='*70}")
    print(f" MinIO Upload Benchmark  --  {len(datasets)} dataset(s)  x  {args.runs} runs")
    print(f" Datasets: {datasets}")
    print(f" MinIO:    {cfg['minio_endpoint']}  bucket={cfg['minio_bucket']}")
    print(f" Mode:     run 1-{args.runs-1} upload+delete | run {args.runs} upload+KEEP")
    print(f"{'='*70}")

    all_rows = []

    for dataset in datasets:
        print(f"\n{'-'*70}")
        print(f" Dataset: {dataset}")
        print(f"{'-'*70}")

        for run_num in range(1, args.runs + 1):
            is_last = (run_num == args.runs)
            ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
            print(f"\n[{dataset}] Run {run_num}/{args.runs}  {ts}"
                  + ("  [KEEP]" if is_last else "  [delete after]"))

            status = "ok"
            notes  = ""
            t1 = n_files = n_bytes = 0

            try:
                t1, n_files, n_bytes = upload_dataset(
                    cfg, dataset, args.data_root, workers=args.workers
                )
                mb_s = (n_bytes / 1e6) / t1
                print(f"  -> T1 = {t1:.2f}s  ({mb_s:.1f} MB/s)")
            except Exception as e:
                status = "error"
                notes  = str(e)
                print(f"  -> ERROR: {e}")

            row = {
                "dataset":         dataset,
                "run_number":      run_num,
                "timestamp_iso":   ts,
                "files":           n_files,
                "bytes_total":     n_bytes,
                "t1_sec":          f"{t1:.3f}",
                "throughput_mb_s": f"{(n_bytes/1e6)/t1:.1f}" if t1 > 0 else "0",
                "status":          status,
                "notes":           notes,
            }
            append_run(row)
            all_rows.append(row)

            # Chỉ xóa nếu KHÔNG phải run cuối; run cuối giữ lại trên MinIO
            if not is_last:
                try:
                    delete_dataset(cfg, dataset)
                except Exception as e:
                    print(f"  -> WARNING delete: {e}")
            else:
                print(f"  [keep] Du lieu giu lai tren MinIO: DATA_SPLIT/{dataset}/")

    write_summary(all_rows)
    print(f"\n[done] Ket qua trong {RESULTS_DIR}/upload_runs.csv va upload_summary.csv")


if __name__ == "__main__":
    main()
