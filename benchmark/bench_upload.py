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

Tính năng an toàn:
  - Interactive menu chọn dataset (hoặc dùng --datasets để bỏ qua menu)
  - Checkpoint/resume: lưu tiến trình mỗi run vào results/checkpoint.json
    Nếu bị interrupt, khởi động lại sẽ hỏi có muốn tiếp tục không
  - --clean cần gõ "yes" để xác nhận (tránh xóa nhầm)

Chạy:
  python bench_upload.py --data-root d:/HK2_2025-2026/DLL/Project/DATA_SPLIT --runs 5

Kết quả:
  benchmark/results/upload_runs.csv     (1 dòng / run)
  benchmark/results/upload_summary.csv  (trung bình + std mỗi size)
  benchmark/results/checkpoint.json     (xóa tự động khi hoàn thành)
"""
import argparse
import csv
import datetime
import json
import os
import random
import re
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
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

def _make_s3(cfg, read_timeout=120, connect_timeout=10, max_attempts=10):
    return boto3.client(
        "s3",
        endpoint_url=f"http://{cfg['minio_endpoint']}",
        aws_access_key_id=cfg["minio_access_key"],
        aws_secret_access_key=cfg["minio_secret_key"],
        config=Config(
            signature_version="s3v4",
            max_pool_connections=32,
            retries={"max_attempts": max_attempts, "mode": "adaptive"},
            read_timeout=read_timeout,
            connect_timeout=connect_timeout,
        ),
    )


def _delete_with_retry(s3, bucket, key, max_attempts=5):
    """Delete a single object with exponential backoff."""
    for attempt in range(max_attempts):
        try:
            s3.delete_object(Bucket=bucket, Key=key)
            return
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            wait = min(2 ** attempt + random.uniform(0, 1), 30)
            print(f"  [delete] retry {attempt+1}/{max_attempts-1} for {key}: {e}  (wait {wait:.1f}s)")
            time.sleep(wait)


def _upload_one(s3, local_path, bucket, key, transfer_cfg):
    s3.upload_file(local_path, bucket, key, Config=transfer_cfg)
    return key


def upload_dataset(cfg, dataset, data_root, workers=4):
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

    # max_concurrency=4 và workers=4: tránh SlowDownWrite trên MinIO 3-node
    # (8×8=64 concurrent writes trước đây quá tải erasure coding quorum)
    transfer_cfg = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,
        multipart_chunksize=64 * 1024 * 1024,
        max_concurrency=4,
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
    s3 = _make_s3(cfg, read_timeout=120, max_attempts=3)

    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    for key in keys:
        _delete_with_retry(s3, bucket, key)
    print(f"  [delete]  Xóa {len(keys)} objects từ s3://{bucket}/{prefix}")


# -- CSV logging ----------------------------------------------------------------

def append_run(row: dict, results_dir=None):
    d = results_dir or RESULTS_DIR
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, "upload_runs.csv")
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RUNS_COLS)
        if write_header:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in RUNS_COLS})


def write_summary(all_rows: list, results_dir=None):
    """Tính avg/min/max/std cho từng dataset và ghi summary.csv."""
    from collections import defaultdict
    groups = defaultdict(list)
    for r in all_rows:
        if r.get("status") == "ok":
            groups[r["dataset"]].append(r)

    d = results_dir or RESULTS_DIR
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, "upload_summary.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_COLS)
        w.writeheader()
        for dataset, rows in sorted(groups.items(), key=lambda x: _size_sort_key(x[0])):
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
    print(f"\n{'-'*70}")
    print(f"{'Dataset':<12} {'Runs':>4} {'AvgT1(s)':>10} {'MinT1(s)':>10} "
          f"{'MaxT1(s)':>10} {'Std(s)':>8} {'MB/s':>8}")
    print(f"{'-'*70}")
    for dataset, rows in sorted(groups.items(), key=lambda x: _size_sort_key(x[0])):
        t1s = [float(r["t1_sec"]) for r in rows]
        n_bytes = int(rows[0]["bytes_total"])
        avg_t1 = statistics.mean(t1s)
        print(f"{dataset:<12} {len(rows):>4} {avg_t1:>10.2f} {min(t1s):>10.2f} "
              f"{max(t1s):>10.2f} "
              f"{(statistics.stdev(t1s) if len(t1s)>1 else 0):>8.2f} "
              f"{(n_bytes/1e6)/avg_t1:>8.1f}")
    print(f"{'-'*70}")


# -- Checkpoint helpers ---------------------------------------------------------

def _checkpoint_path(results_dir):
    return os.path.join(results_dir, "checkpoint.json")


def load_checkpoint(results_dir):
    """Đọc checkpoint.json. Trả về dict nếu tồn tại, None nếu không."""
    path = _checkpoint_path(results_dir)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_checkpoint(results_dir, state: dict):
    """Ghi checkpoint atomically (write temp -> rename) để tránh corrupt nếu bị kill."""
    os.makedirs(results_dir, exist_ok=True)
    path = _checkpoint_path(results_dir)
    tmp  = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def clear_checkpoint(results_dir):
    path = _checkpoint_path(results_dir)
    if os.path.exists(path):
        os.remove(path)


def ask_resume(checkpoint: dict) -> bool:
    """In thông tin checkpoint và hỏi user có muốn resume không. Trả về True = resume."""
    completed = checkpoint.get("completed_runs", {})
    n_done = sum(len(v) for v in completed.values())
    datasets = checkpoint.get("datasets_selected", [])
    runs_total = checkpoint.get("runs_total", 5)
    n_total = len(datasets) * runs_total
    current = checkpoint.get("current", {})

    print(f"\n{'='*60}")
    print(f"  Phat hien checkpoint chua hoan thanh:")
    print(f"  Session :  {checkpoint.get('created_at', 'N/A')}")
    print(f"  Datasets:  {datasets}")
    print(f"  Tien do :  {n_done}/{n_total} runs da hoan thanh")
    if current:
        print(f"  Dung o  :  [{current.get('dataset')}] Run {current.get('run_number')}")
    print(f"{'='*60}\n")
    try:
        ans = input("Resume tu diem dung? [Y/n]: ").strip().lower()
    except EOFError:
        # non-interactive (nohup/pipe) → auto resume để an toàn
        print("  [auto] Khong co input → tu dong resume.")
        ans = "y"
    return ans in ("", "y", "yes")


# -- Interactive dataset selection ----------------------------------------------

def _dataset_info(data_root, name):
    """Trả về (n_files, total_bytes) cho dataset."""
    d = os.path.join(data_root, name)
    files = [f for f in os.listdir(d) if f.endswith(".csv")]
    total = sum(os.path.getsize(os.path.join(d, f)) for f in files)
    return len(files), total


def interactive_select_datasets(datasets: list, data_root: str) -> list:
    """Hiện menu chọn dataset(s). Trả về subset đã chọn."""
    print(f"\n{'='*60}")
    print(f"  MinIO Upload Benchmark - Chon dataset")
    print(f"{'='*60}")
    print(f"  Datasets trong {data_root}:\n")

    for i, name in enumerate(datasets, 1):
        try:
            n_files, n_bytes = _dataset_info(data_root, name)
            print(f"  [{i:2d}]  {name:<8}  {n_files:3d} files  {n_bytes/1e6:8.1f} MB")
        except Exception:
            print(f"  [{i:2d}]  {name}")

    print(f"  [all]  Chay tat ca ({len(datasets)} datasets)")
    print(f"\n  Nhap lua chon (vd: 1  |  1,3,5  |  2-6  |  all):")

    while True:
        try:
            raw = input("  > ").strip().lower()
        except EOFError:
            print("  [auto] Khong co input → chon all.")
            return list(datasets)

        if raw in ("all", ""):
            return list(datasets)

        selected_indices = set()
        valid = True

        for part in raw.split(","):
            part = part.strip()
            if "-" in part:
                try:
                    a, b = part.split("-", 1)
                    a, b = int(a.strip()), int(b.strip())
                    if a < 1 or b > len(datasets) or a > b:
                        raise ValueError()
                    selected_indices.update(range(a, b + 1))
                except ValueError:
                    print(f"  [loi] '{part}' khong hop le (phai la a-b voi 1 <= a <= b <= {len(datasets)}).")
                    valid = False
                    break
            else:
                try:
                    n = int(part)
                    if n < 1 or n > len(datasets):
                        raise ValueError()
                    selected_indices.add(n)
                except ValueError:
                    print(f"  [loi] '{part}' khong hop le (phai tu 1 den {len(datasets)}).")
                    valid = False
                    break

        if valid and selected_indices:
            chosen = [datasets[i - 1] for i in sorted(selected_indices)]
            print(f"\n  Da chon: {chosen}")
            return chosen


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
    s3 = _make_s3(cfg, read_timeout=120, max_attempts=3)
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    for i, key in enumerate(keys):
        _delete_with_retry(s3, bucket, key)
        if (i + 1) % 20 == 0:
            print(f"  [clean] {i+1}/{len(keys)} objects deleted ...")
    print(f"[clean] Xoa {len(keys)} objects duoi s3://{bucket}/{prefix}")
    return len(keys)


def parse_args():
    p = argparse.ArgumentParser(
        description="MinIO upload benchmark (T1) voi checkpoint/resume va interactive menu.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--data-root",   required=True, help="Path to DATA_SPLIT/ folder")
    p.add_argument("--runs",        type=int, default=5, help="Runs per dataset (default: 5)")
    p.add_argument("--datasets",    nargs="*", default=None,
                   help="Specific datasets (vd: 100MB 1GB). Bo qua = hien menu chon")
    p.add_argument("--config",      default=None, help=".env file with MINIO_ENDPOINT etc.")
    p.add_argument("--workers",     type=int, default=4, help="Parallel upload threads (default: 4)")
    p.add_argument("--results-dir", default=None,
                   help="Folder to store CSV logs (default: benchmark/results/)")
    p.add_argument("--clean",       action="store_true",
                   help="Xoa sach toan bo DATA_SPLIT/ tren MinIO (can xac nhan 'yes')")
    return p.parse_args()


def main():
    args = parse_args()
    cfg  = load_cfg(args.config)

    global RESULTS_DIR
    if args.results_dir:
        RESULTS_DIR = os.path.abspath(args.results_dir)
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # --clean: xóa hết MinIO trước khi bắt đầu — cần xác nhận "yes"
    if args.clean:
        print(f"\n[CANH BAO] --clean se xoa TOAN BO DATA_SPLIT/ tren MinIO.")
        print(f"  MinIO: {cfg['minio_endpoint']}  bucket={cfg['minio_bucket']}")
        try:
            ans = input('  Nhap "yes" de xac nhan: ').strip()
        except EOFError:
            # nohup/pipe không có stdin → từ chối để an toàn
            print("  [auto] Khong co input → huy --clean de an toan.")
            ans = ""
        if ans == "yes":
            print(f"\n[clean] Xoa toan bo DATA_SPLIT/ tren MinIO truoc khi chay ...")
            n = clean_all_datasets(cfg)
            print(f"[clean] Done. Da xoa {n} objects.\n")
            clear_checkpoint(RESULTS_DIR)
        else:
            print("  [skip] Bo qua --clean. Tiep tuc ma khong xoa MinIO.")

    # -- Checkpoint / Resume ---------------------------------------------------
    checkpoint = load_checkpoint(RESULTS_DIR)
    resuming = False

    if checkpoint:
        if ask_resume(checkpoint):
            resuming = True
            datasets = checkpoint["datasets_selected"]
            # Nếu --runs thay đổi khi resume, ưu tiên giá trị đã lưu
            args.runs = checkpoint.get("runs_total", args.runs)
        else:
            clear_checkpoint(RESULTS_DIR)
            checkpoint = None

    if not resuming:
        all_available = detect_datasets(args.data_root)
        if not all_available:
            print(f"Khong tim thay dataset nao trong {args.data_root}")
            sys.exit(1)

        if args.datasets:
            datasets = args.datasets
        else:
            datasets = interactive_select_datasets(all_available, args.data_root)

        checkpoint = {
            "version": 1,
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "data_root": args.data_root,
            "runs_total": args.runs,
            "datasets_selected": datasets,
            "completed_runs": {d: [] for d in datasets},
            "current": {},
        }
        save_checkpoint(RESULTS_DIR, checkpoint)

    # -- Header ----------------------------------------------------------------
    print(f"\n{'='*70}")
    print(f" MinIO Upload Benchmark  --  {len(datasets)} dataset(s)  x  {args.runs} runs")
    print(f" Datasets: {datasets}")
    print(f" MinIO:    {cfg['minio_endpoint']}  bucket={cfg['minio_bucket']}")
    print(f" Mode:     run 1-{args.runs-1} upload+delete | run {args.runs} upload+KEEP")
    if resuming:
        n_done = sum(len(v) for v in checkpoint.get("completed_runs", {}).values())
        n_total = len(datasets) * args.runs
        print(f" [RESUME] Tiep tuc tu checkpoint ({n_done}/{n_total} runs da xong)")
    print(f"{'='*70}")

    all_rows = []

    # Nạp lại các runs đã hoàn thành từ checkpoint (để write_summary cuối đầy đủ)
    for d_name, runs in checkpoint.get("completed_runs", {}).items():
        for r in runs:
            all_rows.append(r)

    for dataset in datasets:
        print(f"\n{'-'*70}")
        print(f" Dataset: {dataset}")
        print(f"{'-'*70}")

        completed_run_numbers = {
            r["run_number"]
            for r in checkpoint.get("completed_runs", {}).get(dataset, [])
        }

        for run_num in range(1, args.runs + 1):
            if run_num in completed_run_numbers:
                print(f"  [skip] Run {run_num}/{args.runs} da hoan thanh (tu checkpoint)")
                continue

            is_last = (run_num == args.runs)
            ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

            # Đánh dấu đang chạy run này trước
            checkpoint["current"] = {"dataset": dataset, "run_number": run_num}
            save_checkpoint(RESULTS_DIR, checkpoint)

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
            append_run(row, RESULTS_DIR)
            all_rows.append(row)

            # Ghi checkpoint: run này đã hoàn thành
            checkpoint.setdefault("completed_runs", {}).setdefault(dataset, []).append(row)
            save_checkpoint(RESULTS_DIR, checkpoint)

            if not is_last:
                try:
                    delete_dataset(cfg, dataset)
                except Exception as e:
                    print(f"  -> WARNING delete: {e}")
            else:
                print(f"  [keep] Du lieu giu lai tren MinIO: DATA_SPLIT/{dataset}/")

    write_summary(all_rows, RESULTS_DIR)

    # Hoàn thành toàn bộ → xóa checkpoint
    clear_checkpoint(RESULTS_DIR)
    print(f"\n[done] Ket qua trong {RESULTS_DIR}/upload_runs.csv va upload_summary.csv")


if __name__ == "__main__":
    main()
