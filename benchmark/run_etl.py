#!/usr/bin/env python3
"""
run_etl.py — Spark ETL benchmark wrapper (kubectl apply -f job.yaml)

Chạy trên Control Node:
  python run_etl.py --job-yaml ~/k8s/job.yaml --runs 5

Tính năng:
  - Interactive menu chọn dataset (detect từ MinIO, hoặc dùng --datasets)
  - Checkpoint/resume: ~/.etl_checkpoint.json  (an toàn khi bị interrupt)
  - Retry khi job failed  (--run-retries, --retry-delay)
  - TRUNCATE Postgres trước mỗi run  (tắt bằng --no-truncate)
  - Stream submitter pod logs ra màn hình + file results/logs/<ds>_run<n>.log
  - In bảng tích lũy VERSION/RUN1..RUN5/AVG sau mỗi dataset

Xem log thủ công (chạy song song):
  kubectl logs -n spark-etl -l job-name=spark-etl-submitter -f
"""

import argparse
import csv
import datetime
import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time

CHECKPOINT_PATH = os.path.expanduser("~/.etl_checkpoint.json")
BENCH_DIR       = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR     = os.path.join(BENCH_DIR, "results")

PG_TABLES = [
    "dim_symbols",
    "fact_klines",
    "fact_ticker_24h_snapshots",
    "mart_trade_metrics",
    "mart_top_coins",
]

ETL_RUNS_COLS = [
    "dataset", "run_number", "timestamp_iso",
    "t2_sec", "status", "notes",
]
ETL_SUMMARY_COLS = [
    "dataset", "runs_ok", "avg_t2_sec", "min_t2_sec", "max_t2_sec", "std_t2_sec",
]


# -- Helpers --------------------------------------------------------------------

def _size_sort_key(name):
    m = re.match(r"(\d+)(MB|GB)", name.upper())
    if not m:
        return (2, 0, name)
    val, unit = int(m.group(1)), m.group(2)
    return (1, val if unit == "MB" else val * 1024, name)


def load_cfg(env_file=None):
    if env_file:
        try:
            from dotenv import load_dotenv
            load_dotenv(env_file)
        except ImportError:
            pass
    return {
        "minio_endpoint":   os.environ.get("MINIO_ENDPOINT",   "100.74.195.110:30900"),
        "minio_access_key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        "minio_secret_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        "minio_bucket":     os.environ.get("MINIO_BUCKET",     "binance"),
        "pg_host":          os.environ.get("PG_HOST",          "localhost"),
        "pg_port":          int(os.environ.get("PG_PORT",      "5432")),
        "pg_dbname":        os.environ.get("PG_DBNAME",        "crypto_dw"),
        "pg_user":          os.environ.get("PG_USER",          "postgres"),
        "pg_password":      os.environ.get("PG_PASSWORD",      ""),
    }


# -- MinIO dataset detection ----------------------------------------------------

def detect_datasets_minio(cfg):
    """List dataset folders trong DATA_SPLIT/ trên MinIO."""
    import boto3
    from botocore.config import Config as BotoConfig
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{cfg['minio_endpoint']}",
        aws_access_key_id=cfg["minio_access_key"],
        aws_secret_access_key=cfg["minio_secret_key"],
        config=BotoConfig(signature_version="s3v4", connect_timeout=10, read_timeout=30),
    )
    resp = s3.list_objects_v2(
        Bucket=cfg["minio_bucket"],
        Prefix="DATA_SPLIT/",
        Delimiter="/",
    )
    prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    datasets  = [p.rstrip("/").split("/")[-1] for p in prefixes]
    return sorted(datasets, key=_size_sort_key)


# -- PostgreSQL -----------------------------------------------------------------

def truncate_postgres(cfg):
    import psycopg2
    conn = psycopg2.connect(
        host=cfg["pg_host"], port=cfg["pg_port"],
        dbname=cfg["pg_dbname"], user=cfg["pg_user"],
        password=cfg["pg_password"],
        connect_timeout=10,
    )
    conn.autocommit = True
    cur = conn.cursor()
    tables = ", ".join(PG_TABLES)
    cur.execute(f"TRUNCATE TABLE {tables} RESTART IDENTITY CASCADE")
    cur.close()
    conn.close()
    print(f"  [pg] TRUNCATE: {tables}")


# -- Interactive dataset menu ---------------------------------------------------

def interactive_select_datasets(datasets: list) -> list:
    """Hiện menu chọn dataset(s) từ MinIO. Trả về subset đã chọn."""
    print(f"\n{'='*62}")
    print(f"  Spark ETL Benchmark (kubectl) — Chon dataset")
    print(f"{'='*62}")
    print(f"  Datasets tren MinIO (DATA_SPLIT/):\n")
    for i, name in enumerate(datasets, 1):
        print(f"  [{i:2d}]  {name}")
    print(f"\n  [all]  Chay tat ca ({len(datasets)} datasets)")
    print(f"\n  Nhap lua chon (vd: 1  |  1,3,5  |  2-6  |  all):")

    while True:
        try:
            raw = input("  > ").strip().lower()
        except EOFError:
            print("  [auto] Khong co input → chon all.")
            return list(datasets)

        if raw in ("all", ""):
            return list(datasets)

        selected = set()
        valid = True
        for part in raw.split(","):
            part = part.strip()
            if "-" in part:
                try:
                    a, b = part.split("-", 1)
                    a, b = int(a.strip()), int(b.strip())
                    if a < 1 or b > len(datasets) or a > b:
                        raise ValueError()
                    selected.update(range(a, b + 1))
                except ValueError:
                    print(f"  [loi] '{part}' khong hop le (phai la a-b voi 1 <= a <= b <= {len(datasets)}).")
                    valid = False
                    break
            else:
                try:
                    n = int(part)
                    if n < 1 or n > len(datasets):
                        raise ValueError()
                    selected.add(n)
                except ValueError:
                    print(f"  [loi] '{part}' khong hop le (phai tu 1 den {len(datasets)}).")
                    valid = False
                    break

        if valid and selected:
            chosen = [datasets[i - 1] for i in sorted(selected)]
            print(f"\n  Da chon: {chosen}")
            return chosen


# -- Checkpoint -----------------------------------------------------------------

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_PATH):
        return None
    try:
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_checkpoint(state: dict):
    """Ghi atomic (write temp → rename) để tránh corrupt nếu bị kill."""
    tmp = CHECKPOINT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp, CHECKPOINT_PATH)


def clear_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)


def ask_resume(ckpt: dict) -> bool:
    completed = ckpt.get("completed_runs", {})
    n_done    = sum(len(v) for v in completed.values())
    datasets  = ckpt.get("datasets_selected", [])
    n_total   = len(datasets) * ckpt.get("runs_total", 5)
    current   = ckpt.get("current", {})

    print(f"\n{'='*62}")
    print(f"  Phat hien checkpoint chua hoan thanh:")
    print(f"  Session :  {ckpt.get('created_at', 'N/A')}")
    print(f"  Datasets:  {datasets}")
    print(f"  Tien do :  {n_done}/{n_total} runs da hoan thanh")
    if current:
        print(f"  Dung o  :  [{current.get('dataset')}] Run {current.get('run_number')}")
    print(f"{'='*62}\n")
    try:
        ans = input("Resume tu diem dung? [Y/n]: ").strip().lower()
    except EOFError:
        print("  [auto] Khong co input → tu dong resume.")
        ans = "y"
    return ans in ("", "y", "yes")


# -- kubectl helpers ------------------------------------------------------------

def _kubectl(args_list, capture=False, check=True):
    cmd = ["kubectl"] + args_list
    if capture:
        r = subprocess.run(cmd, capture_output=True, text=True)
        return r.stdout.strip(), r.returncode
    else:
        subprocess.run(cmd, check=check)


def patch_configmap(dataset, namespace):
    patch = json.dumps({"data": {"MINIO_PREFIX_CSV_RAW": f"DATA_SPLIT/{dataset}/"}})
    _kubectl(["patch", "configmap", "etl-config",
              "-n", namespace, "--type", "merge", "-p", patch])
    print(f"  [k8s] ConfigMap patched: MINIO_PREFIX_CSV_RAW=DATA_SPLIT/{dataset}/")


def delete_old_job(namespace):
    # Xóa job (submitter pod)
    subprocess.run(
        ["kubectl", "delete", "job", "spark-etl-submitter",
         "-n", namespace, "--ignore-not-found"],
        capture_output=True,
    )
    # Xóa driver pod còn sót từ lần submit trước (Spark không tự dọn)
    subprocess.run(
        ["kubectl", "delete", "pods", "-n", namespace,
         "-l", "spark-role=driver", "--ignore-not-found"],
        capture_output=True,
    )
    # Xóa executor pod còn sót
    subprocess.run(
        ["kubectl", "delete", "pods", "-n", namespace,
         "-l", "spark-role=executor", "--ignore-not-found"],
        capture_output=True,
    )
    print(f"  [k8s] Old job + driver/executor pods deleted.")
    # Chờ tất cả Spark pods biến mất trước khi submit mới
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        out, _ = _kubectl(
            ["get", "pods", "-n", namespace,
             "-l", "job-name=spark-etl-submitter",
             "-o", "jsonpath={.items[*].metadata.name}"],
            capture=True,
        )
        out2, _ = _kubectl(
            ["get", "pods", "-n", namespace,
             "-l", "spark-role=driver",
             "-o", "jsonpath={.items[*].metadata.name}"],
            capture=True,
        )
        if not out.strip() and not out2.strip():
            break
        time.sleep(3)


def apply_job(job_yaml, namespace):
    _kubectl(["apply", "-f", job_yaml])
    print(f"  [k8s] Applied {job_yaml}")


def wait_for_submitter_pod(namespace, timeout=120) -> str:
    """Chờ submitter pod xuất hiện. Trả về pod name."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        out, rc = _kubectl(
            ["get", "pods", "-n", namespace,
             "-l", "job-name=spark-etl-submitter",
             "-o", "jsonpath={.items[*].metadata.name}"],
            capture=True,
        )
        if rc == 0 and out.strip():
            pod = out.strip().split()[0]
            print(f"  [k8s] Submitter pod: {pod}")
            return pod
        time.sleep(3)
    raise TimeoutError(f"Submitter pod khong xuat hien sau {timeout}s")


def _stream_logs_bg(pod, namespace, log_path, stop_event):
    """Thread: stream kubectl logs → stdout + file cho đến khi pod xong hoặc stop_event."""
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    # Không dùng --pod-running-timeout (không hỗ trợ trên kubectl cũ)
    cmd = ["kubectl", "logs", "-n", namespace, pod, "-f"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    with open(log_path, "w", encoding="utf-8", errors="replace") as lf:
        for raw_line in proc.stdout:
            line = raw_line.decode("utf-8", errors="replace")
            sys.stdout.write(line)
            sys.stdout.flush()
            lf.write(line)
            if stop_event.is_set():
                break
    proc.terminate()
    proc.wait()


def _poll_job_status(namespace, timeout, interval=15) -> str:
    """Poll job status mỗi interval giây cho đến khi succeeded/failed/timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        out_s, _ = _kubectl(
            ["get", "job", "spark-etl-submitter", "-n", namespace,
             "-o", "jsonpath={.status.succeeded}"],
            capture=True,
        )
        out_f, _ = _kubectl(
            ["get", "job", "spark-etl-submitter", "-n", namespace,
             "-o", "jsonpath={.status.failed}"],
            capture=True,
        )
        if out_s.strip() == "1":
            return "succeeded"
        if int(out_f.strip() or "0") >= 1:
            return "failed"
        time.sleep(interval)
    return "timeout"


# -- CSV logging ----------------------------------------------------------------

def append_etl_run(row: dict, results_dir):
    os.makedirs(results_dir, exist_ok=True)
    path = os.path.join(results_dir, "etl_runs.csv")
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ETL_RUNS_COLS)
        if write_header:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in ETL_RUNS_COLS})


def write_etl_summary(all_rows, results_dir):
    from collections import defaultdict
    groups = defaultdict(list)
    for r in all_rows:
        if r.get("status") == "ok":
            groups[r["dataset"]].append(r)

    os.makedirs(results_dir, exist_ok=True)
    path = os.path.join(results_dir, "etl_summary.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ETL_SUMMARY_COLS)
        w.writeheader()
        for dataset, rows in sorted(groups.items(), key=lambda x: _size_sort_key(x[0])):
            t2s = [float(r["t2_sec"]) for r in rows]
            avg = statistics.mean(t2s)
            w.writerow({
                "dataset":    dataset,
                "runs_ok":    len(rows),
                "avg_t2_sec": f"{avg:.2f}",
                "min_t2_sec": f"{min(t2s):.2f}",
                "max_t2_sec": f"{max(t2s):.2f}",
                "std_t2_sec": f"{statistics.stdev(t2s):.2f}" if len(t2s) > 1 else "0.00",
            })

    print(f"\n[summary] Da ghi {path}")
    print(f"\n{'-'*62}")
    print(f"{'Dataset':<12} {'Runs':>4} {'AvgT2(s)':>10} {'Min':>8} {'Max':>8} {'Std':>8}")
    print(f"{'-'*62}")
    for dataset, rows in sorted(groups.items(), key=lambda x: _size_sort_key(x[0])):
        t2s = [float(r["t2_sec"]) for r in rows]
        avg = statistics.mean(t2s)
        print(f"{dataset:<12} {len(rows):>4} {avg:>10.2f} {min(t2s):>8.2f} "
              f"{max(t2s):>8.2f} "
              f"{(statistics.stdev(t2s) if len(t2s) > 1 else 0):>8.2f}")
    print(f"{'-'*62}")


def print_cumulative_table(all_rows, runs):
    from collections import defaultdict
    groups = defaultdict(list)
    for r in all_rows:
        groups[r["dataset"]].append(r)

    col_w   = 10
    headers = ["VERSION"] + [f"RUN{i}" for i in range(1, runs + 1)] + ["AVG"]
    sep     = "-" * (10 + (runs + 1) * col_w + 2)

    print(f"\n{sep}")
    print("  " + "".join(f"{h:<{col_w}}" for h in headers))
    print(sep)

    for dataset in sorted(groups.keys(), key=_size_sort_key):
        rows     = sorted(groups[dataset], key=lambda r: int(r["run_number"]))
        run_vals = {int(r["run_number"]): r for r in rows}
        cells    = [f"{dataset:<10}"]
        t2s_ok   = []
        for i in range(1, runs + 1):
            r = run_vals.get(i)
            if r is None:
                cells.append(f"{'—':<{col_w}}")
            elif r["status"] != "ok":
                cells.append(f"{'ERROR':<{col_w}}")
            else:
                val = float(r["t2_sec"])
                t2s_ok.append(val)
                cells.append(f"{val:.1f}s".ljust(col_w))
        cells.append(f"{statistics.mean(t2s_ok):.1f}s" if t2s_ok else "—")
        print("  " + "".join(cells))

    print(sep)


# -- Args -----------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Spark ETL benchmark (kubectl apply wrapper).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--job-yaml",     required=True,
                   help="Path to k8s/job.yaml tren control1 (vd: ~/k8s/job.yaml)")
    p.add_argument("--namespace",    default="spark-etl")
    p.add_argument("--runs",         type=int, default=5)
    p.add_argument("--datasets",     nargs="*", default=None,
                   help="Specific datasets. Omit = auto-detect tu MinIO + hien menu")
    p.add_argument("--run-retries",  type=int, default=1,
                   help="So lan retry moi run neu bi loi (default 1)")
    p.add_argument("--retry-delay",  type=int, default=60,
                   help="Giay cho giua cac lan retry (default 60)")
    p.add_argument("--no-truncate",  action="store_true",
                   help="Khong TRUNCATE Postgres truoc moi run")
    p.add_argument("--config",       default=None, help=".env file")
    p.add_argument("--results-dir",  default=None,
                   help="Thu muc luu ket qua (default: <script_dir>/results)")
    p.add_argument("--pod-timeout",  type=int, default=120,
                   help="Giay cho submitter pod xuat hien (default 120)")
    p.add_argument("--job-timeout",  type=int, default=7200,
                   help="Giay toi da cho 1 run hoan thanh (default 7200=2h)")
    return p.parse_args()


# -- Main -----------------------------------------------------------------------

def main():
    args = parse_args()
    cfg  = load_cfg(args.config)
    ns   = args.namespace

    results_dir = os.path.abspath(args.results_dir) if args.results_dir else RESULTS_DIR
    os.makedirs(results_dir, exist_ok=True)
    log_dir = os.path.join(results_dir, "logs")

    # -- Checkpoint check -------------------------------------------------------
    ckpt        = load_checkpoint()
    resume_mode = False

    if ckpt:
        if ask_resume(ckpt):
            resume_mode = True
            datasets    = ckpt["datasets_selected"]
            args.runs   = ckpt["runs_total"]
        else:
            clear_checkpoint()
            ckpt = None

    if not resume_mode:
        if args.datasets:
            datasets = args.datasets
        else:
            print("[etl] Dang detect datasets tu MinIO ...")
            try:
                all_ds = detect_datasets_minio(cfg)
            except Exception as e:
                print(f"[warn] Khong ket noi MinIO: {e}")
                try:
                    raw = input("  Nhap ten datasets thu cong (cach nhau dau cach): ").strip()
                except EOFError:
                    raw = ""
                all_ds = [x for x in raw.split() if x]
                if not all_ds:
                    print("[error] Khong co dataset nao.")
                    sys.exit(1)

            if not all_ds:
                print("[error] Khong tim thay dataset nao tren MinIO (DATA_SPLIT/).")
                sys.exit(1)

            datasets = interactive_select_datasets(all_ds)

        ckpt = {
            "version":           1,
            "created_at":        datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "job_yaml":          args.job_yaml,
            "runs_total":        args.runs,
            "datasets_selected": datasets,
            "completed_runs":    {ds: [] for ds in datasets},
            "current":           {},
        }
        save_checkpoint(ckpt)

    # Khôi phục các run đã hoàn thành từ checkpoint (để bảng tích lũy đúng)
    all_rows = []
    for ds in datasets:
        for prev_r in ckpt.get("completed_runs", {}).get(ds, []):
            all_rows.append(prev_r)

    print(f"\n{'='*70}")
    print(f" Spark ETL Benchmark (kubectl)  —  {len(datasets)} dataset(s) x {args.runs} runs")
    print(f" Datasets  : {datasets}")
    print(f" MinIO     : {cfg['minio_endpoint']}")
    print(f" Namespace : {ns}")
    print(f" Job YAML  : {args.job_yaml}")
    print(f" Results   : {results_dir}")
    print(f"{'='*70}\n")

    for dataset in datasets:
        print(f"\n{'-'*70}")
        print(f" Dataset: {dataset}")
        print(f"{'-'*70}")

        completed_run_nums = {
            r["run_number"] for r in ckpt.get("completed_runs", {}).get(dataset, [])
        }

        for run_num in range(1, args.runs + 1):
            if run_num in completed_run_nums:
                t2_prev = next(
                    float(r["t2_sec"])
                    for r in ckpt["completed_runs"][dataset]
                    if r["run_number"] == run_num
                )
                print(f"  [skip] Run {run_num} da hoan thanh tu checkpoint (T2={t2_prev:.1f}s)")
                continue

            ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
            print(f"\n[{dataset}] Run {run_num}/{args.runs}  {ts}")

            ckpt["current"] = {"dataset": dataset, "run_number": run_num}
            save_checkpoint(ckpt)

            status = "error"
            notes  = ""
            t2     = 0.0
            log_path = os.path.join(log_dir, f"{dataset}_run{run_num}.log")

            for attempt in range(args.run_retries + 1):
                if attempt > 0:
                    print(f"\n  [retry] Attempt {attempt}/{args.run_retries} "
                          f"— cho {args.retry_delay}s ...")
                    time.sleep(args.retry_delay)

                # Truncate Postgres
                if not args.no_truncate:
                    try:
                        truncate_postgres(cfg)
                    except Exception as e:
                        print(f"  [pg] WARNING truncate failed: {e}")

                t_start = 0.0
                stop_event = threading.Event()
                log_thread = None
                try:
                    patch_configmap(dataset, ns)
                    delete_old_job(ns)
                    apply_job(args.job_yaml, ns)
                    t_start = time.monotonic()

                    pod = wait_for_submitter_pod(ns, timeout=args.pod_timeout)

                    # Stream logs trong background thread (không block)
                    print(f"  [log] {log_path}")
                    log_thread = threading.Thread(
                        target=_stream_logs_bg,
                        args=(pod, ns, log_path, stop_event),
                        daemon=True,
                    )
                    log_thread.start()

                    # Poll job status (authoritative) — đây mới là điều kiện thoát
                    job_st = _poll_job_status(ns, timeout=args.job_timeout)
                    t2 = time.monotonic() - t_start

                    stop_event.set()
                    log_thread.join(timeout=10)

                    if job_st == "succeeded":
                        print(f"\n  -> T2 = {t2:.2f}s  [succeeded]")
                        status = "ok"
                        notes  = ""
                        break
                    else:
                        print(f"\n  -> {job_st.upper()}  T2={t2:.2f}s")
                        notes = f"job {job_st}"

                except Exception as e:
                    if t_start:
                        t2 = time.monotonic() - t_start
                    if stop_event and log_thread:
                        stop_event.set()
                        log_thread.join(timeout=5)
                    notes = str(e)
                    print(f"  -> ERROR: {e}")

            row = {
                "dataset":       dataset,
                "run_number":    run_num,
                "timestamp_iso": ts,
                "t2_sec":        f"{t2:.2f}",
                "status":        status,
                "notes":         notes,
            }
            append_etl_run(row, results_dir)
            all_rows.append(row)

            ckpt["completed_runs"].setdefault(dataset, []).append(row)
            ckpt["current"] = {}
            save_checkpoint(ckpt)

        print_cumulative_table(all_rows, args.runs)

    write_etl_summary(all_rows, results_dir)
    clear_checkpoint()
    print(f"\n[done] Ket qua: {results_dir}/etl_runs.csv  va  etl_summary.csv")


if __name__ == "__main__":
    main()
