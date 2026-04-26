#!/usr/bin/env python3
"""
Benchmark T2: Đo thời gian Spark ETL (MinIO CSV → PostgreSQL) cho từng dataset size.

Quy trình mỗi run:
  1. TRUNCATE 5 tables Postgres
  2. spark-submit (client mode) launcher.py với MINIO_PREFIX_CSV_RAW=DATA_SPLIT/<dataset>/
  3. Ghi log T2 (time.monotonic)
  4. Lặp lại --runs lần

In bảng tích lũy sau mỗi dataset hoàn thành:
  VERSION   RUN1     RUN2     RUN3     RUN4     RUN5   AVG
  100MB     248.9s   376.8s   306.8s   ...      ...    310.9s
  250MB     ...

Chạy trên Control Node (driver chạy local, executors là K8s pods):
  python benchmark/bench_etl.py \\
      --launcher spark_processing/jobs/launcher.py \\
      --control-ip 100.74.195.110 \\
      --runs 5

Kết quả:
  benchmark/results/etl_runs.csv
  benchmark/results/etl_summary.csv
"""
import argparse
import csv
import datetime
import os
import re
import statistics
import subprocess
import sys
import time

import boto3
import psycopg2
from botocore.config import Config
from dotenv import load_dotenv

BENCH_DIR   = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BENCH_DIR, "results")

ETL_RUNS_COLS = [
    "dataset", "run_number", "timestamp_iso",
    "t2_sec", "status", "returncode", "notes",
]
ETL_SUMMARY_COLS = [
    "dataset", "runs_ok", "avg_t2_sec", "min_t2_sec", "max_t2_sec", "std_t2_sec",
]

PG_TABLES = [
    "dim_symbols",
    "fact_klines",
    "fact_ticker_24h_snapshots",
    "mart_trade_metrics",
    "mart_top_coins",
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
        load_dotenv(env_file)
    return {
        "minio_endpoint":   os.environ["MINIO_ENDPOINT"],
        "minio_access_key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        "minio_secret_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        "minio_bucket":     os.environ.get("MINIO_BUCKET", "binance"),
        "pg_host":          os.environ.get("PG_HOST", "localhost"),
        "pg_port":          int(os.environ.get("PG_PORT", "5432")),
        "pg_dbname":        os.environ.get("PG_DBNAME", "crypto_dw"),
        "pg_user":          os.environ.get("PG_USER", "postgres"),
        "pg_password":      os.environ.get("PG_PASSWORD", ""),
    }


# -- MinIO dataset detection ----------------------------------------------------

def detect_datasets_minio(cfg):
    """List các dataset folders trong DATA_SPLIT/ trên MinIO."""
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{cfg['minio_endpoint']}",
        aws_access_key_id=cfg["minio_access_key"],
        aws_secret_access_key=cfg["minio_secret_key"],
        config=Config(signature_version="s3v4", connect_timeout=10, read_timeout=30),
    )
    resp = s3.list_objects_v2(
        Bucket=cfg["minio_bucket"],
        Prefix="DATA_SPLIT/",
        Delimiter="/",
    )
    prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    datasets = [p.rstrip("/").split("/")[-1] for p in prefixes]
    return sorted(datasets, key=_size_sort_key)


# -- PostgreSQL -----------------------------------------------------------------

def truncate_postgres(cfg):
    """TRUNCATE 5 tables trước mỗi run để bắt đầu sạch."""
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


# -- Spark ETL ------------------------------------------------------------------

def build_spark_cmd(args, cfg, dataset):
    """Xây dựng spark-submit command cho 1 dataset."""
    spark_submit = os.path.join(args.spark_home, "bin", "spark-submit")
    launcher = os.path.abspath(args.launcher)

    cmd = [
        spark_submit,
        "--master",      f"k8s://https://{args.control_ip}:6443",
        "--deploy-mode", "client",
        "--name",        f"CryptoDW-Bench-{dataset}",
        # K8s
        "--conf", f"spark.kubernetes.namespace={args.k8s_namespace}",
        "--conf", f"spark.kubernetes.authenticate.driver.serviceAccountName={args.service_account}",
        "--conf", f"spark.kubernetes.container.image={args.image}",
        "--conf",  "spark.kubernetes.container.image.pullPolicy=Always",
        # Driver network (executor pods dial back to control node)
        "--conf", f"spark.driver.host={args.control_ip}",
        "--conf", f"spark.driver.port={args.driver_port}",
        "--conf", f"spark.driver.blockManager.port={args.block_manager_port}",
        "--conf",  "spark.driver.bindAddress=0.0.0.0",
        # Resources
        "--driver-memory",   args.driver_memory,
        "--conf", f"spark.driver.memoryOverhead={args.driver_memory_overhead}",
        "--conf", f"spark.executor.instances={args.executor_instances}",
        "--conf",  "spark.executor.cores=1",
        "--conf", f"spark.executor.memory={args.executor_memory}",
        "--conf", f"spark.executor.memoryOverhead={args.executor_memory_overhead}",
        "--conf", f"spark.kubernetes.executor.request.cores={args.executor_request_cores}",
        "--conf", f"spark.kubernetes.executor.limit.cores={args.executor_limit_cores}",
        # Dataset-specific path
        "--conf", f"spark.executorEnv.MINIO_PREFIX_CSV_RAW=DATA_SPLIT/{dataset}/",
        # SQL + AQE tuning
        "--conf", "spark.sql.shuffle.partitions=48",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
        "--conf", "spark.sql.adaptive.coalescePartitions.initialPartitionNum=48",
        "--conf", "spark.sql.adaptive.advisoryPartitionSizeInBytes=64m",
        "--conf", "spark.sql.adaptive.skewJoin.enabled=true",
        "--conf", "spark.sql.files.maxPartitionBytes=64m",
        "--conf", "spark.sql.files.openCostInBytes=4m",
        # Memory
        "--conf", "spark.memory.fraction=0.7",
        "--conf", "spark.memory.storageFraction=0.3",
        # S3A
        "--conf", "spark.hadoop.fs.s3a.experimental.input.fadvise=sequential",
        "--conf", "spark.hadoop.fs.s3a.readahead.range=256K",
        "--conf", "spark.hadoop.fs.s3a.block.size=32M",
        "--conf", "spark.hadoop.fs.s3a.connection.maximum=50",
        "--conf", "spark.hadoop.fs.s3a.threads.max=20",
        "--conf", "spark.hadoop.fs.s3a.fast.upload=true",
        "--conf", "spark.hadoop.fs.s3a.multipart.size=67108864",
        # Serializer + timeouts
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf", "spark.network.timeout=600s",
        "--conf", "spark.rpc.askTimeout=600s",
        "--conf", "spark.executor.heartbeatInterval=60s",
        # Entrypoint
        launcher,
    ]
    return cmd


def run_spark_etl(args, cfg, dataset, run_num):
    """Chạy spark-submit, trả về (t2_sec, returncode)."""
    log_dir = os.path.join(args.results_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{dataset}_run{run_num}.log")

    cmd = build_spark_cmd(args, cfg, dataset)

    env = os.environ.copy()
    env["MINIO_ENDPOINT"]       = cfg["minio_endpoint"]
    env["MINIO_ACCESS_KEY"]     = cfg["minio_access_key"]
    env["MINIO_SECRET_KEY"]     = cfg["minio_secret_key"]
    env["MINIO_BUCKET"]         = cfg["minio_bucket"]
    env["MINIO_SECURE"]         = "false"
    env["MINIO_PREFIX_CSV_RAW"] = f"DATA_SPLIT/{dataset}/"
    env["PG_HOST"]              = cfg["pg_host"]
    env["PG_PORT"]              = str(cfg["pg_port"])
    env["PG_DBNAME"]            = cfg["pg_dbname"]
    env["PG_USER"]              = cfg["pg_user"]
    env["PG_PASSWORD"]          = cfg["pg_password"]
    env["JDBC_URL"]             = (f"jdbc:postgresql://{cfg['pg_host']}:{cfg['pg_port']}"
                                   f"/{cfg['pg_dbname']}")
    env["JDBC_USER"]            = cfg["pg_user"]
    env["JDBC_PASSWORD"]        = cfg["pg_password"]
    env["DATA_BASE_PATH"]       = f"s3a://{cfg['minio_bucket']}"
    env["TOP_N_SYMBOLS"]        = "100"

    print(f"  [spark] Log: {log_path}")
    t_start = time.monotonic()
    with open(log_path, "w") as lf:
        proc = subprocess.run(cmd, env=env, stdout=lf, stderr=subprocess.STDOUT)
    t2 = time.monotonic() - t_start
    return t2, proc.returncode


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


def write_etl_summary(all_rows: list, results_dir):
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
    print(f"[summary] Da ghi {path}")


def print_cumulative_table(all_rows: list, runs: int):
    """In bảng tích lũy VERSION / RUN1..RUN5 / AVG sau mỗi dataset."""
    from collections import defaultdict
    groups = defaultdict(list)
    for r in all_rows:
        groups[r["dataset"]].append(r)

    col_w = 10
    headers = ["VERSION"] + [f"RUN{i}" for i in range(1, runs + 1)] + ["AVG"]
    sep = "-" * (10 + (runs + 1) * col_w + 2)

    print(f"\n{sep}")
    print("  " + "".join(f"{h:<{col_w}}" for h in headers))
    print(sep)

    for dataset in sorted(groups.keys(), key=_size_sort_key):
        rows = sorted(groups[dataset], key=lambda r: r["run_number"])
        run_vals = {r["run_number"]: r for r in rows}
        cells = [f"{dataset:<10}"]
        t2s_ok = []
        for i in range(1, runs + 1):
            r = run_vals.get(i)
            if r is None:
                cells.append(f"{'—':<{col_w}}")
            elif r["status"] == "error":
                cells.append(f"{'ERROR':<{col_w}}")
            else:
                val = float(r["t2_sec"])
                t2s_ok.append(val)
                cells.append(f"{val:.1f}s".ljust(col_w))
        avg_str = f"{statistics.mean(t2s_ok):.1f}s" if t2s_ok else "—"
        cells.append(avg_str)
        print("  " + "".join(cells))

    print(sep)


# -- Args -----------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Spark ETL benchmark T2 (MinIO CSV -> PostgreSQL).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--launcher",     required=True,
                   help="Path to spark_processing/jobs/launcher.py")
    p.add_argument("--control-ip",   required=True,
                   help="IP cua Control node (executor pods dial back to this)")
    p.add_argument("--spark-home",   default="/opt/spark")
    p.add_argument("--runs",         type=int, default=5)
    p.add_argument("--datasets",     nargs="*", default=None,
                   help="Specific datasets. Omit = auto-detect from MinIO")
    p.add_argument("--config",       default=None, help=".env file")
    p.add_argument("--results-dir",  default=None)
    p.add_argument("--k8s-namespace",   default="spark-etl")
    p.add_argument("--service-account", default="spark-job-sa")
    p.add_argument("--image",
                   default="huytrongbeou1310/bigdata_spark_postgresql:v1.5.0-csv-bench")
    p.add_argument("--executor-instances",       default="3")
    p.add_argument("--executor-memory",          default="2560m")
    p.add_argument("--executor-memory-overhead", default="512m")
    p.add_argument("--driver-memory",            default="2560m")
    p.add_argument("--driver-memory-overhead",   default="512m")
    p.add_argument("--executor-request-cores",   default="1000m")
    p.add_argument("--executor-limit-cores",     default="1800m")
    p.add_argument("--driver-port",              type=int, default=7078)
    p.add_argument("--block-manager-port",       type=int, default=7079)
    return p.parse_args()


# -- Main -----------------------------------------------------------------------

def main():
    args = parse_args()
    cfg  = load_cfg(args.config)

    global RESULTS_DIR
    if args.results_dir:
        RESULTS_DIR = os.path.abspath(args.results_dir)
    args.results_dir = RESULTS_DIR
    os.makedirs(RESULTS_DIR, exist_ok=True)

    if args.datasets:
        datasets = args.datasets
    else:
        print("[etl] Auto-detect datasets tu MinIO ...")
        datasets = detect_datasets_minio(cfg)
        if not datasets:
            print("Khong tim thay dataset nao tren MinIO (DATA_SPLIT/)")
            sys.exit(1)

    print(f"\n{'='*70}")
    print(f" Spark ETL Benchmark (T2)  --  {len(datasets)} dataset(s) x {args.runs} runs")
    print(f" Datasets : {datasets}")
    print(f" MinIO    : {cfg['minio_endpoint']}  bucket={cfg['minio_bucket']}")
    print(f" Postgres : {cfg['pg_host']}:{cfg['pg_port']}/{cfg['pg_dbname']}")
    print(f" Image    : {args.image}")
    print(f" Driver   : {args.control_ip}:{args.driver_port}")
    print(f"{'='*70}")

    all_rows = []

    for dataset in datasets:
        print(f"\n{'-'*70}")
        print(f" Dataset: {dataset}")
        print(f"{'-'*70}")

        for run_num in range(1, args.runs + 1):
            ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
            print(f"\n[{dataset}] Run {run_num}/{args.runs}  {ts}")

            try:
                truncate_postgres(cfg)
            except Exception as e:
                print(f"  [pg] WARNING truncate failed: {e}")

            status = "ok"
            notes  = ""
            t2     = 0.0
            rc     = -1

            try:
                t2, rc = run_spark_etl(args, cfg, dataset, run_num)
                if rc == 0:
                    print(f"  -> T2 = {t2:.2f}s  (exit 0)")
                else:
                    status = "error"
                    notes  = f"exit code {rc}"
                    print(f"  -> FAILED: exit code {rc}  T2={t2:.2f}s")
            except Exception as e:
                status = "error"
                notes  = str(e)
                print(f"  -> ERROR: {e}")

            row = {
                "dataset":       dataset,
                "run_number":    run_num,
                "timestamp_iso": ts,
                "t2_sec":        f"{t2:.2f}",
                "status":        status,
                "returncode":    rc,
                "notes":         notes,
            }
            append_etl_run(row, RESULTS_DIR)
            all_rows.append(row)

        print_cumulative_table(all_rows, args.runs)

    write_etl_summary(all_rows, RESULTS_DIR)
    print(f"\n[done] Ket qua: {RESULTS_DIR}/etl_runs.csv  va  etl_summary.csv")


if __name__ == "__main__":
    main()
