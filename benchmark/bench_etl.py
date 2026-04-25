#!/usr/bin/env python3
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", write_through=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", write_through=True)
"""
Benchmark T2: Do thoi gian Spark ETL (MinIO -> PostgreSQL) cho tung dataset size.

Quy trinh moi run:
  1. TRUNCATE 5 Postgres tables (clean truoc moi run)
  2. spark-submit launcher.py voi MINIO_PREFIX_CSV_RAW=DATA_SPLIT/<size>/
  3. Ghi log T2 (time.monotonic)
  4. In bang tich luy sau moi dataset

Ngoai le: Run thu 5 cua dataset CAO NHAT -> KHONG truncate sau khi chay xong,
          giu nguyen du lieu trong Postgres.

Chay (tren Control Node):
  python bench_etl.py \\
      --launcher /path/to/launcher.py \\
      --spark-home /opt/spark \\
      --control-ip 100.x.x.x \\
      --runs 5 \\
      [--datasets 100MB 250MB 500MB]   # bo qua de auto-detect tu MinIO

Env vars can thiet:
  MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
  PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD

Ket qua:
  benchmark/results/etl_runs.csv
  benchmark/results/etl_summary.csv
  benchmark/results/logs/<dataset>_<run_id>.log
"""
import argparse
import csv
import datetime
import os
import re
import statistics
import subprocess
import time

import boto3
import psycopg2
from botocore.config import Config
from dotenv import load_dotenv

BENCH_DIR   = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BENCH_DIR, "results")

_PG_TABLES = [
    "dim_symbols",
    "fact_klines",
    "fact_ticker_24h_snapshots",
    "mart_trade_metrics",
    "mart_top_coins",
]

RUNS_COLS = [
    "dataset", "run_number", "timestamp_iso",
    "t2_sec", "status", "returncode", "notes",
]
SUMMARY_COLS = [
    "dataset", "runs_ok",
    "avg_t2_sec", "min_t2_sec", "max_t2_sec", "std_t2_sec",
]
RUN_LABELS = ["FIRST_RUN", "SECOND_RUN", "THIRD_RUN", "FOURTH_RUN", "FIFTH_RUN"]


# -- Helpers -------------------------------------------------------------------

def _size_sort_key(name):
    m = re.match(r"(\d+)(MB|GB)", name.upper())
    if not m:
        return (2, 0, name)
    val, unit = int(m.group(1)), m.group(2)
    return (1, val if unit == "MB" else val * 1024, name)


def _fmt_t(t_sec):
    return f"{t_sec:.1f}s"


# -- PostgreSQL ----------------------------------------------------------------

def truncate_postgres(cfg):
    tables_sql = ", ".join(_PG_TABLES)
    sql = f"TRUNCATE TABLE {tables_sql} RESTART IDENTITY CASCADE"
    conn = psycopg2.connect(
        host=cfg["pg_host"], port=cfg["pg_port"],
        dbname=cfg["pg_dbname"], user=cfg["pg_user"], password=cfg["pg_password"],
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.close()
    print(f"  [pg] TRUNCATED {len(_PG_TABLES)} tables")


# -- MinIO dataset detection ---------------------------------------------------

def detect_datasets_minio(cfg):
    """List dataset folders under DATA_SPLIT/ on MinIO that have objects."""
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{cfg['minio_endpoint']}",
        aws_access_key_id=cfg["minio_access_key"],
        aws_secret_access_key=cfg["minio_secret_key"],
        config=Config(signature_version="s3v4"),
    )
    bucket = cfg["minio_bucket"]
    paginator = s3.get_paginator("list_objects_v2")
    names = []
    for page in paginator.paginate(Bucket=bucket, Prefix="DATA_SPLIT/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            part = cp["Prefix"].rstrip("/").split("/")[-1]
            names.append(part)
    return sorted(names, key=_size_sort_key)


# -- Spark ETL -----------------------------------------------------------------

def run_spark_etl(cfg, dataset, args, run_id):
    """
    spark-submit launcher.py for the given dataset.
    Returns (t2_sec, returncode).
    """
    prefix = f"DATA_SPLIT/{dataset}/"
    spark_submit = os.path.join(args.spark_home, "bin", "spark-submit")
    pod_template = os.path.join(BENCH_DIR, "templates", "executor-pod-template.yaml")

    env = os.environ.copy()
    # Override dataset prefix for driver and executors
    env["MINIO_PREFIX_CSV_RAW"] = prefix
    # Connection info for driver process
    env["MINIO_ENDPOINT"]   = cfg["minio_endpoint"]
    env["MINIO_ACCESS_KEY"] = cfg["minio_access_key"]
    env["MINIO_SECRET_KEY"] = cfg["minio_secret_key"]
    env["MINIO_BUCKET"]     = cfg["minio_bucket"]
    env["PG_HOST"]          = cfg["pg_host"]
    env["PG_PORT"]          = str(cfg["pg_port"])
    env["PG_DBNAME"]        = cfg["pg_dbname"]
    env["PG_USER"]          = cfg["pg_user"]
    env["PG_PASSWORD"]      = cfg["pg_password"]
    # execute_sql() runs on driver JVM (Control Node) — must use NodePort URL, not cluster DNS.
    # Executor pods get JDBC_URL from ConfigMap envFrom (cluster DNS) which overrides this.
    env["JDBC_URL"]      = f"jdbc:postgresql://{cfg['pg_host']}:{cfg['pg_port']}/{cfg['pg_dbname']}"
    env["JDBC_USER"]     = cfg["pg_user"]
    env["JDBC_PASSWORD"] = cfg["pg_password"]

    cmd = [
        spark_submit,
        "--master",      f"k8s://https://127.0.0.1:6443",
        "--deploy-mode", "client",
        "--name",        f"CryptoDW-ETL-Bench-{dataset}-{run_id}",
        "--conf", f"spark.kubernetes.namespace={args.k8s_namespace}",
        "--conf", f"spark.kubernetes.authenticate.driver.serviceAccountName={args.service_account}",
        "--conf", f"spark.kubernetes.container.image={args.image}",
        "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
        "--conf", f"spark.kubernetes.executor.podTemplateFile={pod_template}",
        "--conf", f"spark.driver.host={args.control_ip}",
        "--conf", f"spark.driver.port={args.driver_port}",
        "--conf", f"spark.driver.blockManager.port={args.block_manager_port}",
        "--conf", "spark.driver.bindAddress=0.0.0.0",
        "--conf", f"spark.executor.instances={args.executor_instances}",
        "--conf", "spark.executor.cores=1",
        "--conf", f"spark.kubernetes.executor.request.cores={args.executor_request_cores}",
        "--conf", f"spark.kubernetes.executor.limit.cores={args.executor_limit_cores}",
        "--conf", f"spark.executor.memory={args.executor_memory}",
        "--conf", f"spark.executor.memoryOverhead={args.executor_memory_overhead}",
        "--driver-memory", args.driver_memory,
        "--conf", f"spark.driver.memoryOverhead={args.driver_memory_overhead}",
        # Node placement: driver on control, executors on workers
        "--conf", "spark.kubernetes.driver.node.selector.spark-role=driver",
        "--conf", "spark.kubernetes.executor.node.selector.spark-role=worker",
        # Dataset override for executor pods (driver reads from env directly)
        "--conf", f"spark.executorEnv.MINIO_PREFIX_CSV_RAW={prefix}",
        # SQL tuning — 6 = 2x total cores (3 executors x 1 core), not 48
        "--conf", "spark.sql.shuffle.partitions=6",
        "--conf", "spark.default.parallelism=6",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
        "--conf", "spark.sql.adaptive.advisoryPartitionSizeInBytes=64m",
        "--conf", "spark.sql.adaptive.skewJoin.enabled=true",
        "--conf", "spark.sql.files.maxPartitionBytes=67108864",
        "--conf", "spark.sql.files.openCostInBytes=4194304",
        "--conf", "spark.memory.fraction=0.7",
        "--conf", "spark.memory.storageFraction=0.3",
        # S3A tuning
        "--conf", "spark.hadoop.fs.s3a.experimental.input.fadvise=sequential",
        "--conf", "spark.hadoop.fs.s3a.readahead.range=262144",
        "--conf", "spark.hadoop.fs.s3a.block.size=33554432",
        "--conf", "spark.hadoop.fs.s3a.connection.maximum=50",
        "--conf", "spark.hadoop.fs.s3a.threads.max=20",
        "--conf", "spark.hadoop.fs.s3a.fast.upload=true",
        "--conf", "spark.hadoop.fs.s3a.multipart.size=67108864",
        # Serialization + timeouts
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf", "spark.network.timeout=600s",
        "--conf", "spark.rpc.askTimeout=600s",
        "--conf", "spark.executor.heartbeatInterval=60s",
        "--jars", os.path.join(args.spark_home, "jars", "postgresql-42.7.3.jar"),
        args.launcher,
    ]

    os.makedirs(os.path.join(RESULTS_DIR, "logs"), exist_ok=True)
    log_path = os.path.join(RESULTS_DIR, "logs", f"{dataset}_{run_id}.log")
    print(f"  [spark] spark-submit -> {dataset}  (log: {log_path})")

    t_start = time.monotonic()
    with open(log_path, "w") as lf:
        result = subprocess.run(cmd, env=env, stdout=lf, stderr=subprocess.STDOUT)
    t2 = time.monotonic() - t_start

    return t2, result.returncode


# -- CSV logging ---------------------------------------------------------------

def append_run(row: dict):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, "etl_runs.csv")
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RUNS_COLS)
        if write_header:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in RUNS_COLS})


def write_summary(all_rows: list):
    from collections import defaultdict
    groups = defaultdict(list)
    for r in all_rows:
        if r["status"] == "ok":
            groups[r["dataset"]].append(r)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, "etl_summary.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_COLS)
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
    print(f"\n[summary] Written: {path}")


# -- Cumulative table ----------------------------------------------------------

def print_cumulative_table(all_rows: list):
    """Print cumulative result table for all completed datasets."""
    from collections import defaultdict
    groups = defaultdict(list)
    for r in all_rows:
        groups[r["dataset"]].append(r)

    col_w = 12
    header = f"{'VERSION':<10}"
    for lbl in RUN_LABELS:
        header += f"  {lbl:>{col_w}}"
    header += f"  {'AVG_TIME':>{col_w}}"
    sep = "-" * len(header)

    print(f"\n{sep}")
    print(header)
    print(sep)

    for dataset, rows in sorted(groups.items(), key=lambda x: _size_sort_key(x[0])):
        ok_rows = [r for r in rows if r["status"] == "ok"]
        all_run_rows = sorted(rows, key=lambda r: int(r["run_number"]))

        line = f"{dataset:<10}"
        for i in range(5):
            if i < len(all_run_rows):
                r = all_run_rows[i]
                if r["status"] == "ok":
                    val = f"{float(r['t2_sec']):.1f}s"
                else:
                    val = "FAILED"
                line += f"  {val:>{col_w}}"
            else:
                line += f"  {'---':>{col_w}}"

        if ok_rows:
            avg = statistics.mean(float(r["t2_sec"]) for r in ok_rows)
            line += f"  {_fmt_t(avg):>{col_w}}"
        else:
            line += f"  {'N/A':>{col_w}}"
        print(line)

    print(sep)


# -- Config + CLI --------------------------------------------------------------

def load_cfg(env_file=None):
    if env_file:
        load_dotenv(env_file)
    return {
        "minio_endpoint":   os.environ["MINIO_ENDPOINT"],
        "minio_access_key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        "minio_secret_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        "minio_bucket":     os.environ.get("MINIO_BUCKET", "binance"),
        "pg_host":          os.environ["PG_HOST"],
        "pg_port":          int(os.environ.get("PG_PORT", "5432")),
        "pg_dbname":        os.environ.get("PG_DBNAME", "cryptodw"),
        "pg_user":          os.environ.get("PG_USER", "postgres"),
        "pg_password":      os.environ["PG_PASSWORD"],
    }


def parse_args():
    p = argparse.ArgumentParser(
        description="ETL benchmark (T2): spark-submit launcher.py -> Postgres, N runs per dataset.\n"
                    "TRUNCATE Postgres before every run.\n"
                    "Exception: last run of highest dataset keeps Postgres data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--launcher",               required=True,
                   help="Absolute path to launcher.py")
    p.add_argument("--spark-home",             default="/opt/spark",
                   help="SPARK_HOME directory (default: /opt/spark)")
    p.add_argument("--control-ip",             required=True,
                   help="Driver IP reachable from executor pods (Tailscale IP)")
    p.add_argument("--runs",                   type=int, default=5)
    p.add_argument("--datasets",               nargs="*", default=None,
                   help="Dataset names e.g. 100MB 250MB. Omit = auto-detect from MinIO.")
    p.add_argument("--config",                 default=None,
                   help=".env file with MINIO_* and PG_* vars")
    p.add_argument("--results-dir",            default=None,
                   help="Output folder (default: benchmark/results/)")
    p.add_argument("--k8s-namespace",          default="spark-etl")
    p.add_argument("--service-account",        default="spark-job-sa")
    p.add_argument("--image",
                   default="huytrongbeou1310/bigdata_spark_postgresql:v1.5.0-csv-bench")
    p.add_argument("--executor-instances",     type=int, default=3)
    p.add_argument("--executor-request-cores", default="1000m")
    p.add_argument("--executor-limit-cores",   default="1400m")
    # 1792m+512m=2304MB — requires Postgres on Control (workers then have ~2846MB free).
    # If Postgres still on a worker, lower to 1280m+384m=1664MB to avoid OOMKill on that node.
    p.add_argument("--executor-memory",        default="1792m")
    p.add_argument("--executor-memory-overhead", default="512m")
    p.add_argument("--driver-memory",          default="2048m")  # Control Node 8.7GB, conservative
    p.add_argument("--driver-memory-overhead", default="512m")
    p.add_argument("--driver-port",            type=int, default=7078)
    p.add_argument("--block-manager-port",     type=int, default=7079)
    return p.parse_args()


# -- Main ----------------------------------------------------------------------

def main():
    args = parse_args()
    cfg  = load_cfg(args.config)

    global RESULTS_DIR
    if args.results_dir:
        RESULTS_DIR = os.path.abspath(args.results_dir)
    os.makedirs(RESULTS_DIR, exist_ok=True)

    if args.datasets:
        datasets = sorted(args.datasets, key=_size_sort_key)
    else:
        datasets = detect_datasets_minio(cfg)

    if not datasets:
        print("[error] No datasets found. Upload data first or specify --datasets.")
        sys.exit(1)

    last_dataset = datasets[-1]

    print(f"\n{'='*70}")
    print(f" ETL Benchmark (T2)  --  {len(datasets)} dataset(s)  x  {args.runs} runs")
    print(f" Datasets : {datasets}")
    print(f" Highest  : {last_dataset}  (Postgres kept after run {args.runs})")
    print(f" MinIO    : {cfg['minio_endpoint']}  bucket={cfg['minio_bucket']}")
    print(f" Postgres : {cfg['pg_host']}:{cfg['pg_port']}/{cfg['pg_dbname']}")
    print(f" Image    : {args.image}")
    print(f"{'='*70}")

    all_rows = []

    for dataset in datasets:
        is_last_dataset = (dataset == last_dataset)
        print(f"\n{'-'*70}")
        print(f" Dataset: {dataset}"
              + (f"  [HIGHEST - Postgres kept after run {args.runs}]" if is_last_dataset else ""))
        print(f"{'-'*70}")

        for run_num in range(1, args.runs + 1):
            is_last_run     = (run_num == args.runs)
            is_keep_pg_run  = is_last_run and is_last_dataset

            ts     = datetime.datetime.now(datetime.timezone.utc).isoformat()
            run_id = f"{dataset}-r{run_num}"

            print(f"\n[{dataset}] Run {run_num}/{args.runs}  {ts}"
                  + ("  [KEEP PG]" if is_keep_pg_run else ""))

            # Always truncate before run (ensures clean state for measurement)
            try:
                truncate_postgres(cfg)
            except Exception as e:
                print(f"  [pg] WARNING truncate failed: {e}")

            status     = "ok"
            notes      = ""
            t2         = 0.0
            returncode = 0

            try:
                t2, returncode = run_spark_etl(cfg, dataset, args, run_id)
                if returncode != 0:
                    status = "error"
                    notes  = f"spark-submit exit {returncode}"
                    print(f"  -> FAILED (exit {returncode})  T2={t2:.2f}s")
                else:
                    print(f"  -> T2 = {t2:.2f}s")
            except Exception as e:
                status     = "error"
                notes      = str(e)
                returncode = -1
                print(f"  -> ERROR: {e}")

            row = {
                "dataset":       dataset,
                "run_number":    run_num,
                "timestamp_iso": ts,
                "t2_sec":        f"{t2:.3f}",
                "status":        status,
                "returncode":    returncode,
                "notes":         notes,
            }
            append_run(row)
            all_rows.append(row)

            if is_keep_pg_run:
                print(f"  [keep] Postgres data preserved (last run of highest dataset).")

        # Print cumulative table after every dataset completes
        print_cumulative_table(all_rows)

    write_summary(all_rows)
    print(f"\n[done] Results in {RESULTS_DIR}/etl_runs.csv + etl_summary.csv")


if __name__ == "__main__":
    main()
