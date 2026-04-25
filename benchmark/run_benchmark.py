#!/usr/bin/env python3
"""
Benchmark harness: compare MinIO+Spark vs Hadoop+MapReduce.

Usage:
  python run_benchmark.py --dataset 12GB --runs 5 \
      --data-root /home/duyen/DATA_SPLIT \
      [--config benchmark.env] [--on-failure abort|continue]

Measures:
  T1 = upload local CSV → MinIO (time.monotonic)
  T2 = spark-submit ETL → Postgres (time.monotonic)
  T3 = T1 + T2

Outputs:
  benchmark/results/runs.csv      (one row per run)
  benchmark/results/summary.csv   (stats across all runs)
"""
import argparse
import datetime
import os
import sys
import uuid

# Ensure benchmark/ is on path
BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BENCH_DIR)

import preflight
import cleanup
import upload_to_minio
import trigger_spark
import verify
import log_results


def load_config(env_file=None):
    """Load config from env file or environment variables."""
    if env_file and os.path.exists(env_file):
        from dotenv import load_dotenv
        load_dotenv(env_file)

    cfg = {
        # MinIO
        "minio_endpoint":   os.environ["MINIO_ENDPOINT"],
        "minio_access_key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        "minio_secret_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        "minio_bucket":     os.environ.get("MINIO_BUCKET", "binance"),
        "minio_prefix_base": "DATA_SPLIT",
        # Postgres (via NodePort)
        "pg_host":     os.environ.get("PG_HOST", "localhost"),
        "pg_port":     int(os.environ.get("PG_PORT", 30432)),
        "pg_dbname":   os.environ.get("PG_DBNAME", "crypto_dw"),
        "pg_user":     os.environ.get("PG_USER", "postgres"),
        "pg_password": os.environ["PG_PASSWORD"],
        # Spark client mode
        "control_ip":    os.environ["CONTROL_IP"],
        "spark_home":    os.environ.get("SPARK_HOME", "/opt/spark"),
        "k8s_master":    os.environ.get("K8S_MASTER", "k8s://https://127.0.0.1:6443"),
        "spark_image":   os.environ.get("SPARK_IMAGE",
                             "huytrongbeou1310/bigdata_spark_postgresql:v1.5.0-csv-bench"),
        "k8s_namespace": os.environ.get("K8S_NAMESPACE", "spark-etl"),
        "k8s_sa":        os.environ.get("K8S_SA", "spark-job-sa"),
    }
    return cfg


def parse_args():
    p = argparse.ArgumentParser(description="CryptoDW benchmark harness")
    p.add_argument("--dataset",    required=True, help="Dataset name, e.g. 12GB")
    p.add_argument("--runs",       type=int, default=5, help="Number of runs")
    p.add_argument("--data-root",  required=True, help="Local path to DATA_SPLIT/")
    p.add_argument("--config",     default=None,  help="Path to .env config file")
    p.add_argument("--on-failure", choices=["abort", "continue"], default="abort")
    return p.parse_args()


def main():
    args = parse_args()
    cfg = load_config(args.config)

    results_dir = os.path.join(BENCH_DIR, "results")
    log_dir     = os.path.join(results_dir, "logs")
    os.makedirs(results_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f" CryptoDW Benchmark — dataset={args.dataset}  runs={args.runs}")
    print(f"{'='*60}")

    # Preflight (once, before all runs)
    preflight.run_all(cfg, abort_on_failure=True)

    for run_num in range(1, args.runs + 1):
        run_id = f"{args.dataset}-run{run_num:02d}-{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
        print(f"\n{'─'*60}")
        print(f" Run {run_num}/{args.runs}  run_id={run_id}")
        print(f"{'─'*60}")

        # Cleanup (reset state between runs)
        cleanup.run(cfg, args.dataset)

        # T1: upload
        t1, n_files, n_bytes = upload_to_minio.run(
            cfg, args.dataset, args.data_root
        )

        # T2: Spark ETL
        t2, returncode, log_path = trigger_spark.run(cfg, run_id, BENCH_DIR, log_dir)

        t3 = t1 + t2
        timestamp = datetime.datetime.utcnow().isoformat()

        if returncode != 0:
            note = f"spark_failed_rc={returncode}"
            verify_pass = False
            print(f"[run] FAILED (returncode={returncode}). Log: {log_path}")
        else:
            verify_pass, counts, note = verify.run(cfg, args.dataset, results_dir)

        row = {
            "dataset":          args.dataset,
            "run_number":       run_num,
            "run_id":           run_id,
            "timestamp_iso":    timestamp,
            "minio_upload_sec": f"{t1:.2f}",
            "spark_etl_sec":    f"{t2:.2f}",
            "total_sec":        f"{t3:.2f}",
            "files":            n_files,
            "bytes":            n_bytes,
            "verify_pass":      "true" if verify_pass else "false",
            "returncode":       returncode,
            "notes":            note,
        }
        log_results.append_run(results_dir, row)

        print(f"[run] T1={t1:.1f}s  T2={t2:.1f}s  T3={t3:.1f}s  "
              f"verify={'PASS' if verify_pass else 'FAIL'}")

        if not verify_pass and args.on_failure == "abort":
            print(f"[run] Aborting on failure (--on-failure=abort).")
            break

    # Summary
    log_results.write_summary(results_dir, args.dataset)
    print(f"\n[benchmark] All runs complete. Results in {results_dir}/")


if __name__ == "__main__":
    main()
