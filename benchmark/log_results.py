"""
Append run result to runs.csv.
After all runs, compute and write summary.csv.
"""
import csv
import os
import statistics


_RUNS_COLS = [
    "dataset", "run_number", "run_id", "timestamp_iso",
    "minio_upload_sec", "spark_etl_sec", "total_sec",
    "files", "bytes", "verify_pass", "returncode", "notes",
]

_SUMMARY_COLS = [
    "dataset", "runs",
    "avg_minio_sec", "avg_spark_sec", "avg_total_sec",
    "std_minio", "std_spark", "std_total",
    "throughput_mb_s",
]


def append_run(results_dir, row: dict):
    os.makedirs(results_dir, exist_ok=True)
    runs_path = os.path.join(results_dir, "runs.csv")
    write_header = not os.path.exists(runs_path)
    with open(runs_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_RUNS_COLS)
        if write_header:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in _RUNS_COLS})
    print(f"[log] Appended run {row.get('run_number')} to {runs_path}")


def write_summary(results_dir, dataset):
    runs_path = os.path.join(results_dir, "runs.csv")
    if not os.path.exists(runs_path):
        return

    rows = []
    with open(runs_path) as f:
        reader = csv.DictReader(f)
        rows = [r for r in reader if r["dataset"] == dataset and r["returncode"] == "0"]

    if not rows:
        print(f"[log] No successful runs for {dataset} — skipping summary")
        return

    minio_times = [float(r["minio_upload_sec"]) for r in rows]
    spark_times  = [float(r["spark_etl_sec"])    for r in rows]
    total_times  = [float(r["total_sec"])         for r in rows]
    total_bytes  = float(rows[0]["bytes"]) if rows else 0

    std = lambda lst: statistics.stdev(lst) if len(lst) > 1 else 0.0
    avg_total = statistics.mean(total_times)
    throughput = (total_bytes / 1e6) / avg_total if avg_total > 0 else 0

    summary_row = {
        "dataset":       dataset,
        "runs":          len(rows),
        "avg_minio_sec": f"{statistics.mean(minio_times):.2f}",
        "avg_spark_sec": f"{statistics.mean(spark_times):.2f}",
        "avg_total_sec": f"{avg_total:.2f}",
        "std_minio":     f"{std(minio_times):.2f}",
        "std_spark":     f"{std(spark_times):.2f}",
        "std_total":     f"{std(total_times):.2f}",
        "throughput_mb_s": f"{throughput:.1f}",
    }

    summary_path = os.path.join(results_dir, "summary.csv")
    existing = []
    if os.path.exists(summary_path):
        with open(summary_path) as f:
            existing = [r for r in csv.DictReader(f) if r["dataset"] != dataset]

    with open(summary_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_SUMMARY_COLS)
        writer.writeheader()
        for r in existing:
            writer.writerow(r)
        writer.writerow(summary_row)

    print(f"[log] Summary written to {summary_path}")
    print(f"[log]   avg_total={summary_row['avg_total_sec']}s  "
          f"throughput={summary_row['throughput_mb_s']} MB/s  "
          f"std_total={summary_row['std_total']}s")
