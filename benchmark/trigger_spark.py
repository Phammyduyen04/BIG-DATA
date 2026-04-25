"""
T2: Run Spark ETL via spark-submit (client mode) using the shell template.
Returns (t2_seconds, returncode, log_path).
"""
import os
import subprocess
import string
import time


def _render_template(tpl_path, substitutions):
    with open(tpl_path) as f:
        tpl = f.read()
    return string.Template(tpl).substitute(substitutions)


def run(cfg, run_id, bench_dir, log_dir):
    """
    Render spark_submit_cmd.sh.tpl, write to a temp script, execute it.
    stdout+stderr are tee'd to log_dir/<run_id>.driver.log.
    Returns (t2_seconds: float, returncode: int, log_path: str).
    """
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{run_id}.driver.log")

    tpl_path = os.path.join(bench_dir, "templates", "spark_submit_cmd.sh.tpl")
    subs = {
        "RUN_ID":    run_id,
        "BENCH_DIR": bench_dir,
        "CONTROL_IP": cfg["control_ip"],
        "SPARK_HOME": cfg.get("spark_home", "/opt/spark"),
        "K8S_MASTER": cfg.get("k8s_master", "k8s://https://127.0.0.1:6443"),
        "SPARK_IMAGE": cfg.get("spark_image",
                               "huytrongbeou1310/bigdata_spark_postgresql:v1.5.0-csv-bench"),
        "K8S_NAMESPACE": cfg.get("k8s_namespace", "spark-etl"),
        "SA_NAME": cfg.get("k8s_sa", "spark-job-sa"),
        "LAUNCHER_PY": os.path.join(
            os.path.dirname(bench_dir), "spark_processing", "jobs", "launcher.py"
        ),
    }

    script_content = _render_template(tpl_path, subs)
    script_path = os.path.join(log_dir, f"{run_id}.submit.sh")
    with open(script_path, "w") as f:
        f.write(script_content)
    os.chmod(script_path, 0o755)

    print(f"[trigger] Launching spark-submit (run_id={run_id}) ...")
    print(f"[trigger]   Log: {log_path}")

    t_start = time.monotonic()
    with open(log_path, "w") as logf:
        proc = subprocess.run(
            ["bash", script_path],
            stdout=logf,
            stderr=subprocess.STDOUT,
        )
    t2 = time.monotonic() - t_start

    print(f"[trigger] T2 = {t2:.2f}s  (returncode={proc.returncode})")
    return t2, proc.returncode, log_path
