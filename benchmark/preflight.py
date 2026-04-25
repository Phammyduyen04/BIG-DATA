"""
Preflight checks before each benchmark run.
Verifies: K8s nodes Ready, no stale executor pods, MinIO reachable, Postgres reachable.
"""
import subprocess
import sys


def check_k8s_nodes():
    result = subprocess.run(
        ["kubectl", "get", "nodes", "--no-headers"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        return False, f"kubectl get nodes failed: {result.stderr.strip()}"
    lines = result.stdout.strip().splitlines()
    not_ready = [l for l in lines if "NotReady" in l]
    if not_ready:
        return False, f"Nodes not ready: {not_ready}"
    return True, f"{len(lines)} node(s) Ready"


def check_stale_executors(namespace="spark-etl"):
    result = subprocess.run(
        ["kubectl", "get", "pods", "-n", namespace, "--no-headers",
         "-l", "spark-role=executor"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        return True, f"Could not list pods: {result.stderr.strip()}"
    lines = [l for l in result.stdout.strip().splitlines() if l]
    if lines:
        return False, f"Stale executor pods found ({len(lines)}): {lines}"
    return True, "No stale executor pods"


def check_minio(endpoint, access_key, secret_key, bucket):
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        s3.head_bucket(Bucket=bucket)
        return True, f"MinIO bucket '{bucket}' reachable"
    except (BotoCoreError, ClientError, Exception) as e:
        return False, f"MinIO check failed: {e}"


def check_postgres(host, port, dbname, user, password):
    import psycopg2
    try:
        conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password,
            connect_timeout=5
        )
        conn.close()
        return True, f"Postgres {host}:{port}/{dbname} reachable"
    except Exception as e:
        return False, f"Postgres check failed: {e}"


def run_all(cfg, abort_on_failure=True):
    """Run all preflight checks. cfg is the benchmark config dict."""
    checks = [
        ("K8s nodes",       lambda: check_k8s_nodes()),
        ("Stale executors", lambda: check_stale_executors(cfg.get("k8s_namespace", "spark-etl"))),
        ("MinIO",           lambda: check_minio(
                                cfg["minio_endpoint"], cfg["minio_access_key"],
                                cfg["minio_secret_key"], cfg["minio_bucket"])),
        ("Postgres",        lambda: check_postgres(
                                cfg["pg_host"], cfg["pg_port"], cfg["pg_dbname"],
                                cfg["pg_user"], cfg["pg_password"])),
    ]

    all_ok = True
    print("[preflight] Running checks ...")
    for name, fn in checks:
        ok, msg = fn()
        status = "OK" if ok else "FAIL"
        print(f"[preflight]   [{status}] {name}: {msg}")
        if not ok:
            all_ok = False

    if not all_ok and abort_on_failure:
        print("[preflight] ABORT: preflight failed.")
        sys.exit(1)

    return all_ok
