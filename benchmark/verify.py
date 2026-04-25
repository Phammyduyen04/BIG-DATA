"""
Post-run verification: SELECT COUNT(*) from 4 output tables.
On first run (no baseline), saves counts as baseline_<dataset>.json.
Subsequent runs compare against baseline (tolerance ±0.01%).
"""
import json
import os

import psycopg2


_TABLES = [
    "dim_symbols",
    "fact_klines",
    "fact_ticker_24h_snapshots",
    "mart_trade_metrics",
]

_TOLERANCE = 0.0001  # ±0.01%


def get_counts(host, port, dbname, user, password):
    conn = psycopg2.connect(host=host, port=port, dbname=dbname,
                            user=user, password=password)
    counts = {}
    with conn.cursor() as cur:
        for table in _TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0]
    conn.close()
    return counts


def run(cfg, dataset, results_dir):
    """
    Returns (verify_pass: bool, counts: dict, notes: str).
    """
    baseline_path = os.path.join(results_dir, f"baseline_{dataset}.json")

    counts = get_counts(
        cfg["pg_host"], cfg["pg_port"], cfg["pg_dbname"],
        cfg["pg_user"], cfg["pg_password"]
    )
    print(f"[verify] Row counts: {counts}")

    if not os.path.exists(baseline_path):
        with open(baseline_path, "w") as f:
            json.dump(counts, f, indent=2)
        print(f"[verify] Baseline saved: {baseline_path}")
        return True, counts, "baseline_created"

    with open(baseline_path) as f:
        baseline = json.load(f)

    mismatches = []
    for table, expected in baseline.items():
        actual = counts.get(table, 0)
        if expected == 0:
            if actual != 0:
                mismatches.append(f"{table}: expected 0, got {actual}")
        else:
            diff = abs(actual - expected) / expected
            if diff > _TOLERANCE:
                mismatches.append(
                    f"{table}: expected {expected}, got {actual} (diff={diff:.4%})"
                )

    if mismatches:
        notes = "; ".join(mismatches)
        print(f"[verify] FAIL: {notes}")
        return False, counts, notes

    print(f"[verify] PASS: all counts within ±{_TOLERANCE:.2%} of baseline")
    return True, counts, "ok"
