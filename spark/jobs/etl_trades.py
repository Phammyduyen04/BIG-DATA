"""
ETL: fact_raw_trades  +  mart_trade_metrics  +  mart_whale_alerts

Nguồn: trades/{SYMBOL}/{SYMBOL}-trades-{date}.csv
Cột CSV: trade_id, price, qty, quote_qty, time, is_buyer_maker, is_best_match

Thiết kế:
  - Staging table được tạo trước bằng execute_sql (tránh race condition JDBC)
  - Spark chỉ INSERT (mode="append") — không làm DDL
  - Không cache 9.7M rows trong Spark memory
  - Whale analysis chạy hoàn toàn trong Postgres (reuse staging table)

Luồng:
  [1] execute_sql: DROP + CREATE staging_fact_raw_trades
  [2] Spark write.jdbc(mode="append") → staging
  [3] execute_sql: upsert staging → fact_raw_trades
  [4] execute_sql: TRUNCATE + INSERT mart_trade_metrics  (từ staging, CTE)
  [5] execute_sql: TRUNCATE + INSERT mart_whale_alerts   (từ staging, CTE)
  [6] execute_sql: DROP staging

Whale threshold per symbol = GREATEST(avg + 2σ, avg × 3)
  LARGE : quote_qty > threshold  (avg + 2σ)
  WHALE : quote_qty > avg + 3σ
  MEGA  : quote_qty > avg + 4σ
"""
import os

from pyspark.sql import functions as F

from etl_utils import execute_sql, load_symbol_map

_FNAME_PREFIX = "-trades-"

_WRITE_OPTS = {
    "batchsize":     "50000",
    "numPartitions": "4",
}

# ── SQL: tạo staging (schema khớp chính xác với DataFrame) ───────────
_SQL_DROP_STAGING = "DROP TABLE IF EXISTS staging_fact_raw_trades"

_SQL_CREATE_STAGING = """
    CREATE TABLE staging_fact_raw_trades (
        symbol_id      BIGINT,
        trade_id       BIGINT,
        trade_time     TIMESTAMP,
        price          DECIMAL(30,12),
        qty_base       DECIMAL(30,12),
        quote_qty      DECIMAL(30,12),
        is_buyer_maker BOOLEAN,
        is_best_match  BOOLEAN,
        ingested_at    TIMESTAMP
    )
"""

# ── SQL: upsert staging → fact_raw_trades ─────────────────────────────
_SQL_UPSERT_TRADES = """
    INSERT INTO fact_raw_trades (
        symbol_id, trade_id, trade_time, price, qty_base,
        quote_qty, is_buyer_maker, is_best_match, ingested_at
    )
    SELECT
        symbol_id, trade_id, trade_time, price, qty_base,
        quote_qty, is_buyer_maker, is_best_match, ingested_at
    FROM staging_fact_raw_trades
    ON CONFLICT (symbol_id, trade_id) DO NOTHING
"""

# ── SQL: mart_trade_metrics (1 scan staging) ──────────────────────────
# whale_threshold = GREATEST(avg + 2σ,  avg × 3)
# money_flow      = total_buy_volume - total_sell_volume
# whale_buy_ratio = whale_buy_vol / (whale_buy_vol + whale_sell_vol)
_SQL_TRUNCATE_METRICS = "TRUNCATE mart_trade_metrics"

_SQL_MART_METRICS = """
    INSERT INTO mart_trade_metrics (
        symbol_id,
        total_buy_volume, total_sell_volume, money_flow, total_trades,
        whale_threshold,
        whale_buy_volume, whale_sell_volume, whale_buy_ratio,
        computed_at
    )
    WITH sym_stats AS (
        SELECT
            symbol_id,
            AVG(quote_qty::numeric)                                    AS avg_qty,
            COALESCE(STDDEV(quote_qty::numeric), 0.0)                  AS std_qty,
            GREATEST(
                AVG(quote_qty::numeric) + 2.0 * COALESCE(STDDEV(quote_qty::numeric), 0.0),
                AVG(quote_qty::numeric) * 3.0
            )                                                          AS whale_threshold
        FROM staging_fact_raw_trades
        GROUP BY symbol_id
    )
    SELECT
        t.symbol_id,
        SUM(CASE WHEN NOT t.is_buyer_maker THEN t.quote_qty::numeric ELSE 0 END) AS total_buy_volume,
        SUM(CASE WHEN     t.is_buyer_maker THEN t.quote_qty::numeric ELSE 0 END) AS total_sell_volume,
        SUM(CASE WHEN NOT t.is_buyer_maker THEN t.quote_qty::numeric ELSE 0 END)
            - SUM(CASE WHEN t.is_buyer_maker THEN t.quote_qty::numeric ELSE 0 END) AS money_flow,
        COUNT(*)                                                       AS total_trades,
        s.whale_threshold,
        SUM(CASE WHEN t.quote_qty::numeric > s.whale_threshold AND NOT t.is_buyer_maker
                 THEN t.quote_qty::numeric ELSE 0 END)                 AS whale_buy_volume,
        SUM(CASE WHEN t.quote_qty::numeric > s.whale_threshold AND     t.is_buyer_maker
                 THEN t.quote_qty::numeric ELSE 0 END)                 AS whale_sell_volume,
        CASE
            WHEN SUM(CASE WHEN t.quote_qty::numeric > s.whale_threshold
                         THEN t.quote_qty::numeric ELSE 0 END) > 0
            THEN
                SUM(CASE WHEN t.quote_qty::numeric > s.whale_threshold AND NOT t.is_buyer_maker
                         THEN t.quote_qty::numeric ELSE 0 END)
                / NULLIF(SUM(CASE WHEN t.quote_qty::numeric > s.whale_threshold
                                  THEN t.quote_qty::numeric ELSE 0 END), 0)
            ELSE NULL
        END                                                            AS whale_buy_ratio,
        NOW()                                                          AS computed_at
    FROM staging_fact_raw_trades t
    JOIN sym_stats s USING (symbol_id)
    GROUP BY t.symbol_id, s.whale_threshold
"""

# ── SQL: mart_whale_alerts (1 scan staging) ───────────────────────────
_SQL_TRUNCATE_WHALE = "TRUNCATE mart_whale_alerts"

_SQL_MART_WHALE = """
    INSERT INTO mart_whale_alerts (
        symbol_id, trade_time, direction, price, quote_qty,
        size_multiplier, alert_level, computed_at
    )
    WITH sym_stats AS (
        SELECT
            symbol_id,
            AVG(quote_qty::numeric)                                    AS avg_qty,
            COALESCE(STDDEV(quote_qty::numeric), 0.0)                  AS std_qty,
            GREATEST(
                AVG(quote_qty::numeric) + 2.0 * COALESCE(STDDEV(quote_qty::numeric), 0.0),
                AVG(quote_qty::numeric) * 3.0
            )                                                          AS whale_threshold
        FROM staging_fact_raw_trades
        GROUP BY symbol_id
    )
    SELECT
        t.symbol_id,
        t.trade_time,
        CASE WHEN NOT t.is_buyer_maker THEN 'BUY' ELSE 'SELL' END     AS direction,
        t.price,
        t.quote_qty,
        ROUND((t.quote_qty::numeric / NULLIF(s.avg_qty, 0))::numeric, 2) AS size_multiplier,
        CASE
            WHEN t.quote_qty::numeric > s.avg_qty + 4.0 * s.std_qty THEN 'MEGA'
            WHEN t.quote_qty::numeric > s.avg_qty + 3.0 * s.std_qty THEN 'WHALE'
            ELSE 'LARGE'
        END                                                            AS alert_level,
        NOW()                                                          AS computed_at
    FROM staging_fact_raw_trades t
    JOIN sym_stats s USING (symbol_id)
    WHERE t.quote_qty::numeric > s.whale_threshold
"""


def run(spark, jdbc_url, jdbc_props, data_base_path):
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    trades_base = os.path.join(data_base_path, "trades")

    all_dfs = []

    for symbol_dir in sorted(os.listdir(trades_base)):
        symbol_path = os.path.join(trades_base, symbol_dir)
        if not os.path.isdir(symbol_path):
            continue

        symbol_id = sym_map.get(symbol_dir)
        if symbol_id is None:
            print(f"[trades] Bỏ qua '{symbol_dir}' – không có trong dim_symbols")
            continue

        for fname in sorted(os.listdir(symbol_path)):
            if _FNAME_PREFIX not in fname or not fname.endswith(".csv"):
                continue

            df = (
                spark.read
                .option("header", "true")
                .csv(os.path.join(symbol_path, fname))
            )

            df = df.select(
                F.lit(symbol_id).cast("bigint").alias("symbol_id"),
                F.col("trade_id").cast("bigint"),
                (F.col("time").cast("bigint") / 1_000_000).cast("timestamp").alias("trade_time"),
                F.col("price").cast("decimal(30,12)"),
                F.col("qty").cast("decimal(30,12)").alias("qty_base"),
                F.col("quote_qty").cast("decimal(30,12)"),
                (F.col("is_buyer_maker") == "True").alias("is_buyer_maker"),
                (F.col("is_best_match") == "True").alias("is_best_match"),
                F.current_timestamp().alias("ingested_at"),
            ).filter(F.col("trade_id").isNotNull())

            all_dfs.append(df)
            print(f"[trades] Đọc {fname}")

    if not all_dfs:
        print("[trades] Không có file nào, bỏ qua.")
        return

    final_df = all_dfs[0]
    for d in all_dfs[1:]:
        final_df = final_df.union(d)

    # ── [1] Pre-create staging (committed ngay, tránh race condition) ──
    print("[trades] Tạo staging_fact_raw_trades...")
    execute_sql(spark, jdbc_url, jdbc_props, _SQL_DROP_STAGING)
    execute_sql(spark, jdbc_url, jdbc_props, _SQL_CREATE_STAGING)

    # ── [2] Ghi raw trades vào staging (mode=append, không làm DDL) ───
    print("[trades] Ghi staging_fact_raw_trades...")
    write_props = {**jdbc_props, **_WRITE_OPTS}
    final_df.repartition(4).write.jdbc(
        jdbc_url, "staging_fact_raw_trades",
        mode="append",
        properties=write_props,
    )
    print("[trades] staging_fact_raw_trades xong")

    # ── [3] Upsert staging → fact_raw_trades ──────────────────────────
    print("[trades] Upsert vào fact_raw_trades...")
    execute_sql(spark, jdbc_url, jdbc_props, _SQL_UPSERT_TRADES)
    print("[trades] fact_raw_trades xong")

    # ── [4] mart_trade_metrics (Postgres aggregation từ staging) ───────
    print("[trades] Tính mart_trade_metrics (trong Postgres)...")
    execute_sql(spark, jdbc_url, jdbc_props, _SQL_TRUNCATE_METRICS)
    execute_sql(spark, jdbc_url, jdbc_props, _SQL_MART_METRICS)
    print("[trades] mart_trade_metrics xong")

    # ── [5] mart_whale_alerts (Postgres filter từ staging) ─────────────
    print("[trades] Tính mart_whale_alerts (trong Postgres)...")
    execute_sql(spark, jdbc_url, jdbc_props, _SQL_TRUNCATE_WHALE)
    execute_sql(spark, jdbc_url, jdbc_props, _SQL_MART_WHALE)
    print("[trades] mart_whale_alerts xong")

    # ── [6] Dọn staging ────────────────────────────────────────────────
    execute_sql(spark, jdbc_url, jdbc_props, "DROP TABLE IF EXISTS staging_fact_raw_trades")
    print("[trades] Hoàn tất: fact_raw_trades + mart_trade_metrics + mart_whale_alerts")
