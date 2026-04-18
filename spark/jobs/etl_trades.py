"""
ETL: mart_trade_metrics (money flow + whale signals per symbol)

Nguồn: trades/{SYMBOL}/{SYMBOL}-trades-{date}.csv
Cột CSV: trade_id, price, qty, quote_qty, time, is_buyer_maker, is_best_match

Thiết kế mới (bỏ fact_raw_trades):
  - MinIO giữ full historical raw trades → không cần lưu 9.7M rows vào Postgres
  - Spark đọc CSV → groupBy per symbol → chỉ ghi mart_trade_metrics (~N_symbols rows)
  - Không staging table, không JDBC write cho raw trades
  - mart_whale_alerts bị xoá — whale alerts chuyển sang streaming (real-time only)

Luồng:
  [1] Spark đọc toàn bộ trades CSV
  [2] Pass 1 (groupBy): tính avg, stddev, whale_threshold per symbol
  [3] Pass 2 (join + groupBy): tính total_buy/sell/money_flow/whale_buy_ratio
  [4] TRUNCATE mart_trade_metrics rồi append kết quả (~N_symbols rows)

Whale threshold per symbol = GREATEST(avg + 2σ, avg × 3)
"""
import os

from pyspark.sql import functions as F

from etl_utils import execute_sql, load_symbol_map

_FNAME_PREFIX = "-trades-"


def run(spark, jdbc_url, jdbc_props, data_base_path):
    # Reduced Mode Guard: Thoát sớm nếu launcher xác định thiếu dữ liệu trades
    if os.environ.get("SKIP_ETL_TRADES") == "TRUE":
        print("[trades] REDUCED MODE ENABLED: Skipping trades processing due to missing input data.")
        return

    from etl_utils import load_contract_df
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    trades_base = f"{data_base_path.rstrip('/')}/trades"

    all_dfs = []

    # Discovery: Tìm tất cả symbols có dữ liệu trades (layout partitioned theo date)
    from etl_utils import discover_symbols
    symbol_dirs = discover_symbols(spark, trades_base, pattern="date=*/symbol=*")
    
    for symbol_dir in symbol_dirs:
        symbol_id = sym_map.get(symbol_dir)
        if symbol_id is None:
            print(f"[trades] Bỏ qua '{symbol_dir}' – không có trong dim_symbols")
            continue

        # Adapter: Nạp toàn bộ JSON trades cho symbol này (recursive glob)
        # raw/trades/date=*/symbol=BTCUSDT/*.json
        s3_glob_path = f"{data_base_path.rstrip('/')}/trades/date=*/symbol={symbol_dir}/*.json"
        
        print(f"[trades] Đọc dữ liệu từ S3 cho {symbol_dir}")
        
        # Adapter nạp JSON và khôi phục Contract cột (quote_volume -> quote_qty, ...)
        df = load_contract_df(spark, s3_glob_path, "trades")
        if df.rdd.isEmpty():
            continue

        df = df.select(
            F.lit(symbol_id).cast("bigint").alias("symbol_id"),
            F.col("quote_qty").cast("decimal(30,12)"),
            (F.col("is_buyer_maker") == "True").alias("is_buyer_maker"),
        ).filter(F.col("quote_qty").isNotNull())

        all_dfs.append(df)
        print(f"[trades] {symbol_dir} đã nạp thành công")

    if not all_dfs:
        print("[trades] Không có file nào, bỏ qua.")
        return

    final_df = all_dfs[0]
    for d in all_dfs[1:]:
        final_df = final_df.union(d)

    # ── Pass 1: tính whale_threshold per symbol (groupBy nhẹ) ─────────
    print("[trades] Tính whale_threshold per symbol...")
    stats = (
        final_df.groupBy("symbol_id")
        .agg(
            F.avg("quote_qty").alias("avg_qty"),
            F.coalesce(F.stddev("quote_qty"), F.lit(0.0)).alias("std_qty"),
        )
        .withColumn(
            "whale_threshold",
            F.greatest(
                F.col("avg_qty") + F.lit(2.0) * F.col("std_qty"),
                F.col("avg_qty") * F.lit(3.0),
            ),
        )
        .select("symbol_id", "whale_threshold")
    )

    # ── Pass 2: broadcast join stats → tính metrics per symbol ────────
    # stats rất nhỏ (<100 rows) → broadcast để tránh shuffle lớn
    print("[trades] Tính mart_trade_metrics...")
    enriched = final_df.join(F.broadcast(stats), "symbol_id")

    metrics = (
        enriched.groupBy("symbol_id")
        .agg(
            F.sum(
                F.when(~F.col("is_buyer_maker"), F.col("quote_qty")).otherwise(F.lit(0))
            ).alias("total_buy_volume"),
            F.sum(
                F.when(F.col("is_buyer_maker"), F.col("quote_qty")).otherwise(F.lit(0))
            ).alias("total_sell_volume"),
            F.count(F.lit(1)).alias("total_trades"),
            F.first("whale_threshold").alias("whale_threshold"),
            F.sum(
                F.when(
                    (F.col("quote_qty") > F.col("whale_threshold")) & ~F.col("is_buyer_maker"),
                    F.col("quote_qty"),
                ).otherwise(F.lit(0))
            ).alias("whale_buy_volume"),
            F.sum(
                F.when(
                    (F.col("quote_qty") > F.col("whale_threshold")) & F.col("is_buyer_maker"),
                    F.col("quote_qty"),
                ).otherwise(F.lit(0))
            ).alias("whale_sell_volume"),
        )
        .withColumn("money_flow", F.col("total_buy_volume") - F.col("total_sell_volume"))
        .withColumn(
            "whale_buy_ratio",
            F.when(
                (F.col("whale_buy_volume") + F.col("whale_sell_volume")) > 0,
                F.col("whale_buy_volume")
                / (F.col("whale_buy_volume") + F.col("whale_sell_volume")),
            ),
        )
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "symbol_id",
            F.col("total_buy_volume").cast("decimal(30,12)"),
            F.col("total_sell_volume").cast("decimal(30,12)"),
            F.col("money_flow").cast("decimal(30,12)"),
            F.col("total_trades").cast("bigint"),
            F.col("whale_threshold").cast("decimal(30,12)"),
            F.col("whale_buy_volume").cast("decimal(30,12)"),
            F.col("whale_sell_volume").cast("decimal(30,12)"),
            F.col("whale_buy_ratio").cast("decimal(10,6)"),
            "computed_at",
        )
    )

    # ── Ghi mart_trade_metrics (TRUNCATE + append) ────────────────────
    execute_sql(spark, jdbc_url, jdbc_props, "TRUNCATE mart_trade_metrics")
    metrics.write.jdbc(
        jdbc_url,
        "mart_trade_metrics",
        mode="append",
        properties=jdbc_props,
    )
    print("[trades] mart_trade_metrics xong")
