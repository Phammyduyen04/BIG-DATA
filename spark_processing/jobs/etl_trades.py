"""
ETL: mart_trade_metrics (money flow + whale signals per symbol, hourly)
Nguồn: silver/trades/date=*/symbol=*/hour=*/...parquet
"""
import os
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
from etl_utils import execute_sql, load_symbol_map, load_contract_df
import config

# Explicit read schema: chỉ 3 cột trades ETL cần
TRADES_READ_SCHEMA = StructType([
    StructField("quote_qty",      StringType(),  True),
    StructField("quote_volume",   StringType(),  True),  # tên cũ, coalesced bởi load_contract_df
    StructField("is_buyer_maker", BooleanType(), True),
])

def run(spark, jdbc_url, jdbc_props, data_base_path):
    import time
    _start = time.time()

    if os.environ.get("SKIP_ETL_TRADES") == "TRUE":
        print("[trades] REDUCED MODE ENABLED: Skipping trades processing.")
        return

    print(f"\n{'='*70}")
    print(f"[trades] BẮT ĐẦU BULK LOAD (v1.4.9 - Explicit Schema)")
    print(f"{'='*70}")

    # Bước 1/7: Load symbol map
    _t = time.time()
    print(f"[trades] Buoc 1/7: Load symbol map tu Postgres ...")
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    print(f"[trades]   -> {len(sym_map)} symbols ({time.time()-_t:.1f}s)")

    trades_base = f"{data_base_path.rstrip('/')}/{config.PREFIX_TRADES.strip('/')}"
    # Dùng directory path: 1 recursive LIST thay vì ~250k LIST calls
    parquet_path = f"{trades_base}/"

    # Bước 2/7: Đọc Silver Layer trades (lazy)
    _t = time.time()
    print(f"[trades] Buoc 2/7: Doc Silver Layer (Hadoop recursive listing) ...")
    print(f"[trades]   Path: {parquet_path}")
    raw_df = load_contract_df(spark, parquet_path, "trades", schema=TRADES_READ_SCHEMA)
    if raw_df is None:
        print(f"[trades] CANH BAO: Khong co parquet files. Bo qua.")
        return
    print(f"[trades]   -> Danh sach files xong, DAG san sang ({time.time()-_t:.1f}s)")

    # Bước 3/7: Extract metadata + hour bucket + join symbols
    _t = time.time()
    print(f"[trades] Buoc 3/7: Extract metadata + join symbols (broadcast) ...")
    path_regex = r"date=([^/]+)/symbol=([^/]+)/hour=([^/]+)"
    ext_df = (
        raw_df.withColumn("_path", F.input_file_name())
        .withColumn("_date",   F.regexp_extract(F.col("_path"), path_regex, 1))
        .withColumn("_symbol", F.regexp_extract(F.col("_path"), path_regex, 2))
        .withColumn("_hour",   F.regexp_extract(F.col("_path"), path_regex, 3))
        .withColumn(
            "hour_bucket",
            F.to_timestamp(F.concat(F.col("_date"), F.lit(" "), F.col("_hour"), F.lit(":00:00")))
        )
    )
    sym_rows = [(k, v) for k, v in sym_map.items()]
    sym_schema = StructType([
        StructField("symbol_code", StringType()),
        StructField("symbol_id",   LongType()),
    ])
    sym_lkp = spark.createDataFrame(sym_rows, schema=sym_schema)
    joined_df = ext_df.join(F.broadcast(sym_lkp), F.col("_symbol") == sym_lkp.symbol_code, "inner")
    print(f"[trades]   -> DAG build xong ({time.time()-_t:.1f}s)")

    # Bước 4/7: Filter + cache + count() — trigger compute
    _t = time.time()
    print(f"[trades] Buoc 4/7: Filter + cache + count() — TRIGGER SPARK COMPUTE ...")
    print(f"[trades]   (Doc S3 trades + join + filter tren tat ca workers)")
    final_df = (
        joined_df.select(
            "symbol_id",
            "hour_bucket",
            F.col("quote_qty").cast("decimal(30,12)"),
            F.col("is_buyer_maker").cast("boolean"),
        )
        .filter(F.col("quote_qty").isNotNull())
        .cache()
    )
    raw_count = final_df.count()
    compute_time = time.time() - _t
    print(f"[trades]   -> {raw_count:,} trade records (compute: {compute_time:.1f}s)")

    # Bước 5/7: Pass 1 — tính whale threshold
    _t = time.time()
    print(f"[trades] Buoc 5/7: Pass 1 — Tinh whale threshold (avg + 2*stddev per symbol/hour) ...")
    stats = (
        final_df.groupBy("symbol_id", "hour_bucket")
        .agg(
            F.avg("quote_qty").alias("avg_qty"),
            F.coalesce(F.stddev("quote_qty"), F.lit(0.0)).alias("std_qty"),
        )
        .withColumn(
            "whale_threshold",
            F.greatest(F.col("avg_qty") + F.lit(2.0) * F.col("std_qty"), F.col("avg_qty") * F.lit(3.0)),
        )
        .select("symbol_id", "hour_bucket", "whale_threshold")
    )
    print(f"[trades]   -> Whale threshold DAG ready ({time.time()-_t:.1f}s)")

    # Bước 6/7: Pass 2 — tổng hợp money flow + whale signals
    _t = time.time()
    print(f"[trades] Buoc 6/7: Pass 2 — Tong hop money flow + whale signals ...")
    metrics = (
        final_df.join(F.broadcast(stats), ["symbol_id", "hour_bucket"])
        .groupBy("symbol_id", "hour_bucket")
        .agg(
            F.sum(F.when(~F.col("is_buyer_maker"), F.col("quote_qty")).otherwise(F.lit(0))).alias("total_buy_volume"),
            F.sum(F.when(F.col("is_buyer_maker"), F.col("quote_qty")).otherwise(F.lit(0))).alias("total_sell_volume"),
            F.count(F.lit(1)).alias("total_trades"),
            F.first("whale_threshold").alias("whale_threshold"),
            F.sum(F.when((F.col("quote_qty") > F.col("whale_threshold")) & ~F.col("is_buyer_maker"), F.col("quote_qty")).otherwise(F.lit(0))).alias("whale_buy_volume"),
            F.sum(F.when((F.col("quote_qty") > F.col("whale_threshold")) & F.col("is_buyer_maker"), F.col("quote_qty")).otherwise(F.lit(0))).alias("whale_sell_volume"),
        )
        .withColumn("money_flow", F.col("total_buy_volume") - F.col("total_sell_volume"))
        .withColumn(
            "whale_buy_ratio",
            F.when((F.col("whale_buy_volume") + F.col("whale_sell_volume")) > 0,
            F.col("whale_buy_volume") / (F.col("whale_buy_volume") + F.col("whale_sell_volume"))),
        )
        .withColumn("computed_at", F.current_timestamp())
    )
    final_metrics = metrics.dropDuplicates(["symbol_id", "hour_bucket"])
    print(f"[trades]   -> Metrics DAG ready ({time.time()-_t:.1f}s)")

    # Bước 7/7: TRUNCATE + ghi mart_trade_metrics (4 luồng song song)
    _t = time.time()
    print(f"[trades] Buoc 7/7: TRUNCATE + ghi mart_trade_metrics (JDBC x4 parallel) ...")
    execute_sql(spark, jdbc_url, jdbc_props, "TRUNCATE mart_trade_metrics")
    write_props = {**jdbc_props, "numPartitions": "4"}
    final_metrics.write.jdbc(jdbc_url, "mart_trade_metrics", mode="append", properties=write_props)
    print(f"[trades]   -> Ghi mart_trade_metrics xong ({time.time()-_t:.1f}s)")

    final_df.unpersist()
    print(f"[trades] HOAN TAT mart_trade_metrics trong {time.time()-_start:.1f}s")
