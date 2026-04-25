"""
ETL: mart_trade_metrics (money flow + whale signals per symbol, full-window)
Nguồn: trades_<SYMBOL>_<Nd>.csv trong DATA_SPLIT/<size>/ (flat MinIO layout)
Schema CSV: trade_id, price, qty, quote_qty, time(µs), is_buyer_maker, is_best_match

Edge cases:
  - Symbol KHONG co trong CSV → extract tu filename qua regex
  - `time` la MICROSECONDS (16 digits) → chia 1_000_000 truoc khi to_timestamp
  - DDL PK = symbol_id alone → aggregate 1 row/symbol (toan bo cua so du lieu)
"""
import os
import time

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

from etl_utils import execute_sql, load_symbol_map, load_csv_df, CSV_SCHEMA_TRADES
import config


def run(spark, jdbc_url, jdbc_props, data_base_path):
    _start = time.time()

    if os.environ.get("SKIP_ETL_TRADES") == "TRUE":
        print("[trades] REDUCED MODE ENABLED: Skipping trades processing.")
        return

    print(f"\n{'='*70}")
    print(f"[trades] BAT DAU BULK LOAD (v1.5.0-csv-bench)")
    print(f"{'='*70}")

    # Bước 1/7: Load symbol map
    _t = time.time()
    print(f"[trades] Buoc 1/7: Load symbol map tu Postgres ...")
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    print(f"[trades]   -> {len(sym_map)} symbols ({time.time()-_t:.1f}s)")

    trades_glob = (f"{data_base_path.rstrip('/')}"
                   f"/{config.PREFIX_CSV_RAW.strip('/')}"
                   f"/{config.CSV_GLOB_TRADES}")

    # Bước 2/7: Đọc CSV trades
    _t = time.time()
    print(f"[trades] Buoc 2/7: Doc trades CSV ...")
    print(f"[trades]   Glob: {trades_glob}")
    raw_df = load_csv_df(spark, trades_glob, CSV_SCHEMA_TRADES)
    if raw_df is None:
        print(f"[trades] CANH BAO: Khong co CSV match. Bo qua.")
        return
    print(f"[trades]   -> CSV reader ready ({time.time()-_t:.1f}s)")

    # Bước 3/7: Extract symbol từ filename + join symbols
    _t = time.time()
    print(f"[trades] Buoc 3/7: Extract symbol tu filename + join symbols (broadcast) ...")
    fname_regex = r"trades_([A-Z0-9]+)_\d+d\.csv"
    ext_df = (
        raw_df.withColumn("_path", F.input_file_name())
        .withColumn("_symbol", F.regexp_extract(F.col("_path"), fname_regex, 1))
    )
    sym_rows = [(k, v) for k, v in sym_map.items()]
    sym_schema = StructType([
        StructField("symbol_code", StringType()),
        StructField("symbol_id",   LongType()),
    ])
    sym_lkp = spark.createDataFrame(sym_rows, schema=sym_schema)
    joined_df = ext_df.join(
        F.broadcast(sym_lkp),
        F.col("_symbol") == sym_lkp.symbol_code,
        "inner"
    )
    print(f"[trades]   -> DAG build xong ({time.time()-_t:.1f}s)")

    # Bước 4/7: Filter + cache + count — trigger compute
    _t = time.time()
    print(f"[trades] Buoc 4/7: Filter + cache + count() — TRIGGER SPARK COMPUTE ...")
    final_df = (
        joined_df.select(
            "symbol_id",
            F.col("quote_qty").cast("decimal(30,12)").alias("quote_qty"),
            F.col("is_buyer_maker").cast("boolean").alias("is_buyer_maker"),
        )
        .filter(F.col("quote_qty").isNotNull())
        .cache()
    )
    raw_count = final_df.count()
    print(f"[trades]   -> {raw_count:,} trade records (compute: {time.time()-_t:.1f}s)")

    # Bước 5/7: Pass 1 — whale threshold per symbol (full window)
    _t = time.time()
    print(f"[trades] Buoc 5/7: Pass 1 — Whale threshold per symbol ...")
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
    print(f"[trades]   -> Whale threshold DAG ready ({time.time()-_t:.1f}s)")

    # Bước 6/7: Pass 2 — money flow + whale signals (1 row per symbol)
    _t = time.time()
    print(f"[trades] Buoc 6/7: Pass 2 — Money flow + whale signals (1 row/symbol) ...")
    metrics = (
        final_df.join(F.broadcast(stats), ["symbol_id"])
        .groupBy("symbol_id")
        .agg(
            F.sum(F.when(~F.col("is_buyer_maker"), F.col("quote_qty")).otherwise(F.lit(0)))
                .alias("total_buy_volume"),
            F.sum(F.when( F.col("is_buyer_maker"), F.col("quote_qty")).otherwise(F.lit(0)))
                .alias("total_sell_volume"),
            F.count(F.lit(1)).alias("total_trades"),
            F.first("whale_threshold").alias("whale_threshold"),
            F.sum(F.when(
                (F.col("quote_qty") > F.col("whale_threshold")) & ~F.col("is_buyer_maker"),
                F.col("quote_qty")).otherwise(F.lit(0))
            ).alias("whale_buy_volume"),
            F.sum(F.when(
                (F.col("quote_qty") > F.col("whale_threshold")) & F.col("is_buyer_maker"),
                F.col("quote_qty")).otherwise(F.lit(0))
            ).alias("whale_sell_volume"),
        )
        .withColumn("money_flow", F.col("total_buy_volume") - F.col("total_sell_volume"))
        .withColumn(
            "whale_buy_ratio",
            F.when(
                (F.col("whale_buy_volume") + F.col("whale_sell_volume")) > 0,
                F.col("whale_buy_volume") /
                (F.col("whale_buy_volume") + F.col("whale_sell_volume")),
            ),
        )
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "symbol_id",
            "total_buy_volume",
            "total_sell_volume",
            "money_flow",
            "total_trades",
            "whale_threshold",
            "whale_buy_volume",
            "whale_sell_volume",
            "whale_buy_ratio",
            "computed_at",
        )
    )
    print(f"[trades]   -> Metrics DAG ready ({time.time()-_t:.1f}s)")

    # Bước 7/7: TRUNCATE + ghi mart_trade_metrics
    _t = time.time()
    print(f"[trades] Buoc 7/7: TRUNCATE + ghi mart_trade_metrics (JDBC x3 parallel) ...")
    execute_sql(spark, jdbc_url, jdbc_props, "TRUNCATE mart_trade_metrics")
    write_props = {**jdbc_props, "numPartitions": "3", "batchsize": "5000",
                   "rewriteBatchedInserts": "true"}
    metrics.write.jdbc(jdbc_url, "mart_trade_metrics",
                       mode="append", properties=write_props)
    print(f"[trades]   -> Ghi mart_trade_metrics xong ({time.time()-_t:.1f}s)")

    final_df.unpersist()
    print(f"[trades] HOAN TAT mart_trade_metrics trong {time.time()-_start:.1f}s")
