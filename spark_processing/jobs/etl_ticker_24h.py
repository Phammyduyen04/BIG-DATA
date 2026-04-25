"""
ETL: fact_ticker_24h_snapshots
Nguồn: ticker_24h_*.csv trong DATA_SPLIT/<size>/ (flat MinIO layout)
Schema CSV: symbol, priceChange, ..., openTime(ms), closeTime(ms), count
snapshot_time = date_trunc('hour', openTime/1000) → 1 row/symbol/hour
"""
import time

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

from etl_utils import execute_sql, load_symbol_map, load_csv_df, CSV_SCHEMA_TICKER
import config


def run(spark, jdbc_url, jdbc_props, data_base_path):
    _start = time.time()

    print(f"\n{'='*70}")
    print(f"[ticker_24h] BAT DAU BULK LOAD (v1.5.0-csv-bench)")
    print(f"{'='*70}")

    # Bước 1/6: Load symbol map
    _t = time.time()
    print(f"[ticker_24h] Buoc 1/6: Load symbol map tu Postgres ...")
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    print(f"[ticker_24h]   -> {len(sym_map)} symbols ({time.time()-_t:.1f}s)")

    ticker_glob = (f"{data_base_path.rstrip('/')}"
                   f"/{config.PREFIX_CSV_RAW.strip('/')}"
                   f"/{config.CSV_FILENAME_TICKER}")

    # Bước 2/6: Đọc ticker CSV
    _t = time.time()
    print(f"[ticker_24h] Buoc 2/6: Doc ticker CSV ...")
    print(f"[ticker_24h]   Glob: {ticker_glob}")
    raw_df = load_csv_df(spark, ticker_glob, CSV_SCHEMA_TICKER)
    if raw_df is None:
        print(f"[ticker_24h] CANH BAO: Khong co CSV match. Bo qua.")
        return
    print(f"[ticker_24h]   -> CSV reader ready ({time.time()-_t:.1f}s)")

    # Bước 3/6: Build transform DAG + broadcast join symbols
    _t = time.time()
    print(f"[ticker_24h] Buoc 3/6: Build transform DAG + broadcast join symbols ...")
    sym_rows = [(k, v) for k, v in sym_map.items()]
    sym_schema = StructType([
        StructField("symbol_code", StringType()),
        StructField("symbol_id",   LongType()),
    ])
    sym_lkp = spark.createDataFrame(sym_rows, schema=sym_schema)

    transformed_df = (
        raw_df.join(F.broadcast(sym_lkp),
                    raw_df.symbol == sym_lkp.symbol_code,
                    "inner")
        .select(
            F.col("symbol_id"),
            F.date_trunc("hour",
                (F.col("openTime") / 1000).cast("timestamp")
            ).alias("snapshot_time"),
            F.col("priceChange").cast("decimal(30,12)").alias("price_change"),
            F.col("priceChangePercent").cast("decimal(18,8)").alias("price_change_percent"),
            F.col("lastPrice").cast("decimal(30,12)").alias("last_price"),
            F.col("highPrice").cast("decimal(30,12)").alias("high_price"),
            F.col("lowPrice").cast("decimal(30,12)").alias("low_price"),
            F.col("volume").cast("decimal(30,12)").alias("volume_base_24h"),
            F.col("quoteVolume").cast("decimal(30,12)").alias("volume_quote_24h"),
            F.col("count").cast("bigint").alias("trade_count"),
            F.current_timestamp().alias("ingested_at"),
        )
        .filter(F.col("snapshot_time").isNotNull())
    )
    print(f"[ticker_24h]   -> DAG build xong ({time.time()-_t:.1f}s)")

    # Bước 4/6: Dedup + cache + count() — trigger Spark compute
    _t = time.time()
    print(f"[ticker_24h] Buoc 4/6: Dedup + cache + count() — TRIGGER SPARK COMPUTE ...")
    final_df = transformed_df.dropDuplicates(["symbol_id", "snapshot_time"]).cache()
    final_count = final_df.count()
    compute_time = time.time() - _t
    print(f"[ticker_24h]   -> {final_count:,} rows (compute: {compute_time:.1f}s)")

    # Bước 5/6: Ghi staging JDBC (3 partitions = số executor)
    _t = time.time()
    print(f"[ticker_24h] Buoc 5/6: Ghi {final_count:,} rows vao staging_fact_ticker_24h (JDBC x3 parallel) ...")
    write_props = {**jdbc_props, "numPartitions": "3", "batchsize": "5000",
                   "rewriteBatchedInserts": "true"}
    final_df.write.jdbc(jdbc_url, "staging_fact_ticker_24h",
                        mode="overwrite", properties=write_props)
    print(f"[ticker_24h]   -> Ghi staging xong ({time.time()-_t:.1f}s)")

    # Bước 6/6: UPSERT + dọn staging
    _t = time.time()
    print(f"[ticker_24h] Buoc 6/6: UPSERT vao fact_ticker_24h_snapshots + don dep ...")
    execute_sql(spark, jdbc_url, jdbc_props, """
        INSERT INTO fact_ticker_24h_snapshots (
            symbol_id, snapshot_time, price_change, price_change_percent,
            last_price, high_price, low_price,
            volume_base_24h, volume_quote_24h, trade_count, ingested_at
        )
        SELECT
            symbol_id, snapshot_time, price_change, price_change_percent,
            last_price, high_price, low_price,
            volume_base_24h, volume_quote_24h, trade_count, ingested_at
        FROM staging_fact_ticker_24h
        ON CONFLICT (symbol_id, snapshot_time) DO UPDATE SET
            price_change         = EXCLUDED.price_change,
            price_change_percent = EXCLUDED.price_change_percent,
            last_price           = EXCLUDED.last_price,
            high_price           = EXCLUDED.high_price,
            low_price            = EXCLUDED.low_price,
            volume_base_24h      = EXCLUDED.volume_base_24h,
            volume_quote_24h     = EXCLUDED.volume_quote_24h,
            trade_count          = EXCLUDED.trade_count,
            ingested_at          = EXCLUDED.ingested_at
    """)
    execute_sql(spark, jdbc_url, jdbc_props, "DROP TABLE IF EXISTS staging_fact_ticker_24h")
    print(f"[ticker_24h]   -> UPSERT + don dep xong ({time.time()-_t:.1f}s)")

    final_df.unpersist()
    print(f"[ticker_24h] HOAN TAT: {final_count:,} rows trong {time.time()-_start:.1f}s")
