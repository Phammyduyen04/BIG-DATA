import time
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from etl_utils import execute_sql, load_symbol_map

def run(spark, jdbc_url, jdbc_props, data_base_path):
    import config
    _start = time.time()

    print(f"\n{'='*70}")
    print(f"[ticker_24h] BẮT ĐẦU BULK LOAD (v1.4.7 - Phase B Performance)")
    print(f"{'='*70}")

    # Bước 1/6: Load symbol map
    _t = time.time()
    print(f"[ticker_24h] Buoc 1/6: Load symbol map tu Postgres ...")
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    print(f"[ticker_24h]   -> {len(sym_map)} symbols ({time.time()-_t:.1f}s)")

    ticker_base = f"{data_base_path.rstrip('/')}/{config.PREFIX_TICKER.strip('/')}"
    # Dùng directory path: 1 recursive LIST thay vì ~250k LIST calls
    parquet_path = f"{ticker_base}/"

    # Bước 2/6: Đọc Silver Layer ticker (lazy)
    _t = time.time()
    print(f"[ticker_24h] Buoc 2/6: Doc Silver Layer ticker (recursive dir, 1 LIST call) ...")
    print(f"[ticker_24h]   Path: {parquet_path}")
    raw_df = spark.read.option("mergeSchema", "true").parquet(parquet_path)
    print(f"[ticker_24h]   -> Scan xong (lazy) ({time.time()-_t:.1f}s)")

    # Bước 3/6: Build transform DAG + broadcast join
    _t = time.time()
    print(f"[ticker_24h] Buoc 3/6: Build transform DAG + broadcast join symbols ...")
    path_regex = r"date=([^/]+)/symbol=([^/]+)/hour=([^/]+)"
    sym_rows = [(k, v) for k, v in sym_map.items()]
    sym_schema = StructType([
        StructField("symbol_code", StringType()),
        StructField("symbol_id",   LongType()),
    ])
    sym_lkp = spark.createDataFrame(sym_rows, schema=sym_schema)

    transformed_df = (
        raw_df.withColumn("_path", F.input_file_name())
        .withColumn("_date",   F.regexp_extract(F.col("_path"), path_regex, 1))
        .withColumn("_symbol", F.regexp_extract(F.col("_path"), path_regex, 2))
        .withColumn("_hour",   F.regexp_extract(F.col("_path"), path_regex, 3))
        .withColumn(
            "snapshot_time",
            F.to_timestamp(F.concat(F.col("_date"), F.lit(" "), F.col("_hour"), F.lit(":00:00")))
        )
        .join(F.broadcast(sym_lkp), F.col("_symbol") == sym_lkp.symbol_code, "inner")
        .select(
            F.col("symbol_id"),
            F.col("snapshot_time"),
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
    )
    print(f"[ticker_24h]   -> DAG build xong ({time.time()-_t:.1f}s)")

    # Bước 4/6: Dedup + cache + count() — trigger compute
    _t = time.time()
    print(f"[ticker_24h] Buoc 4/6: Dedup + cache + count() — TRIGGER SPARK COMPUTE ...")
    print(f"[ticker_24h]   (Doc S3 ticker + join + dedup tren tat ca workers)")
    final_df = transformed_df.dropDuplicates(["symbol_id", "snapshot_time"]).cache()
    final_count = final_df.count()
    compute_time = time.time() - _t
    estimated_write = max(5, final_count / 50000)
    print(f"[ticker_24h]   -> {final_count:,} rows (compute: {compute_time:.1f}s)")
    print(f"[ticker_24h]   -> Du kien ghi staging: ~{estimated_write:.0f}s | UPSERT: ~{estimated_write*0.8:.0f}s")

    # Bước 5/6: Ghi staging JDBC (4 luồng song song)
    _t = time.time()
    print(f"[ticker_24h] Buoc 5/6: Ghi {final_count:,} rows vao staging_fact_ticker_24h (JDBC x4 parallel) ...")
    write_props = {**jdbc_props, "numPartitions": "4"}
    final_df.write.jdbc(jdbc_url, "staging_fact_ticker_24h", mode="overwrite", properties=write_props)
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
