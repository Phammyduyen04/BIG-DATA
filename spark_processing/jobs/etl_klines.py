"""
ETL: fact_klines
Nguồn: klines/{SYMBOL}/{SYMBOL}_{interval}_{date}_{date}.csv
Cột CSV: open_time, open, high, low, close, volume, close_time,
         quote_volume, num_trades, taker_buy_base_vol, taker_buy_quote_vol
"""
import os
import re

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

from etl_utils import execute_sql, load_symbol_map

# Regex lấy interval từ tên file: BTCUSDT_1m_20260408_20260409.csv
_FNAME_RE = re.compile(r"^[A-Z]+_([^_]+)_\d{8}_\d{8}\.csv$")



def run(spark, jdbc_url, jdbc_props, data_base_path):
    import time
    import config
    _start = time.time()

    # Bước 1/6: Load symbol map từ Postgres
    _t = time.time()
    print(f"\n{'='*70}")
    print(f"[klines] BẮT ĐẦU BULK LOAD (v1.4.7 - Phase B Performance)")
    print(f"[klines] Buoc 1/6: Load symbol map tu Postgres ...")
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    print(f"[klines]   -> {len(sym_map)} symbols ({time.time()-_t:.1f}s)")

    klines_base = f"{data_base_path.rstrip('/')}/{config.PREFIX_KLINES.strip('/')}"
    interval = "1m"
    # Dùng directory path thay vì deep wildcard: 1 recursive LIST thay vì ~250k LIST calls
    parquet_path = f"{klines_base}/interval={interval}/"

    # Bước 2/6: Đọc Silver Layer (lazy - chưa trigger compute)
    _t = time.time()
    print(f"[klines] Buoc 2/6: Doc Silver Layer (recursive dir, 1 LIST call) ...")
    print(f"[klines]   Path: {parquet_path}")
    raw_df = spark.read.option("mergeSchema", "true").parquet(parquet_path)
    print(f"[klines]   -> Scan xong (lazy, chua compute) ({time.time()-_t:.1f}s)")

    # Bước 3/6: Build transform DAG + broadcast join
    _t = time.time()
    print(f"[klines] Buoc 3/6: Build transform DAG + broadcast join symbols ...")
    path_regex = r"symbol=([^/]+)"
    sym_rows = [(k, v) for k, v in sym_map.items()]
    sym_schema = StructType([
        StructField("symbol_code", StringType()),
        StructField("symbol_id",   LongType()),
    ])
    sym_lkp = spark.createDataFrame(sym_rows, schema=sym_schema)

    transformed_df = (
        raw_df.withColumn("_path", F.input_file_name())
        .withColumn("_symbol_code", F.regexp_extract(F.col("_path"), path_regex, 1))
        .join(F.broadcast(sym_lkp), F.col("_symbol_code") == sym_lkp.symbol_code, "inner")
        .select(
            F.col("symbol_id"),
            F.lit(interval).alias("interval_code"),
            (F.col("open_time") / 1000).cast("timestamp").alias("open_time"),
            (F.col("close_time") / 1000).cast("timestamp").alias("close_time"),
            F.col("open").cast("decimal(30,12)").alias("open_price"),
            F.col("high").cast("decimal(30,12)").alias("high_price"),
            F.col("low").cast("decimal(30,12)").alias("low_price"),
            F.col("close").cast("decimal(30,12)").alias("close_price"),
            F.col("volume").cast("decimal(30,12)").alias("volume_base"),
            F.col("quote_volume").cast("decimal(30,12)").alias("volume_quote"),
            F.col("num_trades").cast("bigint"),
            F.col("taker_buy_quote_vol").cast("decimal(30,12)").alias("taker_buy_quote_volume"),
        )
        .filter(F.col("open_time").isNotNull())
    )
    print(f"[klines]   -> DAG build xong ({time.time()-_t:.1f}s)")

    # Bước 4/6: Dedup + cache + count() — trigger thực sự compute Spark
    _t = time.time()
    print(f"[klines] Buoc 4/6: Dedup + cache + count() — TRIGGER SPARK COMPUTE ...")
    print(f"[klines]   (Day la buoc nang nhat: doc S3 + join + dedup tren tat ca workers)")
    final_df = transformed_df.dropDuplicates(["symbol_id", "interval_code", "open_time"]).cache()
    final_count = final_df.count()
    compute_time = time.time() - _t
    estimated_write = max(10, final_count / 50000)
    print(f"[klines]   -> {final_count:,} rows (compute: {compute_time:.1f}s)")
    print(f"[klines]   -> Du kien ghi JDBC staging: ~{estimated_write:.0f}s | UPSERT: ~{estimated_write*0.8:.0f}s")

    # Bước 5/6: Ghi staging JDBC (4 luồng song song)
    _t = time.time()
    print(f"[klines] Buoc 5/6: Ghi {final_count:,} rows vao staging_fact_klines (JDBC x4 parallel) ...")
    write_props = {**jdbc_props, "numPartitions": "4"}
    final_df.write.jdbc(jdbc_url, "staging_fact_klines", mode="overwrite", properties=write_props)
    print(f"[klines]   -> Ghi staging xong ({time.time()-_t:.1f}s)")

    # Bước 6/6: UPSERT + dọn staging
    _t = time.time()
    print(f"[klines] Buoc 6/6: UPSERT vao fact_klines (ON CONFLICT UPDATE) ...")
    execute_sql(spark, jdbc_url, jdbc_props, """
        INSERT INTO fact_klines (
            symbol_id, interval_code, open_time, close_time,
            open_price, high_price, low_price, close_price,
            volume_base, volume_quote, num_trades, taker_buy_quote_volume
        )
        SELECT
            symbol_id, interval_code, open_time, close_time,
            open_price, high_price, low_price, close_price,
            volume_base, volume_quote, num_trades, taker_buy_quote_volume
        FROM staging_fact_klines
        ON CONFLICT (symbol_id, interval_code, open_time) DO UPDATE SET
            close_time             = EXCLUDED.close_time,
            open_price             = EXCLUDED.open_price,
            high_price             = EXCLUDED.high_price,
            low_price              = EXCLUDED.low_price,
            close_price            = EXCLUDED.close_price,
            volume_base            = EXCLUDED.volume_base,
            volume_quote           = EXCLUDED.volume_quote,
            num_trades             = EXCLUDED.num_trades,
            taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume
    """)
    execute_sql(spark, jdbc_url, jdbc_props, "DROP TABLE IF EXISTS staging_fact_klines")
    print(f"[klines]   -> UPSERT + don dep xong ({time.time()-_t:.1f}s)")

    print(f"[klines] HOAN TAT: {final_count:,} rows trong {time.time()-_start:.1f}s")
    return final_df
