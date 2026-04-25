"""
ETL: fact_klines (1m interval)
Nguồn: klines_1m_*.csv trong DATA_SPLIT/<size>/ (flat MinIO layout)
Schema CSV: open_time, open, high, low, close, volume, close_time, quote_asset_volume,
            num_trades, taker_buy_base_vol, taker_buy_quote_vol, ignore, symbol, open_time_dt
"""
import time

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

from etl_utils import execute_sql, load_symbol_map, load_csv_df, CSV_SCHEMA_KLINES


def run(spark, jdbc_url, jdbc_props, data_base_path):
    import config
    _start = time.time()
    interval = "1m"

    # Bước 1/6: Load symbol map từ Postgres
    _t = time.time()
    print(f"\n{'='*70}")
    print(f"[klines] BAT DAU BULK LOAD (v1.5.0-csv-bench)")
    print(f"[klines] Buoc 1/6: Load symbol map tu Postgres ...")
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    print(f"[klines]   -> {len(sym_map)} symbols ({time.time()-_t:.1f}s)")

    klines_glob = (f"{data_base_path.rstrip('/')}"
                   f"/{config.PREFIX_CSV_RAW.strip('/')}"
                   f"/{config.CSV_FILENAME_KLINES}")

    # Bước 2/6: Đọc CSV với explicit schema
    _t = time.time()
    print(f"[klines] Buoc 2/6: Doc klines CSV ...")
    print(f"[klines]   Glob: {klines_glob}")
    raw_df = load_csv_df(spark, klines_glob, CSV_SCHEMA_KLINES)
    if raw_df is None:
        print(f"[klines] CANH BAO: Khong co CSV match. Bo qua.")
        return None
    print(f"[klines]   -> CSV reader ready ({time.time()-_t:.1f}s)")

    # Bước 3/6: Build transform DAG + broadcast join symbols
    _t = time.time()
    print(f"[klines] Buoc 3/6: Build transform DAG + broadcast join symbols ...")
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
            F.lit(interval).alias("interval_code"),
            (F.col("open_time")  / 1000).cast("timestamp").alias("open_time"),
            (F.col("close_time") / 1000).cast("timestamp").alias("close_time"),
            F.col("open").cast("decimal(30,12)").alias("open_price"),
            F.col("high").cast("decimal(30,12)").alias("high_price"),
            F.col("low").cast("decimal(30,12)").alias("low_price"),
            F.col("close").cast("decimal(30,12)").alias("close_price"),
            F.col("volume").cast("decimal(30,12)").alias("volume_base"),
            F.col("quote_asset_volume").cast("decimal(30,12)").alias("volume_quote"),
            F.col("num_trades").cast("bigint"),
            F.col("taker_buy_quote_vol").cast("decimal(30,12)").alias("taker_buy_quote_volume"),
        )
        .filter(F.col("open_time").isNotNull())
    )
    print(f"[klines]   -> DAG build xong ({time.time()-_t:.1f}s)")

    # Bước 4/6: Dedup + cache + count — trigger Spark compute
    _t = time.time()
    print(f"[klines] Buoc 4/6: Dedup + cache + count() — TRIGGER SPARK COMPUTE ...")
    final_df = transformed_df.dropDuplicates(
        ["symbol_id", "interval_code", "open_time"]
    ).cache()
    final_count = final_df.count()
    compute_time = time.time() - _t
    print(f"[klines]   -> {final_count:,} rows (compute: {compute_time:.1f}s)")

    # Bước 5/6: Ghi staging JDBC (3 partitions = số executor)
    _t = time.time()
    print(f"[klines] Buoc 5/6: Ghi {final_count:,} rows vao staging_fact_klines (JDBC x3 parallel) ...")
    write_props = {**jdbc_props, "numPartitions": "3", "batchsize": "5000",
                   "rewriteBatchedInserts": "true"}
    final_df.write.jdbc(jdbc_url, "staging_fact_klines",
                        mode="overwrite", properties=write_props)
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
