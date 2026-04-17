"""
ETL: fact_klines
Nguồn: klines/{SYMBOL}/{SYMBOL}_{interval}_{date}_{date}.csv
Cột CSV: open_time, open, high, low, close, volume, close_time,
         quote_volume, num_trades, taker_buy_base_vol, taker_buy_quote_vol
"""
import os
import re

from pyspark.sql import functions as F

from etl_utils import execute_sql, load_symbol_map

# Regex lấy interval từ tên file: BTCUSDT_1m_20260408_20260409.csv
_FNAME_RE = re.compile(r"^[A-Z]+_([^_]+)_\d{8}_\d{8}\.csv$")


def run(spark, jdbc_url, jdbc_props, data_base_path):
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    klines_base = os.path.join(data_base_path, "klines")

    all_dfs = []

    for symbol_dir in sorted(os.listdir(klines_base)):
        symbol_path = os.path.join(klines_base, symbol_dir)
        if not os.path.isdir(symbol_path):
            continue

        symbol_id = sym_map.get(symbol_dir)
        if symbol_id is None:
            print(f"[klines] Bỏ qua '{symbol_dir}' – không có trong dim_symbols")
            continue

        for fname in sorted(os.listdir(symbol_path)):
            m = _FNAME_RE.match(fname)
            if not m:
                continue
            interval = m.group(1)   # "1m", "5m", "1h" ...

            df = (
                spark.read
                .option("header", "true")
                .csv(os.path.join(symbol_path, fname))
            )

            df = df.select(
                F.lit(symbol_id).cast("bigint").alias("symbol_id"),
                F.lit(interval).alias("interval_code"),
                # Timestamps có timezone (+00:00) – Spark to_timestamp đọc được
                F.to_timestamp(F.col("open_time")).alias("open_time"),
                F.to_timestamp(F.col("close_time")).alias("close_time"),
                F.col("open").cast("decimal(30,12)").alias("open_price"),
                F.col("high").cast("decimal(30,12)").alias("high_price"),
                F.col("low").cast("decimal(30,12)").alias("low_price"),
                F.col("close").cast("decimal(30,12)").alias("close_price"),
                F.col("volume").cast("decimal(30,12)").alias("volume_base"),
                F.col("quote_volume").cast("decimal(30,12)").alias("volume_quote"),
                F.col("num_trades").cast("bigint"),
                F.col("taker_buy_quote_vol").cast("decimal(30,12)").alias("taker_buy_quote_volume"),
            ).filter(F.col("open_time").isNotNull())

            all_dfs.append(df)
            print(f"[klines] Đọc {fname} ({interval})")

    if not all_dfs:
        print("[klines] Không có file nào, bỏ qua.")
        return None

    final_df = all_dfs[0]
    for d in all_dfs[1:]:
        final_df = final_df.union(d)

    # Cache để resample dùng lại mà không đọc file lần 2
    final_df = final_df.cache()

    final_df.write.jdbc(jdbc_url, "staging_fact_klines", mode="overwrite", properties=jdbc_props)

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

    print(f"[klines] Đã load xong fact_klines")
    return final_df   # trả về để resample dùng lại, tránh đọc Postgres lần 2
