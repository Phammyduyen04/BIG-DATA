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
    from etl_utils import discover_symbols, load_contract_df
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    klines_base = f"{data_base_path.rstrip('/')}/klines"

    all_dfs = []

    # Adapter: Tìm tất cả symbols có dữ liệu klines (mặc định check interval 1m)
    symbol_dirs = discover_symbols(spark, klines_base, pattern="interval=1m/date=*/symbol=*")
    
    for symbol_dir in symbol_dirs:
        symbol_id = sym_map.get(symbol_dir)
        if symbol_id is None:
            print(f"[klines] Bỏ qua '{symbol_dir}' – không có trong dim_symbols")
            continue

        # Giữ nguyên contract intervals: Hiện tại tập trung 1m
        intervals = ["1m"] 
        
        for interval in intervals:
            # Glob path chuẩn cho layout partition thực tế
            s3_glob_path = f"{klines_base}/interval={interval}/date=*/symbol={symbol_dir}/*/*.json"
            
            # Khôi phục 'fname' ảo để Regex _FNAME_RE hoạt động (Source of Truth Contract)
            fname = f"{symbol_dir}_{interval}_99999999_99999999.csv"
            m = _FNAME_RE.match(fname)
            if not m: continue
            
            print(f"[klines] Đọc dữ liệu từ S3 cho {symbol_dir} ({interval})")
            
            # Adapter nạp JSON và khôi phục Contract cột
            df = load_contract_df(spark, s3_glob_path, "klines")
            if df.rdd.isEmpty():
                continue

            df = df.select(
                F.lit(symbol_id).cast("bigint").alias("symbol_id"),
                F.lit(interval).alias("interval_code"),
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
            print(f"[klines] {symbol_dir} ({interval}) đã nạp")

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
