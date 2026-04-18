"""
Batch resample: 1m → 5m, 15m, 30m, 1h, 4h, 1d
Nguồn: fact_klines WHERE interval_code = '1m' (đã có trong Postgres)
Đích:  fact_klines với interval_code tương ứng (upsert)

Chạy sau etl_klines.py hoặc chạy độc lập.
"""
from pyspark.sql import functions as F

from etl_utils import execute_sql

# (interval_code, duration_string cho F.window)
TARGET_INTERVALS = [
    ("5m",  "5  minutes"),
    ("15m", "15 minutes"),
    ("30m", "30 minutes"),
    ("1h",  "60 minutes"),
    ("4h",  "240 minutes"),
    ("1d",  "1440 minutes"),
]


def _resample(df_1m, interval_code: str, window_duration: str):
    """
    Aggregate 1m candles thành 1 interval lớn hơn.

    open_price  = open_price của cây nến 1m đầu tiên trong window  → min_by
    close_price = close_price của cây nến 1m cuối cùng trong window → max_by
    high_price  = max(high_price)
    low_price   = min(low_price)
    close_time  = close_time của cây nến 1m cuối cùng              → max_by
    """
    df_win = df_1m.groupBy(
        "symbol_id",
        F.window(F.col("open_time"), window_duration).alias("w"),
    ).agg(
        F.min_by("open_price",  F.col("open_time")).alias("open_price"),
        F.max("high_price")                        .alias("high_price"),
        F.min("low_price")                         .alias("low_price"),
        F.max_by("close_price", F.col("open_time")).alias("close_price"),
        F.max_by("close_time",  F.col("open_time")).alias("close_time"),
        F.sum("volume_base")                       .alias("volume_base"),
        F.sum("volume_quote")                      .alias("volume_quote"),
        F.sum("num_trades")                        .alias("num_trades"),
        F.sum("taker_buy_quote_volume")            .alias("taker_buy_quote_volume"),
    )

    return df_win.select(
        F.col("symbol_id"),
        F.lit(interval_code).alias("interval_code"),
        F.col("w.start").alias("open_time"),   # window start = open_time
        F.col("close_time"),
        F.col("open_price"),
        F.col("high_price"),
        F.col("low_price"),
        F.col("close_price"),
        F.col("volume_base"),
        F.col("volume_quote"),
        F.col("num_trades"),
        F.col("taker_buy_quote_volume"),
    )


def run(spark, jdbc_url, jdbc_props, df_1m=None, symbol_codes: list = None):
    """
    df_1m       : DataFrame 1m từ etl_klines.run() — truyền vào để tránh đọc Postgres lần 2.
                  Nếu None (chạy standalone / backfill), tự đọc từ Postgres.
    symbol_codes: giới hạn symbol khi chạy standalone, None = tất cả.
    """
    if df_1m is not None:
        # Đường dẫn nhanh: dùng DataFrame đã có sẵn trong bộ nhớ Spark
        print("[resample] Dùng DataFrame từ etl_klines (không đọc lại Postgres)")
    else:
        # Standalone / backfill: đọc từ Postgres
        print("[resample] Đọc 1m candles từ Postgres (chế độ standalone)...")
        query = "(SELECT * FROM fact_klines WHERE interval_code = '1m') AS src"
        df_1m = spark.read.jdbc(jdbc_url, query, properties=jdbc_props)

        if symbol_codes:
            sym_df = spark.read.jdbc(jdbc_url, "dim_symbols", properties=jdbc_props)
            ids = [
                r.symbol_id
                for r in sym_df.filter(F.col("symbol_code").isin(symbol_codes)).collect()
            ]
            df_1m = df_1m.filter(F.col("symbol_id").isin(ids))

        df_1m = df_1m.cache()

    total_1m = df_1m.count()
    print(f"[resample] {total_1m} rows 1m sẵn sàng")

    write_props = {**jdbc_props, "batchsize": "50000", "numPartitions": "4"}

    for interval_code, window_duration in TARGET_INTERVALS:
        print(f"[resample] Tính {interval_code} ...")

        df_agg = _resample(df_1m, interval_code, window_duration)

        df_agg.write.jdbc(
            jdbc_url, "staging_fact_klines_rs",
            mode="overwrite",
            properties=write_props,
        )

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
            FROM staging_fact_klines_rs
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
        execute_sql(spark, jdbc_url, jdbc_props,
                    "DROP TABLE IF EXISTS staging_fact_klines_rs")

        print(f"[resample] {interval_code} xong")

    df_1m.unpersist()
    print("[resample] Hoàn tất tất cả intervals")
