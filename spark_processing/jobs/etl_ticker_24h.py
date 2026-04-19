"""
ETL: fact_ticker_24h_snapshots
Nguồn: ticker_24h/ticker_24h_{YYYYMMDD}_{HHMMSS}.csv
Cột CSV: symbol, priceChange, priceChangePercent, lastPrice,
         highPrice, lowPrice, volume, quoteVolume, count, ...

snapshot_time lấy từ tên file (không có trong dữ liệu).
Chỉ load các symbol có trong import time
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from etl_utils import execute_sql, load_symbol_map, load_contract_df

def run(spark, jdbc_url, jdbc_props, data_base_path):
    start_time = time.time()
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    ticker_base = f"{data_base_path.rstrip('/')}/ticker"

    print(f"[ticker_24h] Bắt đầu Bulk Load từ: {ticker_base}")

    # 1. Bulk Read toàn bộ JSON ticker (Recursive Discovery)
    # Path format: .../raw/ticker/date=YYYY-MM-DD/symbol=SYMBOL/hour=HH/*.json
    raw_df = spark.read.json(f"{ticker_base}/date=*/symbol=*/hour=/*.json")
    
    if raw_df.rdd.isEmpty():
        print("[ticker_24h] Không tìm thấy dữ liệu trên S3, bỏ qua.")
        return

    # 2. Trích xuất Metadata từ Path sử dụng Spark Native Functions (Tránh lặp Driver)
    # regexp_extract lấy (date, symbol, hour) từ input_file_name()
    path_regex = r"date=([^/]+)/symbol=([^/]+)/hour=([^/]+)"
    
    transformed_df = (
        raw_df.withColumn("_path", F.input_file_name())
        .withColumn("_date",   F.regexp_extract(F.col("_path"), path_regex, 1))
        .withColumn("_symbol", F.regexp_extract(F.col("_path"), path_regex, 2))
        .withColumn("_hour",   F.regexp_extract(F.col("_path"), path_regex, 3))
        # Chuẩn hóa snapshot_time: lấy đầu giờ của partition
        .withColumn(
            "snapshot_time", 
            F.to_timestamp(F.concat(F.col("_date"), F.lit(" "), F.col("_hour"), F.lit(":00:00")))
        )
    )

    # 3. Join với dim_symbols một lần duy nhất (Broadcast)
    sym_rows = [(k, v) for k, v in sym_map.items()]
    sym_schema = StructType([
        StructField("symbol_code", StringType()),
        StructField("symbol_id",   LongType()),
    ])
    sym_lkp = spark.createDataFrame(sym_rows, schema=sym_schema)

    final_df = (
        transformed_df.join(F.broadcast(sym_lkp), transformed_df._symbol == sym_lkp.symbol_code, "inner")
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

    # 4. Dedup và Metrics
    before_count = final_df.count()
    final_df = final_df.dropDuplicates(["symbol_id", "snapshot_time"])
    after_count = final_df.count()
    
    print(f"[ticker_24h] Metrics:")
    print(f"  - Tổng row trước dedup: {before_count}")
    print(f"  - Tổng row sau dedup : {after_count}")
    print(f"  - Số row bị loại bỏ   : {before_count - after_count}")

    # 5. Write JDBC (Overwrite Staging -> Upsert Fact)
    final_df.write.jdbc(jdbc_url, "staging_fact_ticker_24h", mode="overwrite", properties=jdbc_props)

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

    duration = time.time() - start_time
    print(f"[ticker_24h] Hoàn tất nạp {after_count} rows trong {duration:.2f}s")
k, jdbc_url, jdbc_props, "DROP TABLE IF EXISTS staging_fact_ticker_24h")

    print(f"[ticker_24h] Đã load {row_count} rows vào fact_ticker_24h_snapshots")
