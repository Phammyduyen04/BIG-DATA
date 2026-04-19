import time
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from etl_utils import execute_sql, load_symbol_map

def run(spark, jdbc_url, jdbc_props, data_base_path):
    start_time = time.time()
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    ticker_base = f"{data_base_path.rstrip('/')}/ticker"

    print(f"\n{'='*70}")
    print(f"[ticker_24h] BẮT ĐẦU BẢN REFACTOR BULK READ (v1.1.5)")
    print(f"[ticker_24h] Path: {ticker_base}")
    print(f"{'='*70}")

    # 1. Bulk Read với Discovery (basePath giúp nhận diện partition hierarchy)
    # Glob pattern quét toàn bộ tệp JSON trong các thư mục date/symbol/hour
    raw_df = spark.read.option("basePath", ticker_base).json(f"{ticker_base}/date=*/symbol=*/hour=*/*.json")
    
    if raw_df.rdd.isEmpty():
        print("[ticker_24h] Không tìm thấy dữ liệu trên S3, bỏ qua.")
        return

    raw_count = raw_df.count()

    # 2. Trích xuất Metadata từ Path (Semantics: Top-of-the-hour snapshot)
    # regexp_extract lấy (date, symbol, hour) từ đường dẫn tệp S3 thực tế
    path_regex = r"date=([^/]+)/symbol=([^/]+)/hour=([^/]+)"
    
    transformed_df = (
        raw_df.withColumn("_path", F.input_file_name())
        .withColumn("_date",   F.regexp_extract(F.col("_path"), path_regex, 1))
        .withColumn("_symbol", F.regexp_extract(F.col("_path"), path_regex, 2))
        .withColumn("_hour",   F.regexp_extract(F.col("_path"), path_regex, 3))
        # Chuẩn hóa snapshot_time theo logic cũ: Round về đầu giờ YYYY-MM-DD HH:00:00
        .withColumn(
            "snapshot_time", 
            F.to_timestamp(F.concat(F.col("_date"), F.lit(" "), F.col("_hour"), F.lit(":00:00")))
        )
    )

    # 3. Join với dim_symbols (Duy nhất 1 lần Broadcast join cho toàn bộ Data)
    sym_rows = [(k, v) for k, v in sym_map.items()]
    sym_schema = StructType([
        StructField("symbol_code", StringType()),
        StructField("symbol_id",   LongType()),
    ])
    sym_lkp = spark.createDataFrame(sym_rows, schema=sym_schema)

    joined_df = (
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

    joined_count = joined_df.count()

    # 4. Khử trùng (Dedup Semantics: One-record-per-symbol-per-hour)
    # Giữ lại bản ghi đại diện (first-encountered) tương đương logic set() cũ
    final_df = joined_df.dropDuplicates(["symbol_id", "snapshot_time"])
    final_count = final_df.count()
    
    # In Metrics báo cáo hiệu năng
    print(f"\n[ticker_24h] REFACTOR METRICS:")
    print(f"  - Total Raw Rows Discoved   : {raw_count:,}")
    print(f"  - Rows after Symbol Join    : {joined_count:,}")
    print(f"  - Final Rows after Dedup    : {final_count:,}")
    print(f"  - Duplicates Removed        : {joined_count - final_count:,}")

    # 5. Thực thi Ghi (Overwrite Staging -> Postgres Upsert)
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
    print(f"[ticker_24h] Hoàn tất nạp {final_count} rows trong {duration:.2f}s")
    print(f"[ticker_24h] Đã load xong fact_ticker_24h_snapshots")
