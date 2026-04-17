"""
ETL: fact_ticker_24h_snapshots
Nguồn: ticker_24h/ticker_24h_{YYYYMMDD}_{HHMMSS}.csv
Cột CSV: symbol, priceChange, priceChangePercent, lastPrice,
         highPrice, lowPrice, volume, quoteVolume, count, ...

snapshot_time lấy từ tên file (không có trong dữ liệu).
Chỉ load các symbol có trong dim_symbols.
"""
import os
import re
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

from etl_utils import execute_sql, load_symbol_map

_FNAME_RE = re.compile(r"^ticker_24h_(\d{8})_(\d{6})\.csv$")


def run(spark, jdbc_url, jdbc_props, data_base_path):
    from etl_utils import emulate_listdir, load_contract_df
    sym_map = load_symbol_map(spark, jdbc_url, jdbc_props)
    ticker_base = f"{data_base_path.rstrip('/')}/ticker"

    all_dfs = []

    # Adapter: Trên MinIO, ticker lưu theo date=*/symbol=*/hour=*. 
    # Ta sẽ khám phá các partition này để giả lập filename ticker_24h_YYYYMMDD_HHMMSS.csv.
    sc = spark.sparkContext
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    conf = sc._jsc.hadoopConfiguration()
    fs = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(sc._gateway.jvm.java.net.URI(ticker_base), conf)
    
    # Tìm tất cả JSON files qua glob (Recursive)
    # raw/ticker/date=2026-04-16/symbol=1000SATSUSDT/hour=07/part-00001.json
    status_list = fs.globStatus(Path(f"{ticker_base}/date=*/symbol=*/hour=*/part-*.json"))
    
    if not status_list:
        print("[ticker_24h] Không tìm thấy dữ liệu trên S3, bỏ qua.")
        return

    # Để bảo tồn logic lặp từng file của bạn, ta sẽ nhóm theo đường dẫn
    processed_files = set()
    for status in status_list:
        path_str = status.getPath().toString()
        
        # Parse metadata từ đường dẫn S3 để giả lập filename
        # Ví dụ: .../date=2026-04-16/.../hour=07/...
        import re
        date_m = re.search(r"date=(\d{4})-(\d{2})-(\d{2})", path_str)
        hour_m = re.search(r"hour=(\d{2})", path_str)
        
        if not date_m or not hour_m: continue
        
        # Tạo virtual filename: ticker_24h_20260416_070000.csv
        v_date = date_m.group(1) + date_m.group(2) + date_m.group(3)
        v_hour = hour_m.group(1) + "0000"
        fname = f"ticker_24h_{v_date}_{v_hour}.csv"
        
        # Chỉ xử lý mỗi symbol+date+hour một lần (đọc cả folder JSON)
        file_key = f"{v_date}_{v_hour}"
        if file_key in processed_files: continue
        processed_files.add(file_key)

        m = _FNAME_RE.match(fname)
        if not m: continue

        # Parse snapshot_time (Logic gốc được giữ nguyên)
        snapshot_dt = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
        snapshot_ts = snapshot_dt.strftime("%Y-%m-%d %H:%M:%S")

        # Nạp dữ liệu từ S3 folder (không phải từng file part để tăng tốc)
        # s3_glob = path_str.rsplit("/", 1)[0] + "/*.json"
        # Nhưng để chính xác nhất, ta nạp folder chứa file hiện tại
        s3_folder = path_str.rsplit("/", 1)[0]
        
        # Khôi phục camelCase: price_change -> priceChange
        df = load_contract_df(spark, s3_folder, "ticker")

        # Chỉ giữ các symbol có trong dim_symbols
        target_symbols = list(sym_map.keys())
        df = df.filter(F.col("symbol").isin(target_symbols))

        if df.rdd.isEmpty():
            print(f"[ticker_24h] {fname}: không có symbol nào khớp, bỏ qua.")
            continue

        # Map symbol_code → symbol_id qua broadcast join
        sym_rows = [(k, v) for k, v in sym_map.items()]
        sym_schema = StructType([
            StructField("symbol_code", StringType()),
            StructField("symbol_id",   LongType()),
        ])
        sym_lkp = spark.createDataFrame(sym_rows, schema=sym_schema)

        df = (
            df.join(F.broadcast(sym_lkp), df.symbol == sym_lkp.symbol_code, "inner")
            .select(
                F.col("symbol_id"),
                F.lit(snapshot_ts).cast("timestamp").alias("snapshot_time"),
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

        all_dfs.append(df)
        print(f"[ticker_24h] Đọc {fname} (snapshot_time={snapshot_ts})")

    if not all_dfs:
        print("[ticker_24h] Không có file nào, bỏ qua.")
        return

    final_df = all_dfs[0]
    for d in all_dfs[1:]:
        final_df = final_df.union(d)

    row_count = final_df.count()
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

    print(f"[ticker_24h] Đã load {row_count} rows vào fact_ticker_24h_snapshots")
