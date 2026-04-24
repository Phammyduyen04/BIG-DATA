"""
Utility: thực thi raw SQL qua JDBC driver đã load sẵn trong Spark JVM.
Dùng cho staging → upsert (INSERT ... ON CONFLICT) vì Spark JDBC
writer không hỗ trợ ON CONFLICT natively.
"""
import os

def execute_sql(spark, jdbc_url, jdbc_props, sql: str) -> None:
    """Chạy một câu SQL bất kỳ qua kết nối JDBC từ JVM của Spark driver."""
    jvm = spark._jvm
    java_props = jvm.java.util.Properties()
    for k, v in jdbc_props.items():
        java_props.setProperty(k, v)

    conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, java_props)
    try:
        conn.setAutoCommit(True)
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


def load_symbol_map(spark, jdbc_url, jdbc_props) -> dict:
    """Trả về dict {symbol_code: symbol_id} từ dim_symbols."""
    df = spark.read.jdbc(jdbc_url, "dim_symbols", properties=jdbc_props)
    return {row.symbol_code: row.symbol_id for row in df.collect()}


def get_spark_session(app_name=None):
    """
    Khởi tạo SparkSession với cấu hình S3A để kết nối tới MinIO.
    Cấu hình được lấy từ file config.py trung tâm.
    """
    from pyspark.sql import SparkSession
    import config

    # Nếu không truyền app_name, dùng mặc định từ config
    final_app_name = app_name or config.SPARK_APP_NAME

    builder = (
        SparkSession.builder
        .appName(final_app_name)
        .config("spark.sql.session.timeZone", "UTC")
        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if config.MINIO_SECURE else "false")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "30000")
        # Fast multipart upload — reduces write latency to MinIO
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864")
        .config("spark.hadoop.fs.s3a.threads.max", "50")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "5")
        # Small-file merging: pack nhiều Parquet nhỏ vào ít partition lớn
        .config("spark.sql.files.maxPartitionBytes", "268435456")
        .config("spark.sql.files.openCostInBytes", "4194304")
        # Parallel partition discovery — list 250k S3 dirs song song thay vì tuần tự
        # Threshold=1: bật ngay từ 1 path; parallelism=32: 32 threads list đồng thời
        .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "1")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "32")
        # Adaptive Query Execution — coalesces shuffle partitions dynamically
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Higher starting value lets AQE coalesce down to actual optimal
        .config("spark.sql.shuffle.partitions", "200")
        # Mixed-schema tolerance: old files have DOUBLE, new files have STRING.
        # The vectorized reader cannot convert DOUBLE→STRING; the row-based
        # reader (ParquetRecordReader) handles this gracefully.
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        # Bỏ qua file lỗi (PlainDoubleDictionary UnsupportedOperationException)
        # thay vì fail toàn bộ job. File bị skip sẽ có warning trong log.
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        # Faster serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Network resilience for Tailscale/VM cluster
        .config("spark.network.timeout", "600s")
        .config("spark.rpc.askTimeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.driver.extraJavaOptions", "-Divy.home=/tmp/ivy_cache -XX:+UseG1GC")
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:G1HeapRegionSize=4m")
    )

    return builder.getOrCreate()


def emulate_listdir(spark, s3a_path):
    """
    Compatibility Adapter: Giả lập os.listdir bằng Hadoop FileSystem API.
    Dùng để liệt kê nội dung trực tiếp của một prefix (không recursive).
    """
    sc = spark.sparkContext
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    conf = sc._jsc.hadoopConfiguration()
    
    uri = sc._gateway.jvm.java.net.URI(s3a_path)
    fs = FileSystem.get(uri, conf)
    
    statuses = fs.listStatus(Path(s3a_path))
    if not statuses: return []
    return sorted([status.getPath().getName() for status in statuses])


def discover_symbols(spark, base_path, pattern="interval=*/date=*/symbol=*"):
    """
    Hadoop S3A Discovery: Tìm các unique symbols từ layout partitioned.
    Mặc định quét: klines/interval=1m/date=*/symbol=BTCUSDT
    """
    import re
    sc = spark.sparkContext
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    conf = sc._jsc.hadoopConfiguration()
    fs = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(sc._gateway.jvm.java.net.URI(base_path), conf)
    
    glob_pattern = f"{base_path.rstrip('/')}/{pattern}"
    status_list = fs.globStatus(Path(glob_pattern))
    
    if not status_list:
        return []
    
    symbols = set()
    for status in status_list:
        path_str = status.getPath().toString()
        match = re.search(r"symbol=([^/]+)", path_str)
        if match:
            symbols.add(match.group(1))
            
    return sorted(list(symbols))


def _collect_parquet_paths(spark, s3a_path):
    """
    2-phase parallel listing:
      Phase 1: listStatus(root) — 1 API call, lấy danh sách top-level subdirs (date=*)
      Phase 2: 32 Python threads, mỗi thread listFiles(subdir, recursive=True)
               Mỗi subdir ~100 files → 1 API call không phân trang → chạy song song
    Kết quả: ~2-3s thay vì 29s (sequential pagination qua Tailscale).
    S3AFileSystem là thread-safe nên dùng chung 1 instance an toàn.
    """
    import concurrent.futures

    sc = spark.sparkContext
    jvm = sc._gateway.jvm
    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem
    uri = jvm.java.net.URI(s3a_path)
    fs = FileSystem.get(uri, sc._jsc.hadoopConfiguration())

    # Phase 1: list immediate children (1 API call)
    top_statuses = fs.listStatus(Path(s3a_path))
    sub_dirs = [
        s.getPath().toString()
        for s in top_statuses
        if s.isDirectory() and not s.getPath().getName().startswith("_")
    ]

    def _list_subdir(subdir):
        it = fs.listFiles(Path(subdir), True)
        result = []
        while it.hasNext():
            p = it.next().getPath().toString()
            if p.endswith(".parquet"):
                result.append(p)
        return result

    if not sub_dirs:
        # Flat structure (no subdirs): fall back to single recursive listing
        return _list_subdir(s3a_path)

    # Phase 2: parallel per-subdir listing
    all_paths = []
    workers = min(32, len(sub_dirs))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for partial in pool.map(_list_subdir, sub_dirs):
            all_paths.extend(partial)
    return all_paths


def load_contract_df(spark, s3a_path, module_type, schema=None):
    """
    Compatibility Adapter: Fast S3 read + contract normalization.

    Khi có explicit schema:
      spark.read.schema(schema).option("recursiveFileLookup", "true").parquet(dir)
      → Spark gọi fs.listFiles(recursive=True) nội bộ = flat S3 listing (~20s cho 2301 files)
      → KHÔNG gọi getFileStatus riêng lẻ từng file (tránh 2301 HEAD requests × 250ms = 575s)
      → KHÔNG đọc Parquet footer → tránh event_time INT/BIGINT CANNOT_MERGE_SCHEMAS
      → File DOUBLE-encoded bị bỏ qua nhờ ignoreCorruptFiles=true (set ở SparkSession)

    Fallback (schema=None): mergeSchema=True với directory path.
    """
    from pyspark.sql import functions as F
    import time

    _t = time.time()

    # Kiểm tra path tồn tại: 1 non-recursive listStatus call (không list toàn bộ)
    sc = spark.sparkContext
    jvm = sc._gateway.jvm
    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem
    uri = jvm.java.net.URI(s3a_path)
    fs = FileSystem.get(uri, sc._jsc.hadoopConfiguration())
    try:
        top = fs.listStatus(Path(s3a_path))
        if not top:
            print(f"[ContractAdapter] Path rong: {s3a_path}")
            return None
    except Exception as e:
        print(f"[ContractAdapter] Path khong ton tai: {s3a_path} — {e}")
        return None

    if schema is not None:
        print(f"[ContractAdapter] Path OK ({time.time()-_t:.1f}s) — explicit schema + recursiveFileLookup ...")
        # recursiveFileLookup=true: Spark dùng listFiles(recursive=True) = flat listing
        # Không truyền explicit paths → không trigger N x getFileStatus HEAD requests
        df = (spark.read
              .schema(schema)
              .option("recursiveFileLookup", "true")
              .parquet(s3a_path))
    else:
        print(f"[ContractAdapter] Path OK ({time.time()-_t:.1f}s) — mergeSchema ...")
        df = spark.read.option("mergeSchema", "true").parquet(s3a_path)

    if module_type == "klines":
        if "taker_buy_quote_volume" in df.columns and "taker_buy_quote_vol" in df.columns:
            df = (df.withColumn("taker_buy_quote_vol",
                      F.coalesce(F.col("taker_buy_quote_vol"), F.col("taker_buy_quote_volume")))
                  .drop("taker_buy_quote_volume"))
        elif "taker_buy_quote_volume" in df.columns:
            df = df.withColumnRenamed("taker_buy_quote_volume", "taker_buy_quote_vol")

    elif module_type == "ticker":
        mapping = {
            "price_change":         "priceChange",
            "price_change_percent": "priceChangePercent",
            "last_price":           "lastPrice",
            "high_price":           "highPrice",
            "low_price":            "lowPrice",
            "volume":               "volume",
            "quote_volume":         "quoteVolume",
            "num_trades":           "count",
        }
        for old_col, new_col in mapping.items():
            if old_col in df.columns:
                if new_col in df.columns:
                    df = (df.withColumn(new_col, F.coalesce(F.col(new_col), F.col(old_col)))
                          .drop(old_col))
                else:
                    df = df.withColumnRenamed(old_col, new_col)

    elif module_type == "trades":
        current_cols = df.columns
        if "quote_qty" not in current_cols and "quote_volume" in current_cols:
            df = df.withColumnRenamed("quote_volume", "quote_qty")
        elif "quote_qty" in current_cols and "quote_volume" in current_cols:
            df = (df.withColumn("quote_qty", F.coalesce(F.col("quote_qty"), F.col("quote_volume")))
                  .drop("quote_volume"))
        if "symbol" not in df.columns:
            print("[ContractAdapter] WARNING: 'symbol' column missing. Relying on partition path.")

    return df
