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
        # Hadoop S3A configurations
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if config.MINIO_SECURE else "false")
        # Optimization for K8s
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.extraJavaOptions", "-Divy.home=/tmp/ivy_cache")
    )

    return builder.getOrCreate()


def emulate_listdir(spark, s3a_path):
    """
    Compatibility Adapter: Giả lập os.listdir bằng Hadoop FileSystem API.
    Dùng để liệt kê Symbol dirs hoặc files trên S3A.
    """
    sc = spark.sparkContext
    # Truy cập Hadoop Path và FileSystem qua JVM
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    conf = sc._jsc.hadoopConfiguration()
    
    # Lấy FileSystem cho S3A URI
    uri = sc._gateway.jvm.java.net.URI(s3a_path)
    fs = FileSystem.get(uri, conf)
    
    # Liệt kê status và trích xuất tên
    statuses = fs.listStatus(Path(s3a_path))
    return sorted([status.getPath().getName() for status in statuses])


def load_contract_df(spark, s3a_path, module_type):
    """
    Compatibility Adapter: Đọc JSON và khôi phục đúng Contract (Column Names).
    Đảm bảo 100% processing logic không bị lỗi do mismatch tên cột JSON.
    """
    df = spark.read.json(s3a_path)
    if df.rdd.isEmpty():
        return df

    if module_type == "klines":
        # Restore contraction: taker_buy_quote_volume -> taker_buy_quote_vol
        if "taker_buy_quote_volume" in df.columns:
            df = df.withColumnRenamed("taker_buy_quote_volume", "taker_buy_quote_vol")
            
    elif module_type == "ticker":
        # Restore camelCase contract for Ticker 24h
        mapping = {
            "price_change": "priceChange",
            "price_change_pct": "priceChangePercent",
            "last_price": "lastPrice",
            "high_price": "highPrice",
            "low_price": "lowPrice",
            "volume": "volume",
            "quote_volume": "quoteVolume",
            "num_trades": "count"
        }
        for old_col, new_col in mapping.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)
                
    elif module_type == "trades":
        # Restore trade semantics expected by etl_trades.py
        mapping = {
            "quote_volume": "quote_qty",
            "event_time": "time"
        }
        for old_col, new_col in mapping.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)

    return df
