"""
Utility: thực thi raw SQL qua JDBC driver đã load sẵn trong Spark JVM.
Dùng cho staging → upsert (INSERT ... ON CONFLICT) vì Spark JDBC
writer không hỗ trợ ON CONFLICT natively.
"""


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


def get_spark_session(app_name="CryptoDW-ETL"):
    """
    Khởi tạo SparkSession với cấu hình S3A để kết nối tới MinIO.
    Cấu hình được lấy từ biến môi trường (load từ .env.processing).
    """
    import os
    from pyspark.sql import SparkSession

    # Lấy thông tin từ environment
    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        # Hadoop S3A configurations
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if secure else "false")
    )

    return builder.getOrCreate()
