"""
Spark Structured Streaming: Redpanda → fact_klines (multi-interval)

Luồng:
  Redpanda topic "klines_1m"
    └─ parse JSON
    └─ tumbling window aggregate → 5m, 15m, 30m, 1h, 4h, 1d
    └─ foreachBatch → upsert vào fact_klines

Message JSON trên topic klines_1m (ví dụ):
  {
    "symbol_id":   1,
    "open_time":   "2026-04-08T14:13:00",
    "close_time":  "2026-04-08T14:13:59.999",
    "open_price":  "71516.48",
    "high_price":  "71565.60",
    "low_price":   "71516.48",
    "close_price": "71560.53",
    "volume_base": "10.09267",
    "volume_quote": "722031.79",
    "num_trades":  2135,
    "taker_buy_quote_volume": "640415.55"
  }

Chạy:
  spark-submit \\
    --master local[*] \\
    --packages org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \\
    streaming_klines.py
"""
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DecimalType, TimestampType,
)

from etl_utils import execute_sql

# ── Cấu hình ──────────────────────────────────────────────
REDPANDA_BROKERS  = os.environ.get("KAFKA_BROKERS",   "redpanda:9092")
KAFKA_TOPIC       = os.environ.get("KAFKA_TOPIC_1M",  "klines_1m")
CHECKPOINT_BASE   = os.environ.get("CHECKPOINT_DIR",  "/tmp/checkpoints/klines")
JDBC_URL          = os.environ.get("JDBC_URL",        "jdbc:postgresql://localhost:5432/crypto_dw")
JDBC_PROPS        = {
    "user":     os.environ.get("JDBC_USER",     "dwuser"),
    "password": os.environ.get("JDBC_PASSWORD", "dwpassword"),
    "driver":   "org.postgresql.Driver",
    "batchsize": "20000",
}

# Schema của JSON message
_MSG_SCHEMA = StructType([
    StructField("symbol_id",              LongType()),
    StructField("open_time",              TimestampType()),
    StructField("close_time",             TimestampType()),
    StructField("open_price",             DecimalType(30, 12)),
    StructField("high_price",             DecimalType(30, 12)),
    StructField("low_price",              DecimalType(30, 12)),
    StructField("close_price",            DecimalType(30, 12)),
    StructField("volume_base",            DecimalType(30, 12)),
    StructField("volume_quote",           DecimalType(30, 12)),
    StructField("num_trades",             LongType()),
    StructField("taker_buy_quote_volume", DecimalType(30, 12)),
])

# Intervals cần tính từ 1m
TARGET_INTERVALS = [
    ("5m",  "5  minutes"),
    ("15m", "15 minutes"),
    ("30m", "30 minutes"),
    ("1h",  "60 minutes"),
    ("4h",  "240 minutes"),
    ("1d",  "1440 minutes"),
]


# ── foreachBatch writer ────────────────────────────────────
def _make_batch_writer(interval_code: str, spark):
    """Tạo hàm write cho mỗi interval, dùng với foreachBatch."""

    def write_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        staging = f"staging_stream_klines_{interval_code.replace(' ', '')}"

        batch_df.write.jdbc(
            JDBC_URL, staging,
            mode="overwrite",
            properties=JDBC_PROPS,
        )

        execute_sql(spark, JDBC_URL, JDBC_PROPS, f"""
            INSERT INTO fact_klines (
                symbol_id, interval_code, open_time, close_time,
                open_price, high_price, low_price, close_price,
                volume_base, volume_quote, num_trades, taker_buy_quote_volume
            )
            SELECT
                symbol_id, '{interval_code}', open_time, close_time,
                open_price, high_price, low_price, close_price,
                volume_base, volume_quote, num_trades, taker_buy_quote_volume
            FROM {staging}
            ON CONFLICT (symbol_id, interval_code, open_time) DO UPDATE SET
                close_time             = EXCLUDED.close_time,
                close_price            = EXCLUDED.close_price,
                high_price             = GREATEST(fact_klines.high_price, EXCLUDED.high_price),
                low_price              = LEAST(fact_klines.low_price,     EXCLUDED.low_price),
                volume_base            = EXCLUDED.volume_base,
                volume_quote           = EXCLUDED.volume_quote,
                num_trades             = EXCLUDED.num_trades,
                taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume
        """)
        execute_sql(spark, JDBC_URL, JDBC_PROPS, f"DROP TABLE IF EXISTS {staging}")
        print(f"[stream] batch {batch_id} → {interval_code} upserted")

    return write_batch


# ── Main ───────────────────────────────────────────────────
def main():
    spark = (
        SparkSession.builder
        .appName("CryptoDW-KlineStreaming")
        .config("spark.sql.session.timeZone", "UTC")
        # Giảm shuffle partitions cho streaming
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Đọc từ Redpanda/Kafka ────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", REDPANDA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        # Tối đa 10k message/trigger để tránh quá tải
        .option("maxOffsetsPerTrigger", "10000")
        .load()
    )

    # Parse JSON value
    df_1m = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), _MSG_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("symbol_id").isNotNull() & F.col("open_time").isNotNull())
    )

    # ── Tạo 1 streaming query cho mỗi target interval ────
    queries = []

    for interval_code, window_duration in TARGET_INTERVALS:
        df_agg = (
            df_1m
            # Watermark: chấp nhận dữ liệu trễ tối đa 2 phút
            .withWatermark("open_time", "2 minutes")
            .groupBy(
                F.col("symbol_id"),
                F.window(F.col("open_time"), window_duration).alias("w"),
            )
            .agg(
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
            .select(
                "symbol_id",
                F.col("w.start").alias("open_time"),
                "close_time", "open_price", "high_price", "low_price", "close_price",
                "volume_base", "volume_quote", "num_trades", "taker_buy_quote_volume",
            )
        )

        q = (
            df_agg.writeStream
            .outputMode("update")          # Emit khi window có thay đổi
            .foreachBatch(_make_batch_writer(interval_code, spark))
            .trigger(processingTime="30 seconds")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/{interval_code}")
            .start()
        )
        queries.append(q)
        print(f"[stream] Query {interval_code} started")

    # Chờ tất cả queries
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
