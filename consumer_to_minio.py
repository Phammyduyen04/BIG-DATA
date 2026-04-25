"""
Redpanda → MinIO Consumer (v1.2.1)
==================================
Subscribes to 3 Redpanda topics, batches messages in memory,
and flushes them as Bronze (JSON) and Silver (Parquet) files to MinIO.

Topics consumed:
  - binance.kline.1m.raw  → bronze/klines/interval=1m/... and silver/klines/interval=1m/...
  - binance.ticker.raw    → bronze/ticker/... and silver/ticker/...
  - binance.trade.raw     → bronze/trades/... and silver/trades/...

Configuration via .env:
  REDPANDA_BROKERS, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
  MINIO_BUCKET, BATCH_SIZE, FLUSH_INTERVAL_SECONDS

Usage:
  python consumer_to_minio.py
"""

import os
import io
import sys
import json
import time
import signal
import logging
import threading
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from datetime import datetime, timezone
from dotenv import load_dotenv

import boto3
from botocore.config import Config as BotoConfig
from kafka import KafkaConsumer

from silver_schema import SILVER_PREFIX_MAP, coerce_numeric_fields_to_string

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ─────────────────────── CONFIG ───────────────────────────────
load_dotenv()

REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "localhost:19092")
GROUP_ID         = os.getenv("KAFKA_GROUP_ID", "minio-consumer-group-v2")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "binance")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"

BATCH_SIZE              = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_INTERVAL_SECONDS  = int(os.getenv("FLUSH_INTERVAL_SECONDS", "60"))
# INGESTION_MODE: DUAL, BRONZE_ONLY, SILVER_ONLY
INGESTION_MODE          = os.getenv("INGESTION_MODE", "DUAL").upper()

TOPICS = [
    "binance.kline.1m.raw",
    "binance.ticker.raw",
    "binance.trade.raw",
]

# Bronze Prefix Mapping (Legacy JSON)
BRONZE_PREFIX_MAP = {
    "binance.kline.1m.raw": "bronze/klines/interval=1m",
    "binance.ticker.raw":   "bronze/ticker",
    "binance.trade.raw":    "bronze/trades",
}

LOG_FILE = "logs/consumer_minio.log"
# ───────────────────────────────────────────────────────────────


# ─────────────────────── LOGGING ──────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)
# ───────────────────────────────────────────────────────────────


# ─────────────────────── MinIO CLIENT ─────────────────────────
def create_s3_client():
    protocol = "https" if MINIO_SECURE else "http"
    endpoint = f"{protocol}://{MINIO_ENDPOINT}"
    log.info(f"Connecting to MinIO at {endpoint}...")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )
    return client


def ensure_bucket(s3_client, bucket: str):
    """Create bucket if it doesn't exist."""
    try:
        s3_client.head_bucket(Bucket=bucket)
        log.info(f"Bucket '{bucket}' exists.")
    except Exception:
        log.info(f"Creating bucket '{bucket}'...")
        s3_client.create_bucket(Bucket=bucket)
        log.info(f"Bucket '{bucket}' created.")
# ───────────────────────────────────────────────────────────────


# ─────────────────────── BUFFER MANAGER ───────────────────────
class BufferManager:
    """
    Manages in-memory message buffers keyed by (topic, symbol, hour).
    Flushes to MinIO when batch size or time threshold is reached.
    """

    def __init__(self, s3_client, bucket: str, batch_size: int, flush_interval: int):
        self.s3_client      = s3_client
        self.bucket         = bucket
        self.batch_size     = batch_size
        self.flush_interval = flush_interval

        # Buffer: (topic, symbol, date_str, hour_str) → list[dict]
        self._buffers: dict[tuple, list[dict]] = defaultdict(list)
        # Part counters: (topic, symbol, date_str, hour_str) → int
        self._part_counters: dict[tuple, int] = defaultdict(int)
        # Last flush time per buffer key
        self._last_flush: dict[tuple, float] = defaultdict(time.time)
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=20)

        # Stats
        self.total_flushed  = 0
        self.total_messages = 0
        self.total_files    = 0

    def add(self, topic: str, message: dict):
        """Add a message to the appropriate buffer with explicit timestamping."""
        msg, event_ts_ms = self._restore_contract(topic, message)

        symbol = msg.get("symbol", "UNKNOWN")

        # Derive date and hour using the explicitly normalized MS timestamp
        dt = datetime.fromtimestamp(event_ts_ms / 1000, tz=timezone.utc)
        date_str = dt.strftime("%Y-%m-%d")
        hour_str = dt.strftime("%H")

        key = (topic, symbol, date_str, hour_str)

        with self._lock:
            self._buffers[key].append(msg)
            self.total_messages += 1

            if key not in self._last_flush:
                self._last_flush[key] = time.time()

            buf_len = len(self._buffers[key])
            elapsed = time.time() - self._last_flush[key]

            if buf_len >= self.batch_size or elapsed >= self.flush_interval:
                self._flush_buffer(key)

    def flush_all(self):
        """Flush all remaining buffers (e.g., on shutdown)."""
        with self._lock:
            keys = list(self._buffers.keys())
            for key in keys:
                if self._buffers[key]:
                    self._flush_buffer(key)
        
        log.info("Waiting for all background uploads to complete...")
        self._executor.shutdown(wait=True)

    def _flush_buffer(self, key: tuple):
        """Flush one buffer to MinIO (Bronze/Silver). Must be called with lock held."""
        topic, symbol, date_str, hour_str = key
        messages = self._buffers.pop(key, [])
        if not messages:
            return

        self._part_counters[key] += 1
        part_num = self._part_counters[key]
        self._last_flush[key] = time.time()
        
        # Offload to executor for concurrent processing
        self._executor.submit(self._execute_flush, key, messages, part_num)

    def _execute_flush(self, key: tuple, messages: list[dict], part_num: int):
        """Actual I/O operation performed in a separate thread."""
        if INGESTION_MODE in ("DUAL", "BRONZE_ONLY"):
            self._flush_bronze(key, messages, part_num)

        if INGESTION_MODE in ("DUAL", "SILVER_ONLY"):
            self._flush_silver(key, messages, part_num)
        
        with self._lock:
            self.total_flushed += len(messages)

    def _flush_bronze(self, key: tuple, messages: list[dict], part_num: int):
        """Standard JSON Lines flush to Bronze layer."""
        topic, symbol, date_str, hour_str = key
        prefix = BRONZE_PREFIX_MAP.get(topic, f"bronze/{topic}")
        object_key = f"{prefix}/date={date_str}/symbol={symbol}/hour={hour_str}/part-{part_num:05d}.json"

        lines = [json.dumps(msg, default=str) for msg in messages]
        content = "\n".join(lines) + "\n"
        content_bytes = content.encode("utf-8")

        try:
            self.s3_client.upload_fileobj(
                Fileobj=io.BytesIO(content_bytes),
                Bucket=self.bucket,
                Key=object_key,
                ExtraArgs={"ContentType": "application/x-ndjson"},
            )
            self.total_files += 1
            log.info(f"[Bronze] {object_key} | {len(messages)} msgs")
        except Exception as e:
            log.error(f"[Bronze Error] {object_key}: {e}")

    def _flush_silver(self, key: tuple, messages: list[dict], part_num: int):
        """Optimized Parquet flush to Silver layer."""
        topic, symbol, date_str, hour_str = key
        prefix = SILVER_PREFIX_MAP.get(topic, f"silver/{topic}")
        object_key = f"{prefix}/date={date_str}/symbol={symbol}/hour={hour_str}/part-{part_num:05d}.parquet"

        try:
            coerce_numeric_fields_to_string(topic, messages)
            table = pa.Table.from_pylist(messages)
            buf = io.BytesIO()
            pq.write_table(table, buf, compression='snappy')
            content_bytes = buf.getvalue()

            self.s3_client.upload_fileobj(
                Fileobj=io.BytesIO(content_bytes),
                Bucket=self.bucket,
                Key=object_key,
                ExtraArgs={"ContentType": "application/x-parquet"},
            )
            self.total_files += 1
            log.info(f"[Silver] {object_key} | {len(messages)} msgs | {len(content_bytes)/1024:.1f} KB")
        except Exception as e:
            log.error(f"[Silver Error] {object_key}: {e}")

    def stats_summary(self) -> str:
        with self._lock:
            pending = sum(len(v) for v in self._buffers.values())
        return (
            f"Messages: received={self.total_messages:,}  "
            f"flushed={self.total_flushed:,}  "
            f"pending={pending:,}  "
            f"files={self.total_files:,}"
        )

    def _restore_contract(self, topic: str, msg: dict) -> tuple[dict, int]:
        """
        Storage Contract Guard: Topic-specific timestamp normalization and field mapping.
        Returns (mapped_message, timestamp_ms).
        """
        m = msg.copy()
        ts_ms = None
        
        if topic == "binance.ticker.raw":
            # Ticker: Use 'event_time' (E) for partitioning
            ts_ms = m.get("event_time") or m.get("E")
            mapping = {
                "price_change":       "priceChange",
                "price_change_pct":   "priceChangePercent",
                "weighted_avg_price": "weightedAvgPrice",
                "prev_close_price":   "prevClosePrice",
                "last_price":         "lastPrice",
                "last_qty":           "lastQty",
                "bid_price":          "bidPrice",
                "ask_price":          "askPrice",
                "open_price":         "openPrice",
                "high_price":         "highPrice",
                "low_price":          "lowPrice",
                "volume":             "volume",
                "quote_volume":       "quoteVolume",
                "num_trades":         "count",
            }
            for old, new in mapping.items():
                if old in m: m[new] = m.pop(old)
                    
        elif topic == "binance.kline.1m.raw":
            # Kline: Use 'open_time' (t) for partitioning
            ts_ms = m.get("open_time") or m.get("t")
            if "taker_buy_quote_volume" in m:
                m["taker_buy_quote_vol"] = m.pop("taker_buy_quote_volume")
        
        elif topic == "binance.trade.raw":
            # Trade: Use 'event_time' (E) or 'time' (T) for partitioning
            ts_ms = m.get("event_time") or m.get("time") or m.get("E") or m.get("T")
            if "event_time" in m and "time" not in m:
                m["time"] = m.pop("event_time")
        
        # Explicit unit check: fallback to current time if missing
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)
        
        return m, int(ts_ms)
# ───────────────────────────────────────────────────────────────


# ─────────────────────── TIMED FLUSHER ────────────────────────
def timed_flusher(buffer_mgr: BufferManager, interval: float, shutdown_event: threading.Event):
    """Background thread that periodically forces flush of stale buffers."""
    while not shutdown_event.is_set():
        shutdown_event.wait(interval)
        if not shutdown_event.is_set():
            # Check and flush any buffers that exceeded time threshold
            with buffer_mgr._lock:
                keys_to_flush = []
                now = time.time()
                for key, last_time in list(buffer_mgr._last_flush.items()):
                    if key in buffer_mgr._buffers and buffer_mgr._buffers[key]:
                        if now - last_time >= buffer_mgr.flush_interval:
                            keys_to_flush.append(key)

                for key in keys_to_flush:
                    buffer_mgr._flush_buffer(key)

            log.info(f"[Stats] {buffer_mgr.stats_summary()}")
# ───────────────────────────────────────────────────────────────


# ─────────────────────── MAIN ─────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("Redpanda → MinIO Consumer")
    log.info(f"  Broker         : {REDPANDA_BROKERS}")
    log.info(f"  Group ID       : {GROUP_ID}")
    log.info(f"  Topics         : {', '.join(TOPICS)}")
    log.info(f"  MinIO endpoint : {MINIO_ENDPOINT}")
    log.info(f"  MinIO bucket   : {MINIO_BUCKET}")
    log.info(f"  Batch size     : {BATCH_SIZE}")
    log.info(f"  Flush interval : {FLUSH_INTERVAL_SECONDS}s")
    log.info(f"  Ingestion Mode : {INGESTION_MODE}")
    log.info("=" * 60)

    # Init MinIO
    s3_client = create_s3_client()
    ensure_bucket(s3_client, MINIO_BUCKET)

    # Init buffer manager
    buffer_mgr = BufferManager(
        s3_client=s3_client,
        bucket=MINIO_BUCKET,
        batch_size=BATCH_SIZE,
        flush_interval=FLUSH_INTERVAL_SECONDS,
    )

    # Init Kafka consumer
    log.info("Connecting to Redpanda...")
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=REDPANDA_BROKERS.split(","),
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        # Resilience tuning
        session_timeout_ms=60000,
        max_poll_interval_ms=600000, # 10 mins for heavy writes
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        key_deserializer=lambda x: x.decode("utf-8") if x else None,
        consumer_timeout_ms=-1,
    )
    log.info("Connected to Redpanda. Listening for messages...")

    # Graceful shutdown
    shutdown_event = threading.Event()

    def handle_signal(sig, frame):
        log.info("\n[SHUTDOWN] Stopping consumer...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Start timed flusher thread
    flush_thread = threading.Thread(
        target=timed_flusher,
        args=(buffer_mgr, FLUSH_INTERVAL_SECONDS / 2, shutdown_event),
        daemon=True,
    )
    flush_thread.start()

    # Consume loop
    MAX_PENDING = 500000
    try:
        for message in consumer:
            if shutdown_event.is_set():
                break

            # Backpressure: Throttle if pending messages in memory exceed limit
            while (buffer_mgr.total_messages - buffer_mgr.total_flushed) > MAX_PENDING:
                log.warning(f"[Backpressure] {buffer_mgr.total_messages - buffer_mgr.total_flushed} messages pending. Pausing poll for 5s...")
                time.sleep(5)
                if shutdown_event.is_set(): break

            topic = message.topic
            value = message.value

            if isinstance(value, dict):
                buffer_mgr.add(topic, value)

    except Exception as e:
        log.error(f"[Consumer Error] {e}")
    finally:
        # Flush remaining data
        log.info("Flushing remaining buffers...")
        buffer_mgr.flush_all()

        consumer.close()
        log.info(f"\n[FINAL] {buffer_mgr.stats_summary()}")
        log.info("Consumer stopped.")


if __name__ == "__main__":
    main()
