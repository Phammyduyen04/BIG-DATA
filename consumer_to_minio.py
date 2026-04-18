"""
Redpanda → MinIO Consumer
===========================
Subscribes to 3 Redpanda topics, batches messages in memory,
and flushes them as JSON Lines files to MinIO.

Topics consumed:
  - binance.kline.1m.raw  → raw/klines/interval=1m/date=.../symbol=.../hour=.../part-XXXXX.json
  - binance.ticker.raw    → raw/ticker/date=.../symbol=.../hour=.../part-XXXXX.json
  - binance.trade.raw     → raw/trades/date=.../symbol=.../hour=.../part-XXXXX.json

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
from collections import defaultdict
from datetime import datetime, timezone
from dotenv import load_dotenv

import boto3
from botocore.config import Config as BotoConfig
from kafka import KafkaConsumer

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ─────────────────────── CONFIG ───────────────────────────────
load_dotenv()

REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "localhost:19092")
GROUP_ID         = "minio-consumer-group"

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "binance")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"

BATCH_SIZE              = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_INTERVAL_SECONDS  = int(os.getenv("FLUSH_INTERVAL_SECONDS", "60"))

TOPICS = [
    "binance.kline.1m.raw",
    "binance.ticker.raw",
    "binance.trade.raw",
]

# Topic → MinIO prefix mapping
TOPIC_PREFIX_MAP = {
    "binance.kline.1m.raw": "raw/klines/interval=1m",
    "binance.ticker.raw":   "raw/ticker",
    "binance.trade.raw":    "raw/trades",
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

        # Stats
        self.total_flushed  = 0
        self.total_messages = 0
        self.total_files    = 0

    def add(self, topic: str, message: dict):
        """Add a message to the appropriate buffer."""
        msg = self._restore_contract(topic, message)

        symbol   = msg.get("symbol", "UNKNOWN")
        event_ts = msg.get("event_time") or int(time.time() * 1000)

        # Derive date and hour from event time
        dt = datetime.fromtimestamp(event_ts / 1000, tz=timezone.utc)
        date_str = dt.strftime("%Y-%m-%d")
        hour_str = dt.strftime("%H")

        key = (topic, symbol, date_str, hour_str)

        with self._lock:
            self._buffers[key].append(msg)
            self.total_messages += 1

            # Initialize last_flush if new key
            if key not in self._last_flush:
                self._last_flush[key] = time.time()

            # Check if we should flush
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

    def _flush_buffer(self, key: tuple):
        """Flush one buffer to MinIO. Must be called with lock held."""
        topic, symbol, date_str, hour_str = key
        messages = self._buffers.pop(key, [])
        if not messages:
            return

        # Increment part counter
        self._part_counters[key] += 1
        part_num = self._part_counters[key]

        # Build object key
        prefix = TOPIC_PREFIX_MAP.get(topic, f"raw/{topic}")
        object_key = (
            f"{prefix}/date={date_str}/symbol={symbol}/"
            f"hour={hour_str}/part-{part_num:05d}.json"
        )

        # Build JSON Lines content
        lines = [json.dumps(msg, default=str) for msg in messages]
        content = "\n".join(lines) + "\n"
        content_bytes = content.encode("utf-8")

        # Upload to MinIO
        try:
            self.s3_client.upload_fileobj(
                Fileobj=io.BytesIO(content_bytes),
                Bucket=self.bucket,
                Key=object_key,
                ExtraArgs={"ContentType": "application/x-ndjson"},
            )
            self.total_flushed += len(messages)
            self.total_files += 1
            self._last_flush[key] = time.time()

            log.info(
                f"[Flush] {object_key}  |  {len(messages)} msgs  |  "
                f"{len(content_bytes)/1024:.1f} KB"
            )
        except Exception as e:
            log.error(f"[MinIO Error] Failed to upload {object_key}: {e}")
            # Put messages back into buffer so they're not lost
            self._buffers[key] = messages + self._buffers.get(key, [])

    def stats_summary(self) -> str:
        with self._lock:
            pending = sum(len(v) for v in self._buffers.values())
        return (
            f"Messages: received={self.total_messages:,}  "
            f"flushed={self.total_flushed:,}  "
            f"pending={pending:,}  "
            f"files={self.total_files:,}"
        )

    def _restore_contract(self, topic: str, msg: dict) -> dict:
        """
        Storage Contract Guard: Translates internal collector fields (snake_case)
        back to the original Binance/Downstream Spark ETL expectations (camelCase/Specific names).
        """
        m = msg.copy()
        
        if topic == "binance.ticker.raw":
            # Map snake_case -> camelCase expected by fact_ticker_24h_snapshots ETL
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
                "num_trades":         "count", # Critical: match etl_ticker_24h.py
            }
            for old, new in mapping.items():
                if old in m:
                    m[new] = m.pop(old)
                    
        elif topic == "binance.kline.1m.raw":
            # Map specific kline fields
            if "taker_buy_quote_volume" in m:
                m["taker_buy_quote_vol"] = m.pop("taker_buy_quote_volume")
        
        # Trades already have quote_qty and is_buyer_maker from ws_collector
        return m
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
    try:
        for message in consumer:
            if shutdown_event.is_set():
                break

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
