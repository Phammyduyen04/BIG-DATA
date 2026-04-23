import gc
import io
import os
import sys
import json
import time
import signal
import logging
import threading
import traceback
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from collections import defaultdict
from dotenv import load_dotenv

import boto3
from botocore.config import Config as BotoConfig
from kafka import KafkaConsumer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from silver_schema import coerce_numeric_fields_to_string

load_dotenv()

BROKERS    = os.getenv("REDPANDA_BROKERS", "localhost:19092")
MINIO_URL  = f"http://{os.getenv('MINIO_ENDPOINT', '100.74.195.110:30900')}"
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET     = os.getenv("MINIO_BUCKET", "binance")
GROUP_ID   = os.getenv("KAFKA_GROUP_ID", "rescue-group-final-v3")

BATCH_SIZE       = 100
MAX_RAM_MESSAGES = 100_000

TOPICS = ["binance.kline.1m.raw", "binance.ticker.raw", "binance.trade.raw"]

SILVER_MAP = {
    "binance.kline.1m.raw": "silver/klines/interval=1m",
    "binance.ticker.raw":   "silver/ticker",
    "binance.trade.raw":    "silver/trades",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("RescueIngest")


def create_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=30,
            read_timeout=30,
            retries={"max_attempts": 3},
        ),
        region_name="us-east-1",
    )


class RescueManager:
    def __init__(self, s3):
        self.s3 = s3
        self.lock = threading.Lock()
        self.buffers = defaultdict(list)
        self.received = 0
        self.flushed = 0
        self.files = 0
        self.part_counts = defaultdict(int)

    def add(self, topic, msg):
        ts_ms = msg.get("E") or msg.get("T") or int(time.time() * 1000)
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        key = (topic, msg.get("symbol", "UNKNOWN"), dt.strftime("%Y-%m-%d"), dt.strftime("%H"))
        with self.lock:
            self.buffers[key].append(msg)
            self.received += 1
            if len(self.buffers[key]) >= BATCH_SIZE:
                self.flush(key)

    def flush(self, key):
        msgs = self.buffers.pop(key, [])
        if not msgs:
            return
        self.part_counts[key] += 1
        self._upload(key, msgs, self.part_counts[key])

    def _upload(self, key, msgs, part_num):
        topic, symbol, d_str, h_str = key
        prefix = SILVER_MAP.get(topic, f"silver/{topic}")
        obj_key = f"{prefix}/date={d_str}/symbol={symbol}/hour={h_str}/part-{part_num:05d}.parquet"

        while True:
            try:
                coerce_numeric_fields_to_string(topic, msgs)
                buf = io.BytesIO()
                pq.write_table(pa.Table.from_pylist(msgs), buf, compression="snappy")
                data = buf.getvalue()

                t0 = time.time()
                create_s3().put_object(Bucket=BUCKET, Key=obj_key, Body=data)
                latency = time.time() - t0

                last_ts = msgs[-1].get("E") or msgs[-1].get("T") or 0
                last_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

                with self.lock:
                    self.flushed += len(msgs)
                    self.files += 1

                log.info(f"[OK] {obj_key} | {len(msgs)} msgs | {latency:.2f}s | msg_time={last_dt}")

                del buf, data
                gc.collect()
                break
            except Exception as e:
                log.error(f"[RETRY] {obj_key} in 5s: {e}")
                time.sleep(5)

    def flush_all(self):
        with self.lock:
            for k in list(self.buffers.keys()):
                self.flush(k)
        log.info("All buffers flushed.")


def main():
    s3 = create_s3()
    try:
        s3.list_buckets()
        log.info("S3 health check: OK")
    except Exception as e:
        log.error(f"S3 health check FAILED: {e}")
        return

    mgr = RescueManager(s3)
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BROKERS.split(","),
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        max_poll_interval_ms=3_600_000,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        session_timeout_ms=30_000,
    )

    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda s, f: stop_event.set())

    log.info(f"Rescue started. Topics: {TOPICS}")
    try:
        for message in consumer:
            if stop_event.is_set():
                break
            while (mgr.received - mgr.flushed) > MAX_RAM_MESSAGES:
                log.warning(f"RAM backpressure: {mgr.received - mgr.flushed} pending. Waiting...")
                time.sleep(5)
                if stop_event.is_set():
                    break
            mgr.add(message.topic, message.value)
            if mgr.received % 10_000 == 0:
                log.info(f"recv={mgr.received} flushed={mgr.flushed} files={mgr.files}")
    except Exception:
        log.error(traceback.format_exc())
    finally:
        mgr.flush_all()
        consumer.close()
        log.info("Rescue done.")


if __name__ == "__main__":
    main()
