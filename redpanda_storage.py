"""
Redpanda Storage — Thay thế DataStorage để đẩy dữ liệu vào Redpanda thay vì lưu file cục bộ.
----------------------------------------------------------------------------------------
Các topic tương ứng:
  binance.kline.1m.raw
  binance.depth.raw
  binance.ticker.raw
"""

import json
import os
import logging
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Topics
TOPIC_KLINE  = "binance.kline.1m.raw"
TOPIC_TICKER = "binance.ticker.raw"
TOPIC_TRADE  = "binance.trade.raw"


class RedpandaStorage:
    def __init__(self, brokers: str = None):
        self.brokers = brokers or os.getenv("REDPANDA_BROKERS", "localhost:19092")
        logger.info(f"Khởi tạo RedpandaStorage kết nối tới: {self.brokers}")
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.brokers.split(","),
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=5,    # Giảm delay gửi batch
                batch_size=32768
            )
            logger.info("Kết nối Redpanda thành công.")
        except Exception as e:
            logger.error(f"Lỗi kết nối Redpanda: {e}")
            raise

        self._counts = {
            TOPIC_KLINE: 0,
            TOPIC_TICKER: 0,
            TOPIC_TRADE: 0
        }

    def _send(self, topic: str, record: dict):
        try:
            symbol = record.get("symbol", "UNKNOWN")
            self.producer.send(topic, key=symbol, value=record)
            self._counts[topic] += 1
        except Exception as e:
            logger.error(f"Lỗi gửi dữ liệu tới Redpanda ({topic}): {e}")

    # ── public interface (giữ nguyên tên hàm giống DataStorage) ──────────────

    def save_kline(self, record: dict):
        self._send(TOPIC_KLINE, record)

    def save_ticker(self, record: dict):
        self._send(TOPIC_TICKER, record)

    def save_trade(self, record: dict):
        self._send(TOPIC_TRADE, record)

    def summary(self) -> dict[str, int]:
        return {
            "binance.kline.1m.raw": self._counts[TOPIC_KLINE],
            "binance.ticker.raw":   self._counts[TOPIC_TICKER],
            "binance.trade.raw":    self._counts[TOPIC_TRADE]
        }

    def close(self):
        if self.producer:
            logger.info("Đang đóng kết nối Redpanda...")
            self.producer.flush()
            self.producer.close()
            logger.info("Đã đóng kết nối Redpanda.")
