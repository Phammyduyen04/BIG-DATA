"""
Storage – JSON Lines, phân loại 3 thư mục con
-----------------------------------------------
  data/klines/    → <SYMBOL>_klines_1m.jsonl
  data/orderbook/ → <SYMBOL>_orderbook.jsonl
  data/ticker24h/ → <SYMBOL>_ticker24h.jsonl

Đọc lại bằng pandas:
  pd.read_json("data/klines/BTCUSDT_klines_1m.jsonl", lines=True)
"""

import json
import logging
from pathlib import Path
from threading import Lock

logger = logging.getLogger(__name__)


class DataStorage:
    def __init__(self, data_dir: str | Path):
        self._root = Path(data_dir)
        # Tạo 3 thư mục con
        self._dirs = {
            "klines":    self._root / "klines",
            "orderbook": self._root / "orderbook",
            "ticker24h": self._root / "ticker24h",
        }
        for d in self._dirs.values():
            d.mkdir(parents=True, exist_ok=True)

        self._locks:  dict[str, Lock] = {}
        self._counts: dict[str, int]  = {}

    # ── internal ──────────────────────────────────────────────────────────────

    def _get_lock(self, key: str) -> Lock:
        if key not in self._locks:
            self._locks[key]  = Lock()
            self._counts[key] = 0
        return self._locks[key]

    def _write(self, subdir: str, filename: str, record: dict):
        key      = f"{subdir}/{filename}"
        filepath = self._dirs[subdir] / filename
        lock     = self._get_lock(key)
        with lock:
            with open(filepath, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
            self._counts[key] += 1

    # ── public ────────────────────────────────────────────────────────────────

    def save_kline(self, record: dict):
        symbol = record.get("symbol", "UNKNOWN")
        self._write("klines", f"{symbol}_klines_1m.jsonl", record)

    def save_depth(self, record: dict):
        symbol = record.get("symbol", "UNKNOWN")
        self._write("orderbook", f"{symbol}_orderbook.jsonl", record)

    def save_ticker(self, record: dict):
        symbol = record.get("symbol", "UNKNOWN")
        self._write("ticker24h", f"{symbol}_ticker24h.jsonl", record)

    def summary(self) -> dict[str, int]:
        return dict(self._counts)

    def close(self):
        total = sum(self._counts.values())
        logger.info("Storage closed – %d files, %d records total", len(self._counts), total)
