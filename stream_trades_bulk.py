"""
Binance Bulk Download: daily trades for the matched 99 active USDT symbols.
Source: https://data.binance.vision (S3)
Date: yesterday UTC (most recent complete daily file available)
"""

import csv
import hashlib
import io
import json
import logging
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ── config ────────────────────────────────────────────────────────────────────
S3_BASE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
DATE = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()  # yesterday UTC

SYMBOLS_FILE = Path("output/json/active_symbols_matched.json")
OUTPUT_DIR   = Path("output")
JSON_DIR     = OUTPUT_DIR / "json"
CSV_DIR      = OUTPUT_DIR / "csv"

TRADES_COLUMNS = [
    "trade_id", "price", "qty", "quote_qty",
    "time", "is_buyer_maker", "is_best_match",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(OUTPUT_DIR / "stream.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "binance-bulk-downloader/1.0"})


# ── helpers ───────────────────────────────────────────────────────────────────

def s3_url(symbol: str, date: str) -> str:
    return f"{S3_BASE}/data/spot/daily/trades/{symbol}/{symbol}-trades-{date}.zip"


def checksum_url(symbol: str, date: str) -> str:
    return s3_url(symbol, date) + ".CHECKSUM"


def fetch_bytes(url: str) -> bytes:
    resp = SESSION.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content


def verify_checksum(data: bytes, symbol: str, date: str) -> bool:
    raw = fetch_bytes(checksum_url(symbol, date)).decode().strip()
    expected_hash = raw.split()[0]
    actual_hash = hashlib.sha256(data).hexdigest()
    return expected_hash == actual_hash


def unzip_csv(data: bytes) -> list[dict]:
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        fname = z.namelist()[0]
        with z.open(fname) as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
            return [dict(zip(TRADES_COLUMNS, row)) for row in reader]


def save_json(data: list[dict], path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info("  JSON saved: %d records -> %s", len(data), path)


def save_csv(data: list[dict], path: Path):
    if not data:
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=TRADES_COLUMNS)
        writer.writeheader()
        writer.writerows(data)
    log.info("  CSV  saved: %d records -> %s", len(data), path)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    symbols: list[str] = json.loads(SYMBOLS_FILE.read_text(encoding="utf-8"))
    log.info("=== Binance Bulk Trades Download ===")
    log.info("Date    : %s", DATE)
    log.info("Symbols : %d (from %s)", len(symbols), SYMBOLS_FILE)

    success, skipped = [], []

    for i, symbol in enumerate(symbols, 1):
        log.info("[%d/%d] %s ...", i, len(symbols), symbol)
        url = s3_url(symbol, DATE)
        try:
            data = fetch_bytes(url)
        except requests.HTTPError as e:
            log.warning("  SKIP — file not found on S3 (%s)", e)
            skipped.append(symbol)
            continue

        if not verify_checksum(data, symbol, DATE):
            log.warning("  SKIP — checksum mismatch for %s", symbol)
            skipped.append(symbol)
            continue

        records = unzip_csv(data)
        log.info("  -> %d trades", len(records))

        stem = f"trades_{symbol}_{DATE}"
        save_json(records, JSON_DIR / f"{stem}.json")
        save_csv(records,  CSV_DIR  / f"{stem}.csv")
        success.append(symbol)

    log.info("=== Done ===")
    log.info("Success : %d / %d symbols", len(success), len(symbols))
    if skipped:
        log.warning("Skipped : %s", skipped)

    # Save matched symbol list for trades (intersection with klines+ticker)
    matched_path = JSON_DIR / "trades_symbols_matched.json"
    with open(matched_path, "w", encoding="utf-8") as f:
        json.dump(success, f, ensure_ascii=False, indent=2)
    log.info("Trades matched symbols saved -> %s", matched_path)


if __name__ == "__main__":
    main()
