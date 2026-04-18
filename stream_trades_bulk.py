"""
Binance Bulk Download: daily trades for the past 7 days
for the 100 matched USDT symbols from active_symbols_matched.csv.

Primary  : Binance Bulk S3 (https://data.binance.vision)
Fallback : Binance REST API /api/v3/historicalTrades (requires API key)
           — used automatically for symbols missing from S3

Output: one CSV per symbol saved to DATA/
"""

import csv
import hashlib
import io
import logging
import os
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ── config ────────────────────────────────────────────────────────────────────
S3_BASE      = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
REST_BASE    = "https://api.binance.com"
API_KEY      = os.getenv("BINANCE_API_KEY")
DATA_DIR     = Path("DATA")
SYMBOLS_FILE = DATA_DIR / "active_symbols_matched.csv"
DAYS_BACK    = 7

TRADES_COLUMNS = [
    "trade_id", "price", "qty", "quote_qty",
    "time", "is_buyer_maker", "is_best_match",
]

DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(DATA_DIR / "stream_trades.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "binance-bulk-downloader/1.0"})
if API_KEY:
    SESSION.headers.update({"X-MBX-APIKEY": API_KEY})


# ── helpers: S3 bulk ──────────────────────────────────────────────────────────

def past_dates(n: int) -> list[str]:
    today = datetime.now(timezone.utc).date()
    return [(today - timedelta(days=i)).isoformat() for i in range(1, n + 1)]


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
    return hashlib.sha256(data).hexdigest() == raw.split()[0]


def unzip_csv(data: bytes) -> list[dict]:
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        with z.open(z.namelist()[0]) as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
            return [dict(zip(TRADES_COLUMNS, row)) for row in reader]


def fetch_bulk_symbol(symbol: str, dates: list[str]) -> list[dict]:
    """Download trades via S3 bulk for all dates. Returns empty list if all dates fail."""
    all_trades: list[dict] = []
    for date in sorted(dates):
        try:
            data = fetch_bytes(s3_url(symbol, date))
        except requests.HTTPError:
            log.warning("  [S3] %s %s — not found", symbol, date)
            continue
        if not verify_checksum(data, symbol, date):
            log.warning("  [S3] %s %s — checksum mismatch", symbol, date)
            continue
        records = unzip_csv(data)
        all_trades.extend(records)
        log.info("  [S3] %s %s: %d trades", symbol, date, len(records))
    return all_trades


# ── helpers: REST API fallback ────────────────────────────────────────────────

def fetch_rest_trades(symbol: str, start_ms: int, end_ms: int) -> list[dict]:
    """Fetch historical trades via REST API for a time window, handling pagination."""
    all_trades: list[dict] = []
    # /api/v3/historicalTrades returns up to 1000 trades per call, paginated by fromId
    # We use /api/v3/aggTrades with startTime/endTime which supports time-based pagination
    current_start = start_ms
    limit = 1000

    while current_start < end_ms:
        params = {
            "symbol": symbol,
            "startTime": current_start,
            "endTime": min(current_start + 60 * 60 * 1000, end_ms),  # 1h window per request
            "limit": limit,
        }
        resp = SESSION.get(f"{REST_BASE}/api/v3/aggTrades", params=params, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            current_start += 60 * 60 * 1000  # advance 1h if empty
            continue

        for t in batch:
            all_trades.append({
                "trade_id":      t["a"],         # agg trade id
                "price":         t["p"],
                "qty":           t["q"],
                "quote_qty":     str(float(t["p"]) * float(t["q"])),
                "time":          t["T"],
                "is_buyer_maker": t["m"],
                "is_best_match":  t.get("M", ""),
            })

        last_time = batch[-1]["T"]
        if len(batch) < limit:
            current_start = last_time + 1
        else:
            current_start = last_time + 1
        time.sleep(0.1)  # respect rate limit

    return all_trades


def fetch_rest_symbol(symbol: str, days: int) -> list[dict]:
    """Fetch trades for a symbol over the past `days` complete days via REST API."""
    if not API_KEY:
        log.error("  [REST] No API key — cannot use fallback for %s", symbol)
        return []

    today = datetime.now(timezone.utc).date()
    start_dt = datetime(today.year, today.month, today.day, tzinfo=timezone.utc) - timedelta(days=days)
    end_dt   = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)

    log.info("  [REST] Fetching %s via aggTrades %s -> %s", symbol, start_dt.date(), end_dt.date())
    trades = fetch_rest_trades(symbol, start_ms, end_ms)
    log.info("  [REST] %s: %d trades total", symbol, len(trades))
    return trades


# ── output ────────────────────────────────────────────────────────────────────

def save_csv(data: list[dict], path: Path):
    if not data:
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=TRADES_COLUMNS)
        writer.writeheader()
        writer.writerows(data)
    log.info("  Saved %d trades -> %s", len(data), path)


def load_symbols() -> list[str]:
    with open(SYMBOLS_FILE, encoding="utf-8") as f:
        return [row["symbol"] for row in csv.DictReader(f)]


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    symbols = load_symbols()
    dates   = past_dates(DAYS_BACK)

    log.info("=== Binance Trades Download (Bulk + REST fallback) ===")
    log.info("Symbols : %d  (from %s)", len(symbols), SYMBOLS_FILE)
    log.info("Dates   : %s -> %s  (%d days)", dates[-1], dates[0], len(dates))
    log.info("API key : %s", "present" if API_KEY else "NOT SET — fallback disabled")

    success, skipped = [], []

    for i, symbol in enumerate(symbols, 1):
        out_path = DATA_DIR / f"trades_{symbol}_7d.csv"

        # skip already-downloaded symbols
        if out_path.exists():
            log.info("[%d/%d] %s — already exists, skipping", i, len(symbols), symbol)
            success.append(symbol)
            continue

        log.info("[%d/%d] %s", i, len(symbols), symbol)

        # try S3 bulk first
        trades = fetch_bulk_symbol(symbol, dates)

        # fallback to REST API if S3 returned nothing
        if not trades:
            log.warning("  [S3] No data — trying REST API fallback...")
            trades = fetch_rest_symbol(symbol, DAYS_BACK)

        if trades:
            save_csv(trades, out_path)
            success.append(symbol)
        else:
            log.warning("  No trades fetched for %s — skipping", symbol)
            skipped.append(symbol)

    log.info("=== Done ===")
    log.info("Success : %d / %d symbols", len(success), len(symbols))
    if skipped:
        log.warning("Skipped : %s", skipped)

    out = DATA_DIR / "trades_symbols_matched.csv"
    with open(out, "w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows([["symbol"]] + [[s] for s in success])
    log.info("Trades matched symbols -> %s", out)


if __name__ == "__main__":
    main()
