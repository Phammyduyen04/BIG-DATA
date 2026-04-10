"""
Binance Snapshot Fetcher
========================
Fetches Ticker 24h + Order Book snapshot for all symbols.
Runs in parallel with fetch_binance_data.py (no rate limit conflict
since they share the 1200 weight/min limit -- this script uses ~200 weight/min).

Output:
  data/ticker_24h/ticker_24h_YYYYMMDD_HHMMSS.csv   <- 1 file, all symbols
  data/order_book/SYMBOL/SYMBOL_depth_YYYYMMDD_HHMMSS.csv
"""

import os
import sys
import time
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ─────────────────────── CONFIG ───────────────────────────────
load_dotenv()
API_KEY  = os.getenv("BINANCE_API_KEY", "")
BASE_URL = "https://api.binance.com"

# Order Book: limit=100 -> weight=5/request -> max 240 req/min
# Use 150 req/batch to leave bandwidth for AggTrades
DEPTH_LIMIT     = 100
BATCH_SIZE      = 150
BATCH_SLEEP_SEC = 65
MAX_RETRIES     = 5

OUTPUT_DIR = Path("data")
LOG_FILE   = Path("logs/snapshot.log")
# ───────────────────────────────────────────────────────────────

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────── RATE LIMITER ─────────────────────────
class RateLimiter:
    def __init__(self, batch_size, sleep_sec):
        self.batch_size = batch_size
        self.sleep_sec  = sleep_sec
        self.count      = 0

    def tick(self):
        self.count += 1
        if self.count >= self.batch_size:
            log.info(f"[RateLimit] Sleeping {self.sleep_sec}s after {self.batch_size} requests...")
            time.sleep(self.sleep_sec)
            self.count = 0

rate_limiter = RateLimiter(BATCH_SIZE, BATCH_SLEEP_SEC)


def api_get(endpoint, params=None):
    url     = BASE_URL + endpoint
    headers = {"X-MBX-APIKEY": API_KEY} if API_KEY else {}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            rate_limiter.tick()
            r = requests.get(url, params=params, headers=headers, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", BATCH_SLEEP_SEC))
                log.warning(f"[429] Sleeping {wait}s... (attempt {attempt})")
                time.sleep(wait)
                continue
            if r.status_code == 418:
                log.error("[418] IP banned! Sleeping 120s...")
                time.sleep(120)
                continue
            log.error(f"[HTTP {r.status_code}] {endpoint}")
            return None
        except requests.exceptions.RequestException as e:
            log.warning(f"[Error] {e} | attempt {attempt}")
            time.sleep(5 * attempt)
    return None


# ─────────────────────── SYMBOLS ──────────────────────────────
def get_symbols() -> list[str]:
    log.info("Fetching symbol list...")
    data = api_get("/api/v3/exchangeInfo")
    if not data:
        raise RuntimeError("Failed to fetch exchangeInfo!")
    symbols = [
        s["symbol"] for s in data["symbols"]
        if s["status"] == "TRADING"
    ]
    log.info(f"Found {len(symbols)} symbols.")
    return sorted(symbols)


# ─────────────────────── TICKER 24H ───────────────────────────
def fetch_ticker_24h() -> pd.DataFrame:
    """1 request to fetch all tickers."""
    log.info("Fetching Ticker 24h (1 request for all symbols)...")
    data = api_get("/api/v3/ticker/24hr")
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    cols = [
        "symbol", "priceChange", "priceChangePercent",
        "weightedAvgPrice", "prevClosePrice",
        "lastPrice", "lastQty",
        "bidPrice", "bidQty", "askPrice", "askQty",
        "openPrice", "highPrice", "lowPrice",
        "volume", "quoteVolume",
        "openTime", "closeTime",
        "firstId", "lastId", "count",
    ]
    df = df[[c for c in cols if c in df.columns]]
    for col in ["priceChange", "priceChangePercent", "weightedAvgPrice",
                "prevClosePrice", "lastPrice", "lastQty",
                "bidPrice", "bidQty", "askPrice", "askQty",
                "openPrice", "highPrice", "lowPrice",
                "volume", "quoteVolume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col])
    df["openTime"]  = pd.to_datetime(df["openTime"],  unit="ms", utc=True)
    df["closeTime"] = pd.to_datetime(df["closeTime"], unit="ms", utc=True)
    return df


def save_ticker_24h(df: pd.DataFrame, timestamp: str):
    out_dir = OUTPUT_DIR / "ticker_24h"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"ticker_24h_{timestamp}.csv"
    df.to_csv(path, index=False)
    log.info(f"[Ticker 24h] {len(df)} symbols -> {path}")


# ─────────────────────── ORDER BOOK ───────────────────────────
def fetch_order_book(symbol: str) -> pd.DataFrame:
    data = api_get("/api/v3/depth", {"symbol": symbol, "limit": DEPTH_LIMIT})
    if not data:
        return pd.DataFrame()

    snapshot_time = datetime.utcnow().isoformat()
    rows = []
    for price, qty in data.get("bids", []):
        rows.append({"side": "bid", "price": float(price), "quantity": float(qty),
                     "snapshot_time": snapshot_time})
    for price, qty in data.get("asks", []):
        rows.append({"side": "ask", "price": float(price), "quantity": float(qty),
                     "snapshot_time": snapshot_time})
    return pd.DataFrame(rows)


def save_order_book(symbol: str, df: pd.DataFrame, timestamp: str):
    out_dir = OUTPUT_DIR / "order_book" / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{symbol}_depth_{timestamp}.csv"
    df.to_csv(path, index=False)


# ─────────────────────── MAIN ─────────────────────────────────
def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    log.info("=" * 55)
    log.info("Binance Snapshot Fetcher")
    log.info(f"  Timestamp  : {ts}")
    log.info(f"  Depth limit: {DEPTH_LIMIT} levels")
    log.info(f"  Batch size : {BATCH_SIZE} req / {BATCH_SLEEP_SEC}s")
    log.info("=" * 55)

    symbols = get_symbols()

    # ── PHASE A: TICKER 24H (1 request) ───────────────────────
    log.info("\n[PHASE A] Ticker 24h")
    df_ticker = fetch_ticker_24h()
    if not df_ticker.empty:
        save_ticker_24h(df_ticker, ts)
        log.info(f"[PHASE A] Done -- {len(df_ticker)} symbols, {len(df_ticker.columns)} fields")
    else:
        log.error("[PHASE A] Failed to fetch Ticker 24h!")

    # ── PHASE B: ORDER BOOK SNAPSHOT ──────────────────────────
    log.info(f"\n[PHASE B] Order Book snapshot -- {len(symbols)} symbols")

    done = 0
    errors = 0
    start = datetime.now()

    for i, symbol in enumerate(symbols, 1):
        df = fetch_order_book(symbol)
        if not df.empty:
            save_order_book(symbol, df, ts)
            done += 1
        else:
            errors += 1

        if i % 100 == 0:
            elapsed   = (datetime.now() - start).total_seconds()
            speed     = i / elapsed
            remaining = (len(symbols) - i) / speed
            pct       = i / len(symbols) * 100
            log.info(
                f"[OrderBook] {i}/{len(symbols)} ({pct:.1f}%) | "
                f"ETA ~{int(remaining//60)}m{int(remaining%60)}s"
            )

    log.info(f"\n[PHASE B] Done -- {done} OK / {errors} errors")

    log.info("\n" + "=" * 55)
    log.info("SNAPSHOT COMPLETE!")
    log.info(f"  Ticker 24h : data/ticker_24h/ticker_24h_{ts}.csv")
    log.info(f"  Order Book : data/order_book/<SYMBOL>/")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
