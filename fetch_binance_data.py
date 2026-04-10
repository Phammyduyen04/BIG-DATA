"""
Binance Historical Data Fetcher
================================
Downloads 1-day historical data from Binance REST API.

Data types:
  - Klines (OHLCV) - 1m interval
  - Aggregate Trades

Rate limiting strategy:
  - Max 300 requests/min (safe under 1200 weight/min limit)
  - Sleep 65s after every 300-request batch
  - Auto-retry on 429 / 418

Output structure:
  data/
    klines/SYMBOL/SYMBOL_1m_YYYYMMDD_YYYYMMDD.csv
    agg_trades/SYMBOL/SYMBOL_agg_trades_YYYYMMDD_YYYYMMDD.csv
  progress.json   <- checkpoint to resume if interrupted
"""

import os
import time
import json
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from tqdm import tqdm

# ─────────────────────────── CONFIG ───────────────────────────
load_dotenv()
API_KEY    = os.getenv("BINANCE_API_KEY", "")
SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")

BASE_URL   = "https://api.binance.com"
DAYS_BACK  = 1           # Days of history to fetch
INTERVAL   = "1m"        # Kline interval

# Rate limiting
BATCH_SIZE      = 300    # Requests per batch
BATCH_SLEEP_SEC = 65     # Sleep after each batch (> 60s to be safe)
RETRY_SLEEP_SEC = 70     # Sleep on 429
MAX_RETRIES     = 5      # Max retries

# Data types to fetch (True/False)
FETCH_KLINES     = True
FETCH_AGG_TRADES = True

# Only fetch USDT pairs (set False to fetch ALL ~2000 pairs)
USDT_ONLY = False

OUTPUT_DIR    = Path("data")
PROGRESS_FILE = Path("progress.json")
LOG_FILE      = Path("logs/download.log")
# ───────────────────────────────────────────────────────────────


# ─────────────────────── LOGGING SETUP ─────────────────────────
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)
# ───────────────────────────────────────────────────────────────


class RateLimiter:
    """Counts requests per batch, auto-sleeps when threshold reached."""

    def __init__(self, batch_size: int, sleep_sec: int):
        self.batch_size = batch_size
        self.sleep_sec  = sleep_sec
        self.count      = 0
        self.total      = 0

    def tick(self):
        self.count += 1
        self.total += 1
        if self.count >= self.batch_size:
            log.info(
                f"[RateLimit] Sent {self.batch_size} requests "
                f"(total {self.total}). Sleeping {self.sleep_sec}s..."
            )
            time.sleep(self.sleep_sec)
            self.count = 0


rate_limiter = RateLimiter(BATCH_SIZE, BATCH_SLEEP_SEC)


def api_get(endpoint: str, params: dict | None = None) -> dict | list | None:
    """GET request to Binance REST API with retry logic."""
    url = BASE_URL + endpoint
    headers = {"X-MBX-APIKEY": API_KEY} if API_KEY else {}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            rate_limiter.tick()
            resp = requests.get(url, params=params, headers=headers, timeout=15)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", RETRY_SLEEP_SEC))
                log.warning(f"[429] Rate limit hit. Sleeping {retry_after}s... (attempt {attempt})")
                time.sleep(retry_after)
                continue

            if resp.status_code == 418:
                log.error(f"[418] IP banned! Sleeping 120s... (attempt {attempt})")
                time.sleep(120)
                continue

            log.error(f"[HTTP {resp.status_code}] {endpoint} | {resp.text[:200]}")
            return None

        except requests.exceptions.RequestException as e:
            log.warning(f"[RequestError] {e} | attempt {attempt}/{MAX_RETRIES}")
            time.sleep(5 * attempt)

    log.error(f"[FAILED] Skipping after {MAX_RETRIES} retries: {endpoint}")
    return None


# ─────────────────────── PROGRESS ──────────────────────────────
def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"klines": {}, "agg_trades": {}}


def save_progress(progress: dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)
# ───────────────────────────────────────────────────────────────


# ─────────────────────── SYMBOLS ───────────────────────────────
def get_symbols() -> list[str]:
    """Fetch all TRADING symbols."""
    log.info("Fetching symbol list from Binance...")
    data = api_get("/api/v3/exchangeInfo")
    if not data:
        raise RuntimeError("Failed to fetch exchangeInfo!")

    symbols = [
        s["symbol"]
        for s in data["symbols"]
        if s["status"] == "TRADING"
        and (not USDT_ONLY or s["symbol"].endswith("USDT"))
    ]
    log.info(f"Found {len(symbols)} symbols.")
    return sorted(symbols)
# ───────────────────────────────────────────────────────────────


# ─────────────────────── KLINES ────────────────────────────────
def fetch_klines_symbol(symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    """Download all 1m klines for 1 symbol in [start_ms, end_ms]."""
    all_rows = []
    current_start = start_ms
    limit = 1000  # max per request

    while current_start < end_ms:
        params = {
            "symbol":    symbol,
            "interval":  INTERVAL,
            "startTime": current_start,
            "endTime":   end_ms,
            "limit":     limit,
        }
        data = api_get("/api/v3/klines", params)
        if not data:
            break

        all_rows.extend(data)

        if len(data) < limit:
            break  # All fetched

        current_start = data[-1][0] + 1

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "num_trades",
        "taker_buy_base_vol", "taker_buy_quote_vol", "ignore",
    ])
    df.drop(columns=["ignore"], inplace=True)
    df["open_time"]  = pd.to_datetime(df["open_time"],  unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close", "volume",
                "quote_volume", "taker_buy_base_vol", "taker_buy_quote_vol"]:
        df[col] = pd.to_numeric(df[col])
    return df


def save_klines(symbol: str, df: pd.DataFrame, start_date: str, end_date: str):
    out_dir = OUTPUT_DIR / "klines" / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{symbol}_{INTERVAL}_{start_date}_{end_date}.csv"
    df.to_csv(path, index=False)
    log.info(f"[Klines] {symbol}: {len(df)} candles -> {path}")
# ───────────────────────────────────────────────────────────────


# ─────────────────────── AGG TRADES ────────────────────────────
def fetch_agg_trades_symbol(symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    """Download all aggTrades for 1 symbol, paginated in 1-hour chunks."""
    all_rows = []
    chunk_ms  = 3_600_000  # 1 hour = 3,600,000 ms
    current_start = start_ms

    while current_start < end_ms:
        chunk_end = min(current_start + chunk_ms, end_ms)
        params = {
            "symbol":    symbol,
            "startTime": current_start,
            "endTime":   chunk_end,
            "limit":     1000,
        }
        data = api_get("/api/v3/aggTrades", params)
        if data:
            all_rows.extend(data)

        current_start = chunk_end + 1

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df.rename(columns={
        "a": "agg_trade_id",
        "p": "price",
        "q": "quantity",
        "f": "first_trade_id",
        "l": "last_trade_id",
        "T": "timestamp",
        "m": "is_buyer_maker",
        "M": "is_best_match",
    }, inplace=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["price"]     = pd.to_numeric(df["price"])
    df["quantity"]  = pd.to_numeric(df["quantity"])
    return df


def save_agg_trades(symbol: str, df: pd.DataFrame, start_date: str, end_date: str):
    out_dir = OUTPUT_DIR / "agg_trades" / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{symbol}_agg_trades_{start_date}_{end_date}.csv"
    df.to_csv(path, index=False)
    log.info(f"[AggTrades] {symbol}: {len(df)} trades -> {path}")
# ───────────────────────────────────────────────────────────────


# ─────────────────────── MAIN ──────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("Binance Historical Data Fetcher")
    log.info(f"  Days back : {DAYS_BACK}")
    log.info(f"  Interval  : {INTERVAL}")
    log.info(f"  Klines    : {FETCH_KLINES}")
    log.info(f"  AggTrades : {FETCH_AGG_TRADES}")
    log.info(f"  USDT only : {USDT_ONLY}")
    log.info(f"  Batch size: {BATCH_SIZE} req / {BATCH_SLEEP_SEC}s")
    log.info("=" * 60)

    now_utc    = datetime.now(timezone.utc)
    start_utc  = now_utc - timedelta(days=DAYS_BACK)
    start_ms   = int(start_utc.timestamp() * 1000)
    end_ms     = int(now_utc.timestamp() * 1000)
    start_date = start_utc.strftime("%Y%m%d")
    end_date   = now_utc.strftime("%Y%m%d")

    log.info(f"Time range: {start_utc.date()} -> {now_utc.date()}")

    symbols  = get_symbols()
    progress = load_progress()

    if "meta" not in progress:
        progress["meta"] = {
            "started_at":    datetime.now().isoformat(),
            "total_symbols": len(symbols),
            "days_back":     DAYS_BACK,
            "fetch_klines":  FETCH_KLINES,
            "fetch_trades":  FETCH_AGG_TRADES,
        }
        save_progress(progress)
    else:
        progress["meta"]["total_symbols"] = len(symbols)
        save_progress(progress)

    # ── PHASE 1: KLINES ──────────────────────────────────────
    if FETCH_KLINES:
        log.info(f"\n{'─'*50}")
        log.info(f"PHASE 1: Klines -- {len(symbols)} symbols")
        log.info(f"{'─'*50}")

        klines_done = progress.get("klines", {})
        pending = [s for s in symbols if s not in klines_done]
        log.info(f"Remaining: {len(pending)} / {len(symbols)} symbols")

        for symbol in tqdm(pending, desc="Klines", unit="symbol"):
            try:
                df = fetch_klines_symbol(symbol, start_ms, end_ms)
                if not df.empty:
                    save_klines(symbol, df, start_date, end_date)
                else:
                    log.warning(f"[Klines] {symbol}: no data")

                klines_done[symbol] = {
                    "rows": len(df),
                    "done_at": datetime.now().isoformat(),
                }
                progress["klines"] = klines_done
                save_progress(progress)

            except Exception as e:
                log.error(f"[Klines] {symbol} ERROR: {e}")

        progress["meta"]["phase"] = "agg_trades"
        save_progress(progress)
        log.info("PHASE 1 complete.")

    # ── PHASE 2: AGG TRADES ───────────────────────────────────
    if FETCH_AGG_TRADES:
        log.info(f"\n{'─'*50}")
        log.info(f"PHASE 2: Aggregate Trades -- {len(symbols)} symbols")
        log.info(f"{'─'*50}")

        trades_done = progress.get("agg_trades", {})
        pending = [s for s in symbols if s not in trades_done]
        log.info(f"Remaining: {len(pending)} / {len(symbols)} symbols")

        for symbol in tqdm(pending, desc="AggTrades", unit="symbol"):
            try:
                df = fetch_agg_trades_symbol(symbol, start_ms, end_ms)
                if not df.empty:
                    save_agg_trades(symbol, df, start_date, end_date)
                else:
                    log.warning(f"[AggTrades] {symbol}: no data")

                trades_done[symbol] = {
                    "rows": len(df),
                    "done_at": datetime.now().isoformat(),
                }
                progress["agg_trades"] = trades_done
                save_progress(progress)

            except Exception as e:
                log.error(f"[AggTrades] {symbol} ERROR: {e}")

        log.info("PHASE 2 complete.")

    log.info("\nALL DONE!")
    log.info(f"Data saved to: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
