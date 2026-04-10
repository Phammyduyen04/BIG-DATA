"""
Binance Futures Data Fetcher
==============================
Downloads Futures data from fapi.binance.com:
  - Mark Price Klines (1m)
  - Funding Rate
  - Liquidations (Force Orders)

fapi.binance.com has its own rate limit (independent from Spot API).
Safe to run in parallel with fetch_binance_data.py.

Output:
  data/futures/mark_price/SYMBOL/SYMBOL_mark_1m_YYYYMMDD.csv
  data/futures/funding_rate/SYMBOL/SYMBOL_funding_YYYYMMDD.csv
  data/futures/liquidations/SYMBOL/SYMBOL_liquidations_YYYYMMDD.csv
"""

import os
import sys
import json
import time
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ─────────────────────── CONFIG ───────────────────────────────
load_dotenv()
API_KEY  = os.getenv("BINANCE_API_KEY", "")
BASE_URL = "https://fapi.binance.com"

DAYS_BACK = 1
INTERVAL  = "1m"

# Mark Price + Funding Rate: weight = 1/request -> batch 200 safe
BATCH_SIZE_LIGHT = 200
# Liquidations: weight = 20/request -> batch 50 -> 1000 weight / 65s
BATCH_SIZE_HEAVY = 50
BATCH_SLEEP_SEC  = 65
MAX_RETRIES      = 5

FETCH_MARK_PRICE   = True
FETCH_FUNDING      = True
FETCH_LIQUIDATIONS = False  # Requires Futures-enabled API key (permission denied)

OUTPUT_DIR    = Path("data/futures")
PROGRESS_FILE = Path("futures_progress.json")
LOG_FILE      = Path("logs/futures.log")
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

    def reset(self):
        self.count = 0


rl_light  = RateLimiter(BATCH_SIZE_LIGHT, BATCH_SLEEP_SEC)
rl_heavy  = RateLimiter(BATCH_SIZE_HEAVY, BATCH_SLEEP_SEC)
active_rl = rl_light


# ─────────────────────── PROGRESS ─────────────────────────────
def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"mark_price": {}, "funding_rate": {}, "liquidations": {}}


def save_progress(p: dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(p, f, indent=2)


# ─────────────────────── API ──────────────────────────────────
def api_get(endpoint, params=None):
    url     = BASE_URL + endpoint
    headers = {"X-MBX-APIKEY": API_KEY} if API_KEY else {}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            active_rl.tick()
            r = requests.get(url, params=params, headers=headers, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", BATCH_SLEEP_SEC))
                log.warning(f"[429] Rate limit. Sleeping {wait}s... (attempt {attempt})")
                time.sleep(wait)
                continue
            if r.status_code == 418:
                log.error("[418] IP banned! Sleeping 120s...")
                time.sleep(120)
                continue
            log.error(f"[HTTP {r.status_code}] {endpoint}: {r.text[:150]}")
            return None
        except requests.exceptions.RequestException as e:
            log.warning(f"[Error] {e} | attempt {attempt}")
            time.sleep(5 * attempt)
    return None


# ─────────────────────── SYMBOLS ──────────────────────────────
def get_futures_symbols() -> list[str]:
    log.info("Fetching futures symbols...")
    data = api_get("/fapi/v1/exchangeInfo")
    if not data:
        raise RuntimeError("Failed to fetch exchangeInfo!")
    symbols = [s["symbol"] for s in data["symbols"] if s["status"] == "TRADING"]
    log.info(f"Found {len(symbols)} futures symbols.")
    return sorted(symbols)


def log_progress(label, i, total, start):
    elapsed   = (datetime.now() - start).total_seconds()
    speed     = i / elapsed if elapsed > 0 else 1
    remaining = (total - i) / speed
    log.info(
        f"[{label}] {i}/{total} ({i/total*100:.1f}%) | "
        f"ETA ~{int(remaining//60)}m{int(remaining%60)}s"
    )


# ─────────────────────── MARK PRICE KLINES ────────────────────
def fetch_mark_price_klines(symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    all_rows = []
    current  = start_ms
    limit    = 1000

    while current < end_ms:
        data = api_get("/fapi/v1/markPriceKlines", {
            "symbol":    symbol,
            "interval":  INTERVAL,
            "startTime": current,
            "endTime":   end_ms,
            "limit":     limit,
        })
        if not data:
            break
        all_rows.extend(data)
        if len(data) < limit:
            break
        current = data[-1][0] + 1

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows, columns=[
        "open_time", "open", "high", "low", "close",
        "ignore1", "close_time", "ignore2", "num_trades",
        "ignore3", "ignore4", "ignore5",
    ])
    df = df[["open_time", "open", "high", "low", "close", "close_time", "num_trades"]]
    df["open_time"]  = pd.to_datetime(df["open_time"],  unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col])
    return df


def save_mark_price(symbol, df, date_str):
    out_dir = OUTPUT_DIR / "mark_price" / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_dir / f"{symbol}_mark_{INTERVAL}_{date_str}.csv", index=False)


# ─────────────────────── FUNDING RATE ──────────────────────────
def fetch_funding_rate(symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    all_rows = []
    current  = start_ms
    limit    = 1000

    while current < end_ms:
        data = api_get("/fapi/v1/fundingRate", {
            "symbol":    symbol,
            "startTime": current,
            "endTime":   end_ms,
            "limit":     limit,
        })
        if not data:
            break
        all_rows.extend(data)
        if len(data) < limit:
            break
        current = data[-1]["fundingTime"] + 1

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
    df["fundingRate"] = pd.to_numeric(df["fundingRate"])
    if "markPrice" in df.columns:
        df["markPrice"] = pd.to_numeric(df["markPrice"])
    return df


def save_funding_rate(symbol, df, date_str):
    out_dir = OUTPUT_DIR / "funding_rate" / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_dir / f"{symbol}_funding_{date_str}.csv", index=False)


# ─────────────────────── LIQUIDATIONS ──────────────────────────
def fetch_liquidations(symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    """
    /fapi/v1/forceOrders: weight=20, limit max=100.
    Paginated in 1-hour time windows.
    """
    all_rows = []
    chunk_ms = 3_600_000  # 1 hour
    current  = start_ms

    while current < end_ms:
        chunk_end = min(current + chunk_ms, end_ms)
        data = api_get("/fapi/v1/forceOrders", {
            "symbol":    symbol,
            "startTime": current,
            "endTime":   chunk_end,
            "limit":     100,
        })
        if data:
            all_rows.extend(data)
        current = chunk_end + 1

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    for col in ["price", "origQty", "executedQty", "averagePrice"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col])
    return df


def save_liquidations(symbol, df, date_str):
    out_dir = OUTPUT_DIR / "liquidations" / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_dir / f"{symbol}_liquidations_{date_str}.csv", index=False)


# ─────────────────────── MAIN ──────────────────────────────────
def main():
    global active_rl

    now_utc   = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=DAYS_BACK)
    start_ms  = int(start_utc.timestamp() * 1000)
    end_ms    = int(now_utc.timestamp() * 1000)
    date_str  = start_utc.strftime("%Y%m%d")

    log.info("=" * 60)
    log.info("Binance Futures Data Fetcher")
    log.info(f"  Date range   : {start_utc.date()} -> {now_utc.date()}")
    log.info(f"  Mark Price   : {FETCH_MARK_PRICE}")
    log.info(f"  Funding Rate : {FETCH_FUNDING}")
    log.info(f"  Liquidations : {FETCH_LIQUIDATIONS}")
    log.info("=" * 60)

    active_rl = rl_light
    symbols   = get_futures_symbols()
    progress  = load_progress()

    # ── PHASE A: MARK PRICE KLINES ────────────────────────────
    if FETCH_MARK_PRICE:
        active_rl = rl_light
        active_rl.reset()
        done_set = set(progress.get("mark_price", {}).keys())
        pending  = [s for s in symbols if s not in done_set]
        log.info(f"\n[PHASE A] Mark Price Klines ({INTERVAL}) -- {len(symbols)} symbols")
        log.info(f"  Remaining: {len(pending)} / {len(symbols)}")
        done = errors = 0
        start = datetime.now()

        for i, symbol in enumerate(pending, 1):
            df = fetch_mark_price_klines(symbol, start_ms, end_ms)
            if not df.empty:
                save_mark_price(symbol, df, date_str)
                done += 1
            else:
                errors += 1
            progress.setdefault("mark_price", {})[symbol] = {"rows": len(df)}
            save_progress(progress)
            if i % 50 == 0 or i == len(pending):
                log_progress("MarkPrice", i, len(pending), start)

        log.info(f"[PHASE A] Done -- {done} OK / {errors} errors")

    # ── PHASE B: FUNDING RATE ─────────────────────────────────
    if FETCH_FUNDING:
        active_rl = rl_light
        active_rl.reset()
        done_set = set(progress.get("funding_rate", {}).keys())
        pending  = [s for s in symbols if s not in done_set]
        log.info(f"\n[PHASE B] Funding Rate -- {len(symbols)} symbols")
        log.info(f"  Remaining: {len(pending)} / {len(symbols)}")
        done = errors = 0
        start = datetime.now()

        for i, symbol in enumerate(pending, 1):
            df = fetch_funding_rate(symbol, start_ms, end_ms)
            if not df.empty:
                save_funding_rate(symbol, df, date_str)
                done += 1
            else:
                errors += 1
            progress.setdefault("funding_rate", {})[symbol] = {"rows": len(df)}
            save_progress(progress)
            if i % 50 == 0 or i == len(pending):
                log_progress("FundingRate", i, len(pending), start)

        log.info(f"[PHASE B] Done -- {done} OK / {errors} errors")

    # ── PHASE C: LIQUIDATIONS ─────────────────────────────────
    if FETCH_LIQUIDATIONS:
        active_rl = rl_heavy
        active_rl.reset()
        done_set = set(progress.get("liquidations", {}).keys())
        pending  = [s for s in symbols if s not in done_set]
        log.info(f"\n[PHASE C] Liquidations -- {len(symbols)} symbols")
        log.info(f"  Remaining: {len(pending)} / {len(symbols)}")
        log.info(f"  (weight=20/req -> batch {BATCH_SIZE_HEAVY} req / {BATCH_SLEEP_SEC}s)")
        done = errors = 0
        start = datetime.now()

        for i, symbol in enumerate(pending, 1):
            df = fetch_liquidations(symbol, start_ms, end_ms)
            if not df.empty:
                save_liquidations(symbol, df, date_str)
                done += 1
            else:
                errors += 1
            progress.setdefault("liquidations", {})[symbol] = {"rows": len(df)}
            save_progress(progress)
            if i % 50 == 0 or i == len(pending):
                log_progress("Liquidations", i, len(pending), start)

        log.info(f"[PHASE C] Done -- {done} OK / {errors} errors")

    log.info("\n" + "=" * 60)
    log.info("FUTURES COMPLETE!")
    log.info("  Output: data/futures/")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
