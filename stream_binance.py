"""
Binance REST API streaming: klines (1m, last 24h) + ticker_24h
for top 100 USDT symbols by quote volume over the past 3 months.
Output saved as CSV to DATA/ directory.
"""

import csv
import os
import time
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("BINANCE_API_KEY")
BASE_URL = "https://api.binance.com"
INTERVAL = "1m"
TOP_N = 100
DATA_DIR = Path("DATA")
DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(DATA_DIR / "stream.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

HEADERS = {"X-MBX-APIKEY": API_KEY} if API_KEY else {}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def get(endpoint: str, params: dict = None) -> dict | list:
    url = BASE_URL + endpoint
    resp = SESSION.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def is_valid_ascii_symbol(symbol: str) -> bool:
    try:
        symbol.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def fetch_top100_usdt_symbols() -> list[str]:
    """Return top 100 USDT symbols ranked by total quoteVolume over the past 3 months.

    Uses monthly klines (interval=1M) for all USDT pairs, sums the last 3 candles'
    quoteAssetVolume to approximate 3-month trading activity.
    """
    log.info("Fetching exchange info to get all USDT spot symbols...")
    info = get("/api/v3/exchangeInfo")
    all_usdt = [
        s["symbol"] for s in info["symbols"]
        if s["quoteAsset"] == "USDT"
        and s["status"] == "TRADING"
        and is_valid_ascii_symbol(s["symbol"])
    ]
    log.info("Found %d active USDT trading pairs", len(all_usdt))

    # 3-month window: endTime = now, startTime = ~91 days ago, interval = 1M (3 candles)
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 91 * 24 * 60 * 60 * 1000

    log.info("Fetching 3-month monthly klines for each symbol to rank by volume...")
    vol_map: dict[str, float] = {}
    for i, symbol in enumerate(all_usdt, 1):
        try:
            candles = get("/api/v3/klines", params={
                "symbol": symbol,
                "interval": "1M",
                "startTime": start_ms,
                "endTime": now_ms,
                "limit": 3,
            })
            # index 7 = quoteAssetVolume
            total_vol = sum(float(c[7]) for c in candles)
            if total_vol > 0:
                vol_map[symbol] = total_vol
        except Exception as e:
            log.debug("Skip %s: %s", symbol, e)
        # stay well within weight limit (~1200/min); klines weight=2
        if i % 50 == 0:
            log.info("  Progress: %d / %d symbols processed", i, len(all_usdt))
        time.sleep(0.05)

    ranked = sorted(vol_map, key=vol_map.get, reverse=True)[:TOP_N]
    log.info("Top 100 USDT symbols by 3-month volume: %s ... (total %d)", ranked[:5], len(ranked))
    return ranked


def fetch_ticker_24h(symbols: list[str]) -> list[dict]:
    """Fetch 24h ticker stats for given symbols using compact batch format."""
    log.info("Fetching ticker_24h for %d symbols...", len(symbols))
    # Use compact JSON array without spaces — Binance requires no extra whitespace
    symbols_param = "[" + ",".join(f'"{s}"' for s in symbols) + "]"
    params = {"symbols": symbols_param}
    data = get("/api/v3/ticker/24hr", params=params)
    log.info("ticker_24h received: %d records", len(data))
    return data


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[list]:
    """Fetch all klines for a symbol within [start_ms, end_ms], handling pagination."""
    all_klines = []
    current_start = start_ms
    limit = 1000  # Binance max per request

    while current_start < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_start,
            "endTime": end_ms,
            "limit": limit,
        }
        batch = get("/api/v3/klines", params=params)
        if not batch:
            break
        all_klines.extend(batch)
        last_open_time = batch[-1][0]
        if len(batch) < limit:
            break
        # Move start past the last candle to avoid duplicates
        current_start = last_open_time + 60_000  # 1m in ms

    return all_klines


def klines_to_records(symbol: str, raw: list[list]) -> list[dict]:
    """Convert raw kline lists to named dicts."""
    keys = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_buy_base_vol", "taker_buy_quote_vol", "ignore",
    ]
    records = []
    for row in raw:
        rec = dict(zip(keys, row))
        rec["symbol"] = symbol
        rec["open_time_dt"] = datetime.fromtimestamp(rec["open_time"] / 1000, tz=timezone.utc).isoformat()
        records.append(rec)
    return records


def save_csv(data: list[dict], path: Path):
    if not data:
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    log.info("Saved %d records -> %s", len(data), path)


def stream_once(symbols: list[str], start_ms: int, end_ms: int):
    """One streaming cycle: fetch klines + ticker_24h and save CSV to DATA/."""
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # --- klines (fetch first to determine active symbols) ---
    all_klines: list[dict] = []
    active_symbols: list[str] = []

    for i, symbol in enumerate(symbols, 1):
        log.info("[%d/%d] Fetching klines for %s ...", i, len(symbols), symbol)
        raw = fetch_klines(symbol, INTERVAL, start_ms, end_ms)
        records = klines_to_records(symbol, raw)
        if records:
            all_klines.extend(records)
            active_symbols.append(symbol)
            log.info("  -> %d candles", len(records))
        else:
            log.warning("  -> 0 candles — excluding %s from both outputs", symbol)
        # klines weight=2; stay within ~1200 weight/min
        time.sleep(0.1)

    save_csv(all_klines, DATA_DIR / f"klines_1m_{timestamp}.csv")

    # --- ticker_24h — only for symbols that matched klines ---
    ticker_data = fetch_ticker_24h(active_symbols)
    ticker_map = {t["symbol"]: t for t in ticker_data}
    # Preserve the same symbol order as klines; drop any ticker-only symbols
    ticker_ordered = [ticker_map[s] for s in active_symbols if s in ticker_map]
    save_csv(ticker_ordered, DATA_DIR / f"ticker_24h_{timestamp}.csv")

    return ticker_ordered, all_klines, active_symbols


def main():
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 91 * 24 * 60 * 60 * 1000  # last 3 months of 1m candles

    log.info("=== Binance Streaming Start ===")
    log.info("Window: %s -> %s (UTC)",
             datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat(),
             datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat())

    # Step 1: top 100 USDT symbols by 3-month quote volume
    symbols = fetch_top100_usdt_symbols()

    # Step 2: save symbol list as CSV
    with open(DATA_DIR / "top100_usdt_symbols.csv", "w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows([["symbol"]] + [[s] for s in symbols])
    log.info("Saved symbol list -> %s", DATA_DIR / "top100_usdt_symbols.csv")

    # Step 3: stream klines (1m) + ticker_24h — symbols matched between both
    ticker_data, klines_data, active_symbols = stream_once(symbols, start_ms, now_ms)

    # Save matched symbol list
    with open(DATA_DIR / "active_symbols_matched.csv", "w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows([["symbol"]] + [[s] for s in active_symbols])

    klines_syms = {r["symbol"] for r in klines_data}
    ticker_syms = {t["symbol"] for t in ticker_data}

    log.info("=== Done ===")
    log.info("Active symbols     : %d  (excluded %d with no klines)",
             len(active_symbols), len(symbols) - len(active_symbols))
    log.info("ticker_24h records : %d", len(ticker_data))
    log.info("klines records     : %d", len(klines_data))
    log.info("Symbols matched    : klines=%d  ticker=%d  intersection=%d",
             len(klines_syms), len(ticker_syms), len(klines_syms & ticker_syms))
    if klines_syms != ticker_syms:
        log.warning("Mismatch! klines-only=%s  ticker-only=%s",
                    klines_syms - ticker_syms, ticker_syms - klines_syms)


if __name__ == "__main__":
    main()
