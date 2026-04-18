"""
Binance REST API streaming: klines (1m, last 24h) + ticker_24h
for top 100 USDT symbols by quote volume.
"""

import csv
import io
import os
import time
import json
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
OUTPUT_DIR = Path("output")
JSON_DIR = OUTPUT_DIR / "json"
CSV_DIR = OUTPUT_DIR / "csv"
for d in (JSON_DIR, CSV_DIR):
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(OUTPUT_DIR / "stream.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

HEADERS = {"X-MBX-APIKEY": API_KEY} if API_KEY else {}


def get(endpoint: str, params: dict = None) -> dict | list:
    url = BASE_URL + endpoint
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def is_valid_ascii_symbol(symbol: str) -> bool:
    """Binance REST API requires pure ASCII symbol names."""
    try:
        symbol.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def fetch_top100_usdt_symbols() -> list[str]:
    """Return top 100 USDT symbols ranked by quoteVolume (24h)."""
    log.info("Fetching 24h ticker for all symbols to determine top 100 USDT pairs...")
    tickers = get("/api/v3/ticker/24hr")

    usdt_tickers = [
        t for t in tickers
        if t["symbol"].endswith("USDT") and is_valid_ascii_symbol(t["symbol"])
    ]
    usdt_tickers.sort(key=lambda t: float(t["quoteVolume"]), reverse=True)

    top100 = [t["symbol"] for t in usdt_tickers[:TOP_N]]
    log.info("Top 100 USDT symbols selected: %s ... (total %d)", top100[:5], len(top100))
    return top100


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


def save_json(data: list[dict] | list, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info("Saved %d records -> %s", len(data), path)


def save_csv(data: list[dict], path: Path):
    if not data:
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    log.info("Saved %d records -> %s", len(data), path)


def stream_once(symbols: list[str], start_ms: int, end_ms: int):
    """One streaming cycle: fetch klines + ticker_24h and save to output."""
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
        # Respect Binance rate limit: ~1200 weight/min; klines costs 2 weight each
        time.sleep(0.1)

    save_json(all_klines, JSON_DIR / f"klines_1m_{timestamp}.json")
    save_csv(all_klines, CSV_DIR / f"klines_1m_{timestamp}.csv")

    # --- ticker_24h — only for symbols that have klines data ---
    ticker_data = fetch_ticker_24h(active_symbols)
    ticker_map = {t["symbol"]: t for t in ticker_data}
    ticker_ordered = [ticker_map[s] for s in active_symbols if s in ticker_map]
    save_json(ticker_ordered, JSON_DIR / f"ticker_24h_{timestamp}.json")
    save_csv(ticker_ordered, CSV_DIR / f"ticker_24h_{timestamp}.csv")

    return ticker_ordered, all_klines, active_symbols


def main():
    # Time window: last 24 hours
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 24 * 60 * 60 * 1000  # 24h ago

    log.info("=== Binance Streaming Start ===")
    log.info("Window: %s -> %s (UTC)",
             datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat(),
             datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat())

    # Step 1: determine top 100 USDT symbols
    symbols = fetch_top100_usdt_symbols()

    # Step 2: save symbol list
    save_json(symbols, JSON_DIR / "top100_usdt_symbols.json")

    # Step 3: stream klines + ticker_24h for those exact symbols
    ticker_data, klines_data, active_symbols = stream_once(symbols, start_ms, now_ms)

    # Save final reconciled symbol list
    save_json(active_symbols, JSON_DIR / "active_symbols_matched.json")

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
