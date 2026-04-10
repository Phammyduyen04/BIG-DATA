"""
Binance Vision Trades Fetcher
==============================
Downloads Raw Trades from data.binance.vision for all spot symbols.
Uses parallel downloads (20 concurrent), no API key required.

Output:
  data/trades/SYMBOL/SYMBOL-trades-YYYY-MM-DD.csv
"""

import sys
import logging
import zipfile
import requests
import pandas as pd
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────── CONFIG ───────────────────────────────
BASE_VISION_URL = "https://data.binance.vision/data/spot/daily/trades"
BASE_API_URL    = "https://api.binance.com"
DAYS_BACK       = 1    # Days of history (counting from yesterday)
MAX_WORKERS     = 20   # Parallel downloads
OUTPUT_DIR      = Path("data")
LOG_FILE        = Path("logs/vision.log")
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


def get_symbols() -> list[str]:
    log.info("Fetching symbol list...")
    r = requests.get(f"{BASE_API_URL}/api/v3/exchangeInfo", timeout=15)
    r.raise_for_status()
    symbols = [s["symbol"] for s in r.json()["symbols"] if s["status"] == "TRADING"]
    log.info(f"Found {len(symbols)} symbols.")
    return sorted(symbols)


def download_trades(symbol: str, date_str: str) -> tuple[str, int, str]:
    """Download and save trades for 1 symbol on 1 date. Returns (symbol, rows, status)."""
    url      = f"{BASE_VISION_URL}/{symbol}/{symbol}-trades-{date_str}.zip"
    out_dir  = OUTPUT_DIR / "trades" / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{symbol}-trades-{date_str}.csv"

    if out_path.exists():
        return symbol, 0, "skip"

    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 404:
            return symbol, 0, "404"
        r.raise_for_status()

        with zipfile.ZipFile(BytesIO(r.content)) as z:
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f, header=None, names=[
                    "trade_id", "price", "qty", "quote_qty",
                    "time", "is_buyer_maker", "is_best_match",
                ])

        df.to_csv(out_path, index=False)
        return symbol, len(df), "ok"

    except Exception as e:
        return symbol, 0, f"error: {e}"


def main():
    now   = datetime.now(timezone.utc)
    dates = [(now - timedelta(days=i + 1)).strftime("%Y-%m-%d") for i in range(DAYS_BACK)]

    log.info("=" * 55)
    log.info("Binance Vision Trades Fetcher")
    log.info(f"  Dates   : {', '.join(dates)}")
    log.info(f"  Workers : {MAX_WORKERS}")
    log.info("=" * 55)

    symbols = get_symbols()

    for date_str in dates:
        log.info(f"\n[DATE] {date_str} -- {len(symbols)} symbols")
        tasks      = [(s, date_str) for s in symbols]
        done = ok = skip = err = 0
        total_rows = 0
        start      = datetime.now()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(download_trades, s, d): s for s, d in tasks}
            for fut in as_completed(futures):
                symbol, rows, status = fut.result()
                done += 1
                total_rows += rows
                if status == "ok":
                    ok += 1
                elif status == "skip":
                    skip += 1
                else:
                    err += 1

                if done % 100 == 0 or done == len(tasks):
                    elapsed   = (datetime.now() - start).total_seconds()
                    speed     = done / elapsed if elapsed > 0 else 1
                    remaining = (len(tasks) - done) / speed
                    log.info(
                        f"[Trades] {done}/{len(tasks)} ({done/len(tasks)*100:.1f}%) | "
                        f"OK={ok} Skip={skip} Err={err} | "
                        f"ETA ~{int(remaining//60)}m{int(remaining%60)}s"
                    )

        log.info(
            f"[DATE {date_str}] Done: {ok} OK / {skip} skip / {err} errors | "
            f"{total_rows:,} trades"
        )

    log.info("\n" + "=" * 55)
    log.info("TRADES COMPLETE!")
    log.info("  Output: data/trades/<SYMBOL>/")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
