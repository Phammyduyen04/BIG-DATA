import asyncio
import logging
import signal
import sys
import os
from collections import defaultdict
from datetime import datetime, timezone

import config
from redpanda_storage import RedpandaStorage
from rest_collector import RestCollector
from ws_collector import WebSocketCollector
import s3_utils

# Cấu hình logging chuyên nghiệp
log_dir = config.DATA_DIR / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
_log_file = log_dir / f"stream_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")

# Global storage for access by REST backfiller
shared_storage: RedpandaStorage | None = None

# ── Lifecycle ─────────────────────────────────────────────────────────────────

async def shutdown(loop, signal=None):
    if signal:
        logger.info(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    global shared_storage
    shared_storage = RedpandaStorage()
    rest = RestCollector(config.REST_BASE_URL, config.API_KEY)
    
    # ── stats ────────────────────────────────────────────────────────────────
    stats: dict[str, int] = defaultdict(int)

    # ── STEP 1: Unified Symbol Universe Coordination (Anti-Drift) ────────────
    logger.info(f"--- [v1.4.0] Symbol Universe Coordination ---")
    
    symbols = s3_utils.load_active_symbols()
    if symbols:
        logger.info(f"[S3] Found active whitelist ({len(symbols)} symbols). Locking universe.")
    else:
        logger.info(f"No active whitelist found or stale. Discovering Top {config.TOP_N_SYMBOLS} USDT pairs...")
        symbols = await rest.fetch_top_symbols(config.TOP_N_SYMBOLS, config.QUOTE_ASSET)
        if not symbols:
            logger.error("Failed to discover symbols. Aborting.")
            return
        s3_utils.save_active_symbols(symbols)

    # ── STEP 2: Bootstrap Phase (REST Backfill) ──────────────────────────────
    logger.info(f"--- [v1.4.0] BOOTSTRAP PHASE: 3m Klines / 7d Trades ---")
    
    # Ticker: 1 request cho tất cả (Weight 40)
    tickers = await rest.fetch_all_tickers(symbols)
    for t in tickers:
        shared_storage.save_ticker(t)
    stats["hist_ticker"] += len(tickers)

    # Klines Backfill (3 months)
    k_count = await rest.backfill_klines_batch(symbols, config.KLINE_INTERVAL, config.KLINES_LOOKBACK_LIMIT, shared_storage)
    stats["hist_klines"] += k_count

    # Trades Backfill (7 days aggTrades)
    t_count = await rest.backfill_trades_batch(symbols, config.TRADES_LOOKBACK_DAYS, shared_storage)
    stats["hist_trades"] += t_count

    logger.info(f"[BOOTSTRAP] FINISHED: Klines={stats['hist_klines']} | Trades={stats['hist_trades']}")

    # ── WS callbacks ─────────────────────────────────────────────────────────
    async def on_kline(r: dict):
        stats["ws_klines"] += 1
        shared_storage.save_kline(r)
        if stats["ws_klines"] % 1000 == 0:
            logger.info(f"[WS] klines={stats['ws_klines']} | {r['symbol']} close={r['close']}")

    async def on_ticker(r: dict):
        stats["ws_ticker"] += 1
        shared_storage.save_ticker(r)

    async def on_trade(r: dict):
        stats["ws_trade"] += 1
        shared_storage.save_trade(r)
        if stats["ws_trade"] % 2000 == 0:
            logger.info(f"[WS] trade={stats['ws_trade']} | {r['symbol']} price={r['price']}")

    ws = WebSocketCollector(
        base_url  = config.WS_BASE_URL,
        on_kline  = on_kline,
        on_ticker = on_ticker,
        on_trade  = on_trade,
    )

    # ── Banner ───────────────────────────────────────────────────────────────
    logger.info("=" * 70)
    logger.info(f"  BINANCE FULL-MODE PIPELINE - {config.VERSION}")
    logger.info(f"  Coordination: active_symbols.json (MinIO)")
    logger.info(f"  Symbols     : {len(symbols)} coins")
    logger.info(f"  Backfill    : 3 months Klines, 7 days Trades")
    logger.info(f"  Duration    : {config.STREAM_DURATION_SECONDS / 3600:.1f}h")
    logger.info("=" * 70)

    # ── Task A: WebSocket real-time ───────────────────────────────────────────
    async def ws_stream():
        logger.info("[WS] Starting live stream for Unified Universe...")
        await ws.stream(symbols, config.KLINE_INTERVAL, config.WS_MAX_RECONNECTS)

    # ── Task B: Periodic REST refresh ────────────────────────────────────────
    async def periodic_refresh():
        while True:
            await asyncio.sleep(config.REST_REFRESH_INTERVAL)
            try:
                tickers = await rest.fetch_all_tickers(symbols)
                for t in tickers:
                    shared_storage.save_ticker(t)
            except Exception as e:
                logger.error(f"[REST] Periodic refresh error: {e}")

    # ── Execution ────────────────────────────────────────────────────────────
    try:
        await asyncio.gather(
            ws_stream(),
            periodic_refresh(),
        )
    except asyncio.CancelledError:
        logger.info("Service tasks cancelled.")
    finally:
        logger.info("Final stats: %s", dict(stats))
        await rest.close()
        shared_storage.close()

def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Service manually stopped.")
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    main()
