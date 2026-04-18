"""
Binance Data Streamer – main.py
================================
Luồng hoạt động:
  1. Khám phá top 100 symbols (USDT) theo 24h volume qua REST.
  2. Song song:
       a. Fetch lịch sử 24h klines cho tất cả 100 symbols (concurrent, rate-limited).
       b. Fetch order book snapshot cho tất cả 100 symbols.
       c. Fetch ticker 24h snapshot cho tất cả 100 symbols (1 request duy nhất).
       d. Mở WebSocket combined stream (tự động tách thành nhiều connection).
  3. Mỗi REST_REFRESH_INTERVAL giây: refresh lại order book + ticker qua REST.
  4. Chạy 24h rồi tự dừng (hoặc Ctrl+C).

Output (data/):
  <SYMBOL>_klines_1m.jsonl    – nến 1m
  <SYMBOL>_orderbook.jsonl    – order book top-20
  <SYMBOL>_ticker24h.jsonl    – ticker 24h
"""

import asyncio
import io
import logging
import signal
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import config
from rest_collector import RestCollector
from ws_collector    import WebSocketCollector
from redpanda_storage import RedpandaStorage

# ── Logging ───────────────────────────────────────────────────────────────────

Path(config.LOG_DIR).mkdir(exist_ok=True)
Path(config.DATA_DIR).mkdir(exist_ok=True)

_log_file = config.LOG_DIR / f"stream_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"

_stdout_handler = logging.StreamHandler(
    io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stdout, "buffer") else sys.stdout
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        _stdout_handler,
        logging.FileHandler(_log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    storage = RedpandaStorage()
    rest    = RestCollector(config.REST_BASE_URL, config.API_KEY)

    # ── stats ────────────────────────────────────────────────────────────────
    stats: dict[str, int] = defaultdict(int)

    # ── STEP 1: khám phá top 100 symbols ─────────────────────────────────────
    logger.info("Đang lấy top %d symbols từ Binance…", config.TOP_N_SYMBOLS)
    symbols = await rest.fetch_top_symbols(config.TOP_N_SYMBOLS, config.QUOTE_ASSET)
    if not symbols:
        logger.error("Không lấy được danh sách symbols. Kiểm tra kết nối / API key.")
        return

    # ── WS callbacks ─────────────────────────────────────────────────────────
    async def on_kline(r: dict):
        stats["ws_klines"] += 1
        storage.save_kline(r)
        if stats["ws_klines"] % 1000 == 0:
            logger.info(
                "[WS] klines=%d | %s close=%.4f closed=%s",
                stats["ws_klines"], r["symbol"], r["close"], r["is_closed"],
            )

    async def on_ticker(r: dict):
        stats["ws_ticker"] += 1
        storage.save_ticker(r)
        if stats["ws_ticker"] % 1000 == 0:
            logger.info(
                "[WS] ticker=%d | %s last=%.4f pct=%.2f%%",
                stats["ws_ticker"], r["symbol"], r["last_price"], r["price_change_pct"],
            )

    async def on_trade(r: dict):
        stats["ws_trade"] += 1
        storage.save_trade(r)
        if stats["ws_trade"] % 2000 == 0:
            logger.info(
                "[WS] trade=%d | %s price=%.4f qty=%.4f",
                stats["ws_trade"], r["symbol"], r["price"], r["qty"],
            )

    ws = WebSocketCollector(
        base_url  = config.WS_BASE_URL,
        on_kline  = on_kline,
        on_ticker = on_ticker,
        on_trade  = on_trade,
    )

    # ── banner ───────────────────────────────────────────────────────────────
    logger.info("=" * 70)
    logger.info("  Binance Data Streamer  –  %s", datetime.now(timezone.utc).isoformat())
    logger.info("  Symbols  : %d symbols (%s … %s)", len(symbols), symbols[0], symbols[-1])
    logger.info("  Interval : %s  |  Lookback: %d candles (24h)", config.KLINE_INTERVAL, config.KLINES_LOOKBACK_LIMIT)
    logger.info("  Duration : %.1fh  |  WS connections: %d",
                config.STREAM_DURATION_SECONDS / 3600,
                -(-len(symbols) * 3 // config.MAX_STREAMS_PER_CONNECTION))  # ceiling div
    logger.info("  Data dir : %s", config.DATA_DIR.resolve())
    logger.info("=" * 70)

    # ── TASK A: historical data via REST ─────────────────────────────────────
    async def fetch_historical():
        logger.info("[REST] Bắt đầu fetch historical data cho %d symbols…", len(symbols))

        # Ticker: 1 request cho tất cả (weight = 40, thay vì 100 × weight 2)
        tickers = await rest.fetch_all_tickers(symbols)
        for t in tickers:
            storage.save_ticker(t)
        stats["hist_ticker"] += len(tickers)

        # Klines: concurrent với semaphore
        klines = await rest.fetch_klines_batch(symbols, config.KLINE_INTERVAL, config.KLINES_LOOKBACK_LIMIT)
        for k in klines:
            storage.save_kline(k)
        stats["hist_klines"] += len(klines)

        stats["hist_klines"] += len(klines)

        logger.info(
            "[REST] Historical xong – klines=%d  tickers=%d",
            stats["hist_klines"], stats["hist_ticker"],
        )

    # ── TASK B: WebSocket real-time ───────────────────────────────────────────
    async def ws_stream():
        await ws.stream(symbols, config.KLINE_INTERVAL, config.WS_MAX_RECONNECTS)

    # ── TASK C: periodic REST refresh ────────────────────────────────────────
    async def periodic_refresh():
        while True:
            await asyncio.sleep(config.REST_REFRESH_INTERVAL)
            try:
                # Ticker: 1 request cho tất cả
                tickers = await rest.fetch_all_tickers(symbols)
                for t in tickers:
                    storage.save_ticker(t)

                s = storage.summary()
                total = sum(s.values())
                logger.info(
                    "[REST] Refresh OK – total records in Redpanda: %d | ws klines=%d ticker=%d trade=%d",
                    total, stats["ws_klines"], stats["ws_ticker"], stats["ws_trade"],
                )
            except Exception as e:
                logger.error("[REST] Refresh lỗi: %s", e)

    # ── graceful shutdown ─────────────────────────────────────────────────────
    loop = asyncio.get_running_loop()

    def _shutdown(sig_name: str):
        logger.info("Signal %s – shutting down…", sig_name)
        ws.stop()
        for t in asyncio.all_tasks(loop):
            t.cancel()

    try:
        loop.add_signal_handler(signal.SIGINT,  lambda: _shutdown("SIGINT"))
        loop.add_signal_handler(signal.SIGTERM, lambda: _shutdown("SIGTERM"))
    except NotImplementedError:
        pass   # Windows: dùng KeyboardInterrupt

    # ── run all tasks ─────────────────────────────────────────────────────────
    tasks = [
        asyncio.create_task(fetch_historical(), name="rest-historical"),
        asyncio.create_task(ws_stream(),        name="ws-stream"),
        asyncio.create_task(periodic_refresh(), name="rest-refresh"),
    ]

    try:
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=config.STREAM_DURATION_SECONDS,
        )
    except asyncio.TimeoutError:
        logger.info("Stream duration (%ds / 24h) completed", config.STREAM_DURATION_SECONDS)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Shutdown requested")
    finally:
        ws.stop()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await rest.close()
        storage.close()

    # ── final summary ─────────────────────────────────────────────────────────
    final = storage.summary()
    logger.info("=" * 70)
    logger.info("  FINAL SUMMARY")
    logger.info("  Historical : klines=%d  tickers=%d",
                stats["hist_klines"], stats["hist_ticker"])
    logger.info("  Real-time  : klines=%d  ticker=%d  trade=%d",
                stats["ws_klines"], stats["ws_ticker"], stats["ws_trade"])
    logger.info("  Topics written:")
    for tname, count in sorted(final.items()):
        logger.info("    %-45s %d records", tname, count)
    logger.info("  Location: %s", config.DATA_DIR.resolve())
    logger.info("=" * 70)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
