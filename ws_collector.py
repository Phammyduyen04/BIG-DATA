"""
WebSocket Collector
-------------------
Tự động tách thành nhiều connection (mỗi connection tối đa MAX_STREAMS_PER_CONNECTION).

Streams per symbol:
  - @kline_<interval> : nến real-time
  - @ticker           : 24h ticker snapshot
  - @trade            : market trades
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Callable, Awaitable

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

import config

logger = logging.getLogger(__name__)

DataCallback = Callable[[dict], Awaitable[None]]


class WebSocketCollector:
    def __init__(
        self,
        base_url:  str,
        on_kline:  DataCallback,
        on_ticker: DataCallback,
        on_trade:  DataCallback,
    ):
        self.base_url   = base_url
        self._on_kline   = on_kline
        self._on_ticker  = on_ticker
        self._on_trade   = on_trade
        self._running    = False

    # ── stream URL helpers ────────────────────────────────────────────────────

    @staticmethod
    def _build_streams(symbols: list[str], interval: str) -> list[str]:
        """Tạo list tất cả stream names cho các symbols."""
        streams = []
        for sym in symbols:
            s = sym.lower()
            streams.append(f"{s}@kline_{interval}")
            streams.append(f"{s}@ticker")
            streams.append(f"{s}@trade")
        return streams

    def _chunk_url(self, streams: list[str]) -> str:
        return f"{self.base_url}/stream?streams=" + "/".join(streams)

    # ── message dispatch ──────────────────────────────────────────────────────

    async def _dispatch(self, raw: str) -> None:
        try:
            msg    = json.loads(raw)
            stream = msg.get("stream", "")
            data   = msg.get("data", {})
        except (json.JSONDecodeError, AttributeError):
            return

        try:
            if "@kline_" in stream:
                await self._handle_kline(data)
            elif "@ticker" in stream:
                await self._handle_ticker(data)
            elif "@trade" in stream:
                await self._handle_trade(data)
        except Exception as e:
            logger.error("Dispatch error stream='%s': %s", stream, e)

    # ── handlers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _ts() -> str:
        return datetime.now(timezone.utc).isoformat()

    async def _handle_kline(self, data: dict) -> None:
        k = data.get("k", {})
        await self._on_kline({
            "symbol":                   data.get("s"),
            "event_time":               data.get("E"),
            "open_time":                k.get("t"),
            "close_time":               k.get("T"),
            "interval":                 k.get("i"),
            "open":                     float(k.get("o", 0)),
            "high":                     float(k.get("h", 0)),
            "low":                      float(k.get("l", 0)),
            "close":                    float(k.get("c", 0)),
            "volume":                   float(k.get("v", 0)),
            "quote_volume":             float(k.get("q", 0)),
            "num_trades":               int(k.get("n", 0)),
            "taker_buy_base_volume":    float(k.get("V", 0)),
            "taker_buy_quote_volume":   float(k.get("Q", 0)),
            "is_closed":                bool(k.get("x", False)),
            "source":                   "websocket",
            "timestamp":                self._ts(),
        })

    async def _handle_ticker(self, data: dict) -> None:
        await self._on_ticker({
            "symbol":             data.get("s"),
            "event_time":         data.get("E"),
            "price_change":       float(data.get("p", 0)),
            "price_change_pct":   float(data.get("P", 0)),
            "weighted_avg_price": float(data.get("w", 0)),
            "prev_close_price":   float(data.get("x", 0)),
            "last_price":         float(data.get("c", 0)),
            "last_qty":           float(data.get("Q", 0)),
            "bid_price":          float(data.get("b", 0)),
            "ask_price":          float(data.get("a", 0)),
            "open_price":         float(data.get("o", 0)),
            "high_price":         float(data.get("h", 0)),
            "low_price":          float(data.get("l", 0)),
            "volume":             float(data.get("v", 0)),
            "quote_volume":       float(data.get("q", 0)),
            "open_time":          data.get("O"),
            "close_time":         data.get("C"),
            "num_trades":         int(data.get("n", 0)),
            "source":             "websocket",
            "timestamp":          self._ts(),
        })

    async def _handle_trade(self, data: dict) -> None:
        p = float(data.get("p", 0))
        q = float(data.get("q", 0))
        await self._on_trade({
            "symbol":         data.get("s"),
            "event_time":     data.get("E"),
            "trade_id":       data.get("t"),
            "price":          p,
            "qty":            q,
            "quote_qty":      p * q,
            "time":           data.get("T"),
            "is_buyer_maker": data.get("m"),
            "source":         "websocket",
            "timestamp":      self._ts(),
        })

    # ── single connection loop ────────────────────────────────────────────────

    async def _stream_chunk(
        self,
        streams: list[str],
        conn_id: int,
        max_reconnects: int,
    ) -> None:
        """Chạy 1 WebSocket connection với danh sách streams cho trước."""
        url             = self._chunk_url(streams)
        reconnect_count = 0
        backoff         = config.WS_RECONNECT_DELAY
        tag             = f"[WS-{conn_id}]"

        logger.info("%s %d streams | URL: %s…", tag, len(streams), url[:120])

        while self._running and reconnect_count <= max_reconnects:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=30,
                    close_timeout=10,
                    max_size=10 * 1024 * 1024,   # 10 MB limit for incoming messages
                ) as ws:
                    logger.info("%s Connected (attempt %d)", tag, reconnect_count + 1)
                    reconnect_count = 0
                    backoff         = config.WS_RECONNECT_DELAY

                    async for message in ws:
                        if not self._running:
                            return
                        await self._dispatch(message)

            except ConnectionClosed as e:
                logger.warning("%s Connection closed: %s", tag, e)
            except WebSocketException as e:
                logger.error("%s WebSocket error: %s", tag, e)
            except asyncio.CancelledError:
                logger.info("%s Task cancelled", tag)
                return
            except Exception as e:
                logger.error("%s Unexpected error: %s", tag, e)

            if not self._running:
                break

            reconnect_count += 1
            if reconnect_count > max_reconnects:
                logger.error("%s Max reconnects (%d) reached", tag, max_reconnects)
                break

            logger.info("%s Reconnect %d/%d in %ds…", tag, reconnect_count, max_reconnects, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

        logger.info("%s Stream ended", tag)

    # ── main entry point ──────────────────────────────────────────────────────

    async def stream(
        self,
        symbols: list[str],
        interval: str,
        max_reconnects: int = 20,
    ) -> None:
        """
        Tách symbols thành nhiều WS connection dựa trên MAX_STREAMS_PER_CONNECTION.
        (Mỗi symbol hiện có 3 streams: kline, ticker, trade).
        """
        self._running = True

        all_streams = self._build_streams(symbols, interval)
        chunk_size  = config.MAX_STREAMS_PER_CONNECTION

        chunks = [
            all_streams[i : i + chunk_size]
            for i in range(0, len(all_streams), chunk_size)
        ]

        logger.info(
            "[WS] %d symbols → %d streams → %d connection(s) (%d streams each)",
            len(symbols), len(all_streams), len(chunks), chunk_size,
        )

        tasks = [
            asyncio.create_task(
                self._stream_chunk(chunk, conn_id, max_reconnects),
                name=f"ws-conn-{conn_id}",
            )
            for conn_id, chunk in enumerate(chunks)
        ]

        await asyncio.gather(*tasks)

    def stop(self) -> None:
        self._running = False
