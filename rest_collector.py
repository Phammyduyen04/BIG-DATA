"""
REST Collector
--------------
- fetch_top_symbols()   : lấy top N đồng theo 24h quote volume
- fetch_all_tickers()   : snapshot tất cả ticker 24h (1 request duy nhất)
- fetch_klines()        : nến lịch sử

Rate limiting: dùng asyncio.Semaphore để không vượt 1200 weight/phút.
"""

import asyncio
import logging
from datetime import datetime, timezone

import aiohttp

import config

logger = logging.getLogger(__name__)

# Semaphore dùng chung cho toàn bộ ứng dụng
_sem: asyncio.Semaphore | None = None

def _get_sem() -> asyncio.Semaphore:
    global _sem
    if _sem is None:
        _sem = asyncio.Semaphore(config.REST_CONCURRENCY)
    return _sem


class RestCollector:
    def __init__(self, base_url: str, api_key: str = ""):
        self.base_url = base_url
        self._headers = {"X-MBX-APIKEY": api_key} if api_key else {}
        self._session: aiohttp.ClientSession | None = None

    # ── session lifecycle ────────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self._headers)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ── helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    async def _get(self, endpoint: str, params: dict) -> dict | list | None:
        session = await self._get_session()
        url     = f"{self.base_url}{endpoint}"
        async with _get_sem():
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    resp.raise_for_status()
                    return await resp.json()
            except aiohttp.ClientResponseError as e:
                logger.error("HTTP %s %s %s – %s", e.status, endpoint, params, e.message)
            except asyncio.TimeoutError:
                logger.error("Timeout %s %s", endpoint, params)
            except Exception as e:
                logger.error("Request error %s: %s", endpoint, e)
        return None

    # ── symbol discovery ─────────────────────────────────────────────────────

    async def fetch_top_symbols(
        self,
        n: int = 100,
        quote_asset: str = "USDT",
    ) -> list[str]:
        """
        Trả về top N symbol USDT theo 24h quoteVolume giảm dần.
        Loại bỏ leveraged tokens (3L/3S, UP/DOWN, BULL/BEAR).
        """
        raw = await self._get("/ticker/24hr", {})
        if not raw:
            logger.error("Không lấy được danh sách ticker để chọn symbols")
            return []

        filtered = [
            t for t in raw
            if t["symbol"].endswith(quote_asset)
            and not any(t["symbol"].endswith(s) for s in config.SYMBOL_BLOCKLIST_SUFFIXES)
            and float(t["quoteVolume"]) > 0
        ]
        filtered.sort(key=lambda t: float(t["quoteVolume"]), reverse=True)

        symbols = [t["symbol"] for t in filtered[:n]]
        logger.info("Top %d symbols (by 24h volume): %s … %s",
                    len(symbols), symbols[:5], symbols[-3:])
        return symbols

    # ── data fetchers ─────────────────────────────────────────────────────────

    async def fetch_all_tickers(self, symbols: list[str]) -> list[dict]:
        """
        Lấy 24h ticker cho nhiều symbol trong 1 request duy nhất.
        Trả về list dict đã chuẩn hoá.
        """
        raw = await self._get("/ticker/24hr", {})
        if not raw:
            return []

        sym_set  = set(symbols)
        now      = self._now_iso()
        records  = []

        for t in raw:
            if t["symbol"] not in sym_set:
                continue
            records.append({
                "symbol":             t["symbol"],
                "price_change":       float(t["priceChange"]),
                "price_change_pct":   float(t["priceChangePercent"]),
                "weighted_avg_price": float(t["weightedAvgPrice"]),
                "prev_close_price":   float(t["prevClosePrice"]),
                "last_price":         float(t["lastPrice"]),
                "last_qty":           float(t["lastQty"]),
                "bid_price":          float(t["bidPrice"]),
                "ask_price":          float(t["askPrice"]),
                "open_price":         float(t["openPrice"]),
                "high_price":         float(t["highPrice"]),
                "low_price":          float(t["lowPrice"]),
                "volume":             float(t["volume"]),
                "quote_volume":       float(t["quoteVolume"]),
                "open_time":          int(t["openTime"]),
                "close_time":         int(t["closeTime"]),
                "num_trades":         int(t["count"]),
                "event_time":         None,
                "source":             "rest_snapshot",
                "timestamp":          now,
            })

        logger.info("[REST] Ticker snapshot: %d/%d symbols", len(records), len(symbols))
        return records

    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 1440,
    ) -> list[dict]:
        raw = await self._get("/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        if not raw:
            return []

        now = self._now_iso()
        return [
            {
                "symbol":                   symbol,
                "open_time":                int(k[0]),
                "open":                     float(k[1]),
                "high":                     float(k[2]),
                "low":                      float(k[3]),
                "close":                    float(k[4]),
                "volume":                   float(k[5]),
                "close_time":               int(k[6]),
                "quote_volume":             float(k[7]),
                "num_trades":               int(k[8]),
                "taker_buy_base_volume":    float(k[9]),
                "taker_buy_quote_volume":   float(k[10]),
                "interval":                 interval,
                "is_closed":                True,
                "event_time":               None,
                "source":                   "rest_historical",
                "timestamp":                now,
            }
            for k in raw
        ]



    # ── batch helpers ─────────────────────────────────────────────────────────

    async def fetch_klines_batch(
        self,
        symbols: list[str],
        interval: str,
        limit: int,
    ) -> list[dict]:
        """Fetch klines cho nhiều symbol đồng thời (giới hạn bởi semaphore)."""
        tasks   = [self.fetch_klines(s, interval, limit) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_records = []
        for sym, res in zip(symbols, results):
            if isinstance(res, Exception):
                logger.error("[REST] Klines lỗi %s: %s", sym, res)
            elif res:
                all_records.extend(res)

        logger.info("[REST] Klines batch: %d symbols → %d nến", len(symbols), len(all_records))
        return all_records
