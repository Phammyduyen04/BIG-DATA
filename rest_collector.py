import asyncio
import logging
from datetime import datetime, timezone, timedelta
import aiohttp
import config

logger = logging.getLogger(__name__)

# Semaphore dùng chung cho toàn bộ ứng dụng REST
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

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self._headers)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    async def _get(self, endpoint: str, params: dict, max_retries: int = 3) -> dict | list | None:
        session = await self._get_session()
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(max_retries):
            async with _get_sem():
                try:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        if resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", 5))
                            logger.warning(f"[REST] Rate limited (429). Retrying after {retry_after}s...")
                            await asyncio.sleep(retry_after)
                            continue
                        
                        resp.raise_for_status()
                        return await resp.json()
                except aiohttp.ClientResponseError as e:
                    logger.error(f"HTTP {e.status} {endpoint} {params} – {e.message}")
                    if e.status >= 500: # Rerun on server error
                        await asyncio.sleep(2 ** attempt)
                        continue
                    break
                except asyncio.TimeoutError:
                    logger.error(f"Timeout {endpoint} {params}")
                except Exception as e:
                    logger.error(f"Request error {endpoint}: {e}")
            
            # Backoff for general failures
            await asyncio.sleep(2 ** attempt)
            
        return None

    # ── symbol discovery ─────────────────────────────────────────────────────

    async def fetch_top_symbols(self, n: int = 100, quote_asset: str = "USDT") -> list[str]:
        """
        Lấy top N symbols theo logic của nhánh streaming-data (Volume + Activity).
        """
        raw = await self._get("/ticker/24hr", {})
        if not raw:
            return []

        filtered = [
            t for t in raw
            if t["symbol"].endswith(quote_asset)
            and not any(t["symbol"].endswith(s) for s in config.SYMBOL_BLOCKLIST_SUFFIXES)
            and float(t["quoteVolume"]) > 0
        ]
        # Xếp hạng theo quoteVolume giảm dần (đại diện cho Volume + Activity)
        filtered.sort(key=lambda t: float(t["quoteVolume"]), reverse=True)

        symbols = [t["symbol"] for t in filtered[:n]]
        logger.info(f"[REST] Đã khám phá {len(symbols)} symbols theo logic Top Volume.")
        return symbols

    # ── market data fetchers ──────────────────────────────────────────────────

    async def fetch_all_tickers(self, symbols: list[str]) -> list[dict]:
        raw = await self._get("/ticker/24hr", {})
        if not raw: return []
        sym_set = set(symbols)
        now = self._now_iso()
        records = []
        for t in raw:
            if t["symbol"] not in sym_set: continue
            records.append({
                "symbol": t["symbol"], "price_change": float(t["priceChange"]),
                "price_change_pct": float(t["priceChangePercent"]), "weighted_avg_price": float(t["weightedAvgPrice"]),
                "prev_close_price": float(t["prevClosePrice"]), "last_price": float(t["lastPrice"]),
                "last_qty": float(t["lastQty"]), "bid_price": float(t["bidPrice"]),
                "ask_price": float(t["askPrice"]), "open_price": float(t["openPrice"]),
                "high_price": float(t["highPrice"]), "low_price": float(t["lowPrice"]),
                "volume": float(t["volume"]), "quote_volume": float(t["quoteVolume"]),
                "open_time": int(t["openTime"]), "close_time": int(t["closeTime"]),
                "num_trades": int(t["count"]), "event_time": None, "source": "rest_snapshot", "timestamp": now,
            })
        return records

    async def fetch_klines(self, symbol: str, interval: str, limit: int = 1000, start_time: int = None) -> list[dict]:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time: params["startTime"] = start_time
        
        raw = await self._get("/klines", params)
        if not raw: return []
        now = self._now_iso()
        return [{
            "symbol": symbol, "open_time": int(k[0]), "open": float(k[1]), "high": float(k[2]),
            "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]), "close_time": int(k[6]),
            "quote_volume": float(k[7]), "num_trades": int(k[8]), "taker_buy_base_volume": float(k[9]),
            "taker_buy_quote_volume": float(k[10]), "interval": interval, "is_closed": True,
            "event_time": None, "source": "rest_historical", "timestamp": now,
        } for k in raw]

    async def fetch_klines_recursive(self, symbol: str, interval: str, total_limit: int) -> list[dict]:
        """Fetch nến lịch sử sâu (ví dụ 3 tháng) bằng cách lặp lại request."""
        all_klines = []
        current_limit = total_limit
        last_start_time = None
        
        while current_limit > 0:
            batch_size = min(current_limit, 1000)
            batch = await self.fetch_klines(symbol, interval, limit=batch_size, start_time=last_start_time)
            if not batch: break
            
            all_klines.extend(batch)
            current_limit -= len(batch)
            # Binance klines trả về theo thứ tự thời gian tăng dần. 
            # Request tiếp theo bắt đầu sau nến cuối cùng.
            last_start_time = batch[-1]["close_time"] + 1
            if len(batch) < batch_size: break # Hết dữ liệu
            
        return all_klines

    async def fetch_agg_trades(self, symbol: str, start_time: int, end_time: int) -> list[dict]:
        """Fetch aggregate trades cho 1 cửa sổ thời gian."""
        raw = await self._get("/aggTrades", {"symbol": symbol, "startTime": start_time, "endTime": end_time, "limit": 1000})
        if not raw: return []
        now = self._now_iso()
        return [{
            "symbol": symbol,
            "trade_id": int(t["a"]),
            "price": float(t["p"]),
            "qty": float(t["q"]),
            "first_trade_id": int(t["f"]),
            "last_trade_id": int(t["l"]),
            "time": int(t["T"]),
            "is_buyer_maker": t["m"],
            "is_best_match": t["M"],
            "source": "rest_agg_trade",
            "timestamp": now
        } for t in raw]

    async def fetch_agg_trades_historical(self, symbol: str, days: int) -> list[dict]:
        """Backfill aggTrades cho N ngày bằng cách trượt cửa sổ thời gian."""
        all_trades = []
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days)
        
        current_start = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)
        
        # aggTrades window limit thường là 1h
        window_ms = 60 * 60 * 1000 
        
        while current_start < end_ts:
            current_end = min(current_start + window_ms, end_ts)
            batch = await self.fetch_agg_trades(symbol, current_start, current_end)
            if batch:
                all_trades.extend(batch)
            current_start = current_end
            if len(all_trades) % 5000 == 0 and batch:
                logger.debug(f"[REST] {symbol} backfilled {len(all_trades)} trades...")
                
        return all_trades

    # ── batch processing ──────────────────────────────────────────────────────

    async def backfill_klines_batch(self, symbols: list[str], interval: str, total_limit: int, storage) -> int:
        """Thực hiện backfill klines cho toàn danh sách (có giới hạn song song)."""
        logger.info(f"[BOOTSTRAP] Khởi động Klines backfill ({total_limit} nến/symbol)...")
        
        count = 0
        for symbol in symbols:
            klines = await self.fetch_klines_recursive(symbol, interval, total_limit)
            for k in klines:
                storage.save_kline(k)
            count += len(klines)
            logger.info(f"[BOOTSTRAP] {symbol}: {len(klines)} klines backfilled.")
        return count

    async def backfill_trades_batch(self, symbols: list[str], days: int, storage) -> int:
        """Thực hiện backfill trades cho toàn danh sách (aggTrades)."""
        logger.info(f"[BOOTSTRAP] Khởi động Trades backfill ({days} ngày/symbol)...")
        
        count = 0
        for symbol in symbols:
            trades = await self.fetch_agg_trades_historical(symbol, days)
            for t in trades:
                storage.save_trade(t)
            count += len(trades)
            logger.info(f"[BOOTSTRAP] {symbol}: {len(trades)} aggTrades backfilled.")
        return count
