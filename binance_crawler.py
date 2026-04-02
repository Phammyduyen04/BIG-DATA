"""
Binance Crypto Data Crawler
- Public endpoints: klines, tickers, depth, trades, aggTrades
- Private endpoints (cần API key): account info, balances, order history, trade history
Target: ~1200 requests
"""

import requests
import pandas as pd
import time
import json
import os
import hmac
import hashlib
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY    = os.getenv("BINANCE_API_KEY")
SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

BASE_URL   = "https://api.binance.com"
OUTPUT_DIR = "binance_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

REQUEST_DELAY = 0.05  # 50ms giữa mỗi request
total_requests = 0

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _sign(params: dict) -> dict:
    """Thêm timestamp + signature HMAC-SHA256 cho private endpoints."""
    params["timestamp"] = int(time.time() * 1000)
    query = "&".join(f"{k}={v}" for k, v in params.items())
    sig = hmac.new(SECRET_KEY.encode(), query.encode(), hashlib.sha256).hexdigest()
    params["signature"] = sig
    return params

def get_public(endpoint, params=None):
    global total_requests
    resp = requests.get(BASE_URL + endpoint, params=params, timeout=10)
    resp.raise_for_status()
    total_requests += 1
    time.sleep(REQUEST_DELAY)
    return resp.json()

def get_private(endpoint, params=None):
    global total_requests
    params = params or {}
    params = _sign(params)
    headers = {"X-MBX-APIKEY": API_KEY}
    resp = requests.get(BASE_URL + endpoint, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    total_requests += 1
    time.sleep(REQUEST_DELAY)
    return resp.json()

def save_json(data, filename):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    n = len(data) if isinstance(data, list) else 1
    print(f"  [saved] {filename}  ({n} records)")

def save_csv(df: pd.DataFrame, filename):
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"  [saved] {filename}  ({len(df)} rows × {len(df.columns)} cols)")

def log_progress(step_name):
    print(f"\n{'-'*55}")
    print(f"  {step_name}  |  requests so far: {total_requests}")
    print(f"{'-'*55}")

# ──────────────────────────────────────────────
# PUBLIC ENDPOINTS
# ──────────────────────────────────────────────

# 1. Exchange Info  (1 req)
def fetch_exchange_info():
    log_progress("[1] Exchange Info")
    data = get_public("/api/v3/exchangeInfo")
    save_json(data, "exchange_info.json")
    rows = [
        {
            "symbol":                 s["symbol"],
            "baseAsset":              s["baseAsset"],
            "quoteAsset":             s["quoteAsset"],
            "status":                 s["status"],
            "baseAssetPrecision":     s["baseAssetPrecision"],
            "quoteAssetPrecision":    s["quoteAssetPrecision"],
            "isSpotTradingAllowed":   s["isSpotTradingAllowed"],
            "isMarginTradingAllowed": s["isMarginTradingAllowed"],
            "orderTypes":             ",".join(s["orderTypes"]),
        }
        for s in data["symbols"]
    ]
    save_csv(pd.DataFrame(rows), "exchange_symbols.csv")
    return data

# 2. All 24hr Tickers  (1 req)
def fetch_all_tickers():
    log_progress("[2] All 24hr Tickers")
    data = get_public("/api/v3/ticker/24hr")
    df = pd.DataFrame(data)
    df["openTime_dt"]  = pd.to_datetime(df["openTime"],  unit="ms")
    df["closeTime_dt"] = pd.to_datetime(df["closeTime"], unit="ms")
    save_csv(df, "tickers_24hr_all.csv")
    return df

# 3. All Book Tickers  (1 req)
def fetch_all_book_tickers():
    log_progress("[3] Book Tickers (best bid/ask)")
    data = get_public("/api/v3/ticker/bookTicker")
    save_csv(pd.DataFrame(data), "book_tickers_all.csv")

# 4. Latest Prices  (1 req)
def fetch_latest_prices():
    log_progress("[4] Latest Prices")
    data = get_public("/api/v3/ticker/price")
    save_csv(pd.DataFrame(data), "latest_prices.csv")

# 5. Klines
KLINE_COLS = [
    "openTime", "open", "high", "low", "close", "volume",
    "closeTime", "quoteAssetVolume", "numberOfTrades",
    "takerBuyBaseVolume", "takerBuyQuoteVolume", "ignore",
]

def fetch_klines(symbol, interval, limit=500):
    data = get_public("/api/v3/klines",
                      {"symbol": symbol, "interval": interval, "limit": limit})
    df = pd.DataFrame(data, columns=KLINE_COLS)
    df["openTime_dt"]  = pd.to_datetime(df["openTime"],  unit="ms")
    df["closeTime_dt"] = pd.to_datetime(df["closeTime"], unit="ms")
    df["symbol"]   = symbol
    df["interval"] = interval
    for c in ["open","high","low","close","volume",
              "quoteAssetVolume","takerBuyBaseVolume","takerBuyQuoteVolume"]:
        df[c] = df[c].astype(float)
    df["numberOfTrades"] = df["numberOfTrades"].astype(int)
    return df

# 6. Order Book Depth
def fetch_depth(symbol, limit=100):
    data = get_public("/api/v3/depth", {"symbol": symbol, "limit": limit})
    rows = []
    for side, entries in [("bid", data["bids"]), ("ask", data["asks"])]:
        for price, qty in entries:
            rows.append({
                "symbol": symbol, "lastUpdateId": data["lastUpdateId"],
                "side": side, "price": float(price), "qty": float(qty),
            })
    return pd.DataFrame(rows)

# 7. Recent Trades
def fetch_trades(symbol, limit=500):
    data = get_public("/api/v3/trades", {"symbol": symbol, "limit": limit})
    df = pd.DataFrame(data)
    df["symbol"]  = symbol
    df["time_dt"] = pd.to_datetime(df["time"], unit="ms")
    return df

# 8. Aggregate Trades
def fetch_agg_trades(symbol, limit=500):
    data = get_public("/api/v3/aggTrades", {"symbol": symbol, "limit": limit})
    df = pd.DataFrame(data)
    df.rename(columns={
        "a": "aggTradeId", "p": "price", "q": "qty",
        "f": "firstTradeId", "l": "lastTradeId",
        "T": "timestamp", "m": "isBuyerMaker", "M": "isBestMatch",
    }, inplace=True)
    df["symbol"]  = symbol
    df["time_dt"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df

# ──────────────────────────────────────────────
# PRIVATE ENDPOINTS (cần API key)
# ──────────────────────────────────────────────

# 9. Account Info + Balances
def fetch_account_info():
    log_progress("[9] Account Info & Balances (PRIVATE)")
    data = get_private("/api/v3/account")
    # Tất cả các trường account:
    # makerCommission, takerCommission, buyerCommission, sellerCommission,
    # commissionRates, canTrade, canWithdraw, canDeposit, brokered,
    # requireSelfTradePrevention, preventSor, updateTime, accountType,
    # balances (asset, free, locked), permissions, uid
    save_json(data, "account_info.json")

    balances = pd.DataFrame(data.get("balances", []))
    balances["free"]   = balances["free"].astype(float)
    balances["locked"] = balances["locked"].astype(float)
    balances["total"]  = balances["free"] + balances["locked"]
    # Lọc có số dư
    non_zero = balances[balances["total"] > 0]
    save_csv(balances, "balances_all.csv")
    save_csv(non_zero, "balances_nonzero.csv")
    return data

# 10. My Trades (lịch sử giao dịch) cho từng symbol
# Trường: symbol, id, orderId, orderListId, price, qty, quoteQty,
#          commission, commissionAsset, time, isBuyer, isMaker, isBestMatch
def fetch_my_trades(symbol, limit=500):
    try:
        data = get_private("/api/v3/myTrades",
                           {"symbol": symbol, "limit": limit})
        if not data:
            return None
        df = pd.DataFrame(data)
        df["symbol"]  = symbol
        df["time_dt"] = pd.to_datetime(df["time"], unit="ms")
        return df
    except Exception as e:
        print(f"    [WARN] myTrades {symbol}: {e}")
        return None

# 11. All Orders (lịch sử lệnh) cho từng symbol
# Trường: symbol, orderId, orderListId, clientOrderId, price, origQty,
#          executedQty, cummulativeQuoteQty, status, timeInForce, type, side,
#          stopPrice, icebergQty, time, updateTime, isWorking, origQuoteOrderQty
def fetch_all_orders(symbol, limit=500):
    try:
        data = get_private("/api/v3/allOrders",
                           {"symbol": symbol, "limit": limit})
        if not data:
            return None
        df = pd.DataFrame(data)
        df["time_dt"]       = pd.to_datetime(df["time"],       unit="ms")
        df["updateTime_dt"] = pd.to_datetime(df["updateTime"], unit="ms")
        return df
    except Exception as e:
        print(f"    [WARN] allOrders {symbol}: {e}")
        return None

# 12. Open Orders
def fetch_open_orders():
    log_progress("[12] Open Orders (PRIVATE)")
    try:
        data = get_private("/api/v3/openOrders")
        df = pd.DataFrame(data) if data else pd.DataFrame()
        save_csv(df if not df.empty else pd.DataFrame(columns=["note"]),
                 "open_orders.csv")
    except Exception as e:
        print(f"    [WARN] openOrders: {e}")

# 13. Account Trade List tổng hợp (dùng /api/v3/myTrades với recvWindow lớn)
def fetch_account_trades_summary(symbols_with_history):
    log_progress("[13] My Trade History (PRIVATE)")
    trade_frames = []
    for sym in symbols_with_history:
        if total_requests >= 1195:
            break
        df = fetch_my_trades(sym)
        if df is not None and not df.empty:
            trade_frames.append(df)
    if trade_frames:
        save_csv(pd.concat(trade_frames, ignore_index=True), "my_trades_history.csv")

# 14. All Orders History tổng hợp
def fetch_all_orders_history(symbols_with_history):
    log_progress("[14] All Orders History (PRIVATE)")
    order_frames = []
    for sym in symbols_with_history:
        if total_requests >= 1200:
            break
        df = fetch_all_orders(sym)
        if df is not None and not df.empty:
            order_frames.append(df)
    if order_frames:
        save_csv(pd.concat(order_frames, ignore_index=True), "all_orders_history.csv")

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    if not API_KEY or not SECRET_KEY:
        print("[ERROR] Không tìm thấy BINANCE_API_KEY / BINANCE_SECRET_KEY trong .env")
        return

    start = time.time()
    print("=" * 55)
    print("  BINANCE DATA CRAWLER  -  Target: 1200 requests")
    print("=" * 55)

    # ── Public ─────────────────────────────────
    fetch_exchange_info()              # req 1
    ticker_df = fetch_all_tickers()    # req 2
    fetch_all_book_tickers()           # req 3
    fetch_latest_prices()              # req 4

    # Top 100 USDT symbols
    usdt = ticker_df[ticker_df["symbol"].str.endswith("USDT")].copy()
    usdt["quoteVolume"] = usdt["quoteVolume"].astype(float)
    top100 = usdt.sort_values("quoteVolume", ascending=False).head(100)["symbol"].tolist()
    pd.DataFrame({"symbol": top100}).to_csv(
        os.path.join(OUTPUT_DIR, "selected_symbols.csv"), index=False)
    print(f"\n  Top {len(top100)} USDT symbols selected.")

    # Klines – 7 intervals × 100 symbols = 700 req  (tổng ~704)
    INTERVALS = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]
    log_progress(f"[5] Klines {INTERVALS} × {len(top100)} symbols = {len(INTERVALS)*len(top100)} req")
    kline_frames = {iv: [] for iv in INTERVALS}
    for i, sym in enumerate(top100, 1):
        for iv in INTERVALS:
            try:
                kline_frames[iv].append(fetch_klines(sym, iv, limit=500))
            except Exception as e:
                print(f"    [WARN] klines {sym} {iv}: {e}")
        if i % 20 == 0:
            print(f"    klines: {i}/100 symbols | requests={total_requests}")
    for iv in INTERVALS:
        if kline_frames[iv]:
            save_csv(pd.concat(kline_frames[iv], ignore_index=True), f"klines_{iv}.csv")

    # Depth – 50 symbols = 50 req  (tổng ~754)
    log_progress(f"[6] Order Book Depth – 50 symbols")
    depth_frames = []
    for sym in top100[:50]:
        try:
            depth_frames.append(fetch_depth(sym))
        except Exception as e:
            print(f"    [WARN] depth {sym}: {e}")
    if depth_frames:
        save_csv(pd.concat(depth_frames, ignore_index=True), "order_book_depth.csv")

    # Recent Trades – 50 symbols = 50 req  (tổng ~804)
    log_progress(f"[7] Recent Trades – 50 symbols")
    trade_frames = []
    for sym in top100[:50]:
        try:
            trade_frames.append(fetch_trades(sym))
        except Exception as e:
            print(f"    [WARN] trades {sym}: {e}")
    if trade_frames:
        save_csv(pd.concat(trade_frames, ignore_index=True), "recent_trades.csv")

    # Agg Trades – 50 symbols = 50 req  (tổng ~854)
    log_progress(f"[8] Agg Trades – 50 symbols")
    agg_frames = []
    for sym in top100[:50]:
        try:
            agg_frames.append(fetch_agg_trades(sym))
        except Exception as e:
            print(f"    [WARN] aggTrades {sym}: {e}")
    if agg_frames:
        save_csv(pd.concat(agg_frames, ignore_index=True), "agg_trades.csv")

    # ── Private ────────────────────────────────  (~854 → 1200)
    fetch_account_info()     # req ~855
    fetch_open_orders()      # req ~856

    # My Trades + All Orders cho top symbols  (tối đa ~170 symbols × 2 = 340 req)
    # Đủ để đạt 1200 tổng cộng
    remaining = 1200 - total_requests
    n_private_syms = min(remaining // 2, len(top100))
    print(f"\n  Private history: sẽ fetch {n_private_syms} symbols × (myTrades + allOrders)")

    fetch_account_trades_summary(top100[:n_private_syms])   # n req
    fetch_all_orders_history(top100[:n_private_syms])       # n req

    # ── Summary ────────────────────────────────
    elapsed = time.time() - start
    files   = os.listdir(OUTPUT_DIR)
    total_size = sum(os.path.getsize(os.path.join(OUTPUT_DIR, f)) for f in files)

    print("\n" + "=" * 55)
    print(f"  DONE!")
    print(f"  Total requests : {total_requests}")
    print(f"  Elapsed        : {elapsed:.1f}s  ({elapsed/60:.1f} min)")
    print(f"  Files saved    : {len(files)}")
    print(f"  Total size     : {total_size/1024/1024:.1f} MB")
    print(f"  Output folder  : {os.path.abspath(OUTPUT_DIR)}/")
    print("=" * 55)

    save_json({
        "crawl_timestamp": datetime.utcnow().isoformat() + "Z",
        "total_requests":  total_requests,
        "elapsed_seconds": round(elapsed, 2),
        "symbols_count":   len(top100),
        "top_symbols":     top100,
        "files":           files,
    }, "crawl_metadata.json")


if __name__ == "__main__":
    main()
