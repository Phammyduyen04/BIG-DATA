import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Binance credentials ───────────────────────────────────────────────────────
API_KEY    = os.getenv("BINANCE_API_KEY", "")
SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")

# ── Symbol discovery ──────────────────────────────────────────────────────────
TOP_N_SYMBOLS  = 100          # lấy top N theo 24h quote volume
QUOTE_ASSET    = "USDT"       # chỉ lấy cặp USDT

# Loại bỏ leveraged/synthetic tokens
SYMBOL_BLOCKLIST_SUFFIXES = (
    "UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT",
    "3LUSDT", "3SUSDT", "2LUSDT", "2SUSDT",
)

# ── Kline timeframe ───────────────────────────────────────────────────────────
KLINE_INTERVAL = "1m"

# ── REST base URL ─────────────────────────────────────────────────────────────
REST_BASE_URL = "https://api.binance.com/api/v3"

# ── WebSocket base URL ────────────────────────────────────────────────────────
WS_BASE_URL = "wss://stream.binance.com:9443"

# ── Storage ───────────────────────────────────────────────────────────────────
DATA_DIR = Path("data")
LOG_DIR  = Path("logs")

# ── Duration: 1 day ───────────────────────────────────────────────────────────
STREAM_DURATION_SECONDS = 24 * 60 * 60

# ── Historical klines lookback (1440 = 24h × 60 min at 1m) ───────────────────
KLINES_LOOKBACK_LIMIT = 1440

# ── Order book depth levels ───────────────────────────────────────────────────
DEPTH_LEVELS = 20

# ── WebSocket ─────────────────────────────────────────────────────────────────
MAX_STREAMS_PER_CONNECTION = 200   # Binance limit: 1024, dùng 200 để an toàn
WS_RECONNECT_DELAY         = 5     # back-off ban đầu (giây)
WS_MAX_RECONNECTS          = 20

# ── REST concurrency (tránh rate limit 1200 weight/phút) ─────────────────────
REST_CONCURRENCY = 10              # max 10 request REST đồng thời

# ── Periodic REST refresh (giây) ─────────────────────────────────────────────
REST_REFRESH_INTERVAL = 60
