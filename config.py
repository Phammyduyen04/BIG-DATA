import os
from pathlib import Path
from dotenv import load_dotenv

# Ưu tiên load file môi trường từ biến ENV_FILE (dùng cho Spark Processing)
env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(dotenv_path=env_file)

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

# ── MinIO Storage (Spark Processing) ──────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "binance")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Prefixes dữ liệu (Ghép thành: s3a://{bucket}/{prefix})
PREFIX_KLINES    = os.getenv("MINIO_PREFIX_KLINES", "raw/klines/")
PREFIX_DEPTH     = os.getenv("MINIO_PREFIX_DEPTH",  "raw/depth/")
PREFIX_TICKER    = os.getenv("MINIO_PREFIX_TICKER", "raw/ticker/")

SPARK_APP_NAME   = os.getenv("SPARK_APP_NAME", "read-minio-json")
