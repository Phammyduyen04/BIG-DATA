-- ============================================================
-- Crypto DW Schema
-- ============================================================

-- ── dim_symbols ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_symbols (
    symbol_id   BIGSERIAL    PRIMARY KEY,
    symbol_code VARCHAR(20)  UNIQUE NOT NULL,
    base_asset  VARCHAR(20),
    quote_asset VARCHAR(20),
    created_at  TIMESTAMP    DEFAULT NOW(),
    updated_at  TIMESTAMP    DEFAULT NOW()
);

-- ── fact_klines ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_klines (
    kline_id               BIGSERIAL        PRIMARY KEY,
    symbol_id              BIGINT           NOT NULL,
    interval_code          VARCHAR(10)      NOT NULL,
    open_time              TIMESTAMP        NOT NULL,
    close_time             TIMESTAMP        NOT NULL,
    open_price             DECIMAL(30, 12)  NOT NULL,
    high_price             DECIMAL(30, 12)  NOT NULL,
    low_price              DECIMAL(30, 12)  NOT NULL,
    close_price            DECIMAL(30, 12)  NOT NULL,
    volume_base            DECIMAL(30, 12),
    volume_quote           DECIMAL(30, 12),
    num_trades             BIGINT,
    taker_buy_quote_volume DECIMAL(30, 12),
    CONSTRAINT uq_klines        UNIQUE (symbol_id, interval_code, open_time),
    CONSTRAINT fk_klines_symbol FOREIGN KEY (symbol_id) REFERENCES dim_symbols(symbol_id)
);
CREATE INDEX IF NOT EXISTS idx_klines_lookup
    ON fact_klines (symbol_id, interval_code, open_time DESC);

-- ── fact_ticker_24h_snapshots ─────────────────────────────
CREATE TABLE IF NOT EXISTS fact_ticker_24h_snapshots (
    ticker_snapshot_id   BIGSERIAL       PRIMARY KEY,
    symbol_id            BIGINT          NOT NULL,
    snapshot_time        TIMESTAMP       NOT NULL,
    price_change         DECIMAL(30, 12),
    price_change_percent DECIMAL(18, 8),
    last_price           DECIMAL(30, 12),
    high_price           DECIMAL(30, 12),
    low_price            DECIMAL(30, 12),
    volume_base_24h      DECIMAL(30, 12),
    volume_quote_24h     DECIMAL(30, 12),
    trade_count          BIGINT,
    ingested_at          TIMESTAMP       DEFAULT NOW(),
    CONSTRAINT uq_ticker_snapshot  UNIQUE (symbol_id, snapshot_time),
    CONSTRAINT fk_ticker_symbol    FOREIGN KEY (symbol_id) REFERENCES dim_symbols(symbol_id)
);
CREATE INDEX IF NOT EXISTS idx_ticker_lookup
    ON fact_ticker_24h_snapshots (symbol_id, snapshot_time DESC);

-- ── fact_raw_trades ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_raw_trades (
    raw_trade_sk   BIGSERIAL       PRIMARY KEY,
    symbol_id      BIGINT          NOT NULL,
    trade_id       BIGINT          NOT NULL,
    trade_time     TIMESTAMP       NOT NULL,
    price          DECIMAL(30, 12) NOT NULL,
    qty_base       DECIMAL(30, 12) NOT NULL,
    quote_qty      DECIMAL(30, 12),
    is_buyer_maker BOOLEAN,
    is_best_match  BOOLEAN,
    ingested_at    TIMESTAMP       DEFAULT NOW(),
    CONSTRAINT uq_raw_trades      UNIQUE (symbol_id, trade_id),
    CONSTRAINT fk_trades_symbol   FOREIGN KEY (symbol_id) REFERENCES dim_symbols(symbol_id)
);
CREATE INDEX IF NOT EXISTS idx_trades_lookup
    ON fact_raw_trades (symbol_id, trade_time DESC);

-- ── mart_trade_metrics ────────────────────────────────────
-- Aggregate money flow + whale signals per symbol
-- Được tính từ Spark trong etl_trades, ghi đè mỗi lần ETL chạy
CREATE TABLE IF NOT EXISTS mart_trade_metrics (
    symbol_id         BIGINT          PRIMARY KEY,
    total_buy_volume  DECIMAL(30,12),  -- quote_qty tổng lệnh mua chủ động (is_buyer_maker=False)
    total_sell_volume DECIMAL(30,12),  -- quote_qty tổng lệnh bán chủ động (is_buyer_maker=True)
    money_flow        DECIMAL(30,12),  -- buy - sell: dương = tiền vào, âm = tiền ra
    total_trades      BIGINT,
    whale_threshold   DECIMAL(30,12),  -- ngưỡng "lệnh lớn" = avg + 2*stddev per symbol
    whale_buy_volume  DECIMAL(30,12),  -- whale buy quote volume
    whale_sell_volume DECIMAL(30,12),  -- whale sell quote volume
    whale_buy_ratio   DECIMAL(10,6),   -- whale_buy / (whale_buy + whale_sell): 0.0→1.0
    computed_at       TIMESTAMP        DEFAULT NOW()
);

-- ── mart_whale_alerts ─────────────────────────────────────
-- Từng lệnh khớp lớn bất thường — dùng cho dashboard cảnh báo
-- Được tính từ Spark trong etl_trades, ghi đè mỗi lần ETL chạy
CREATE TABLE IF NOT EXISTS mart_whale_alerts (
    alert_id        BIGSERIAL       PRIMARY KEY,
    symbol_id       BIGINT          NOT NULL,
    trade_time      TIMESTAMP       NOT NULL,
    direction       VARCHAR(4)      NOT NULL,   -- 'BUY' | 'SELL'
    price           DECIMAL(30,12),
    quote_qty       DECIMAL(30,12),
    size_multiplier DECIMAL(10,2),              -- lớn gấp bao nhiêu lần lệnh TB của symbol
    alert_level     VARCHAR(10)     NOT NULL,   -- 'LARGE'(>2σ) | 'WHALE'(>3σ) | 'MEGA'(>4σ)
    computed_at     TIMESTAMP       DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_whale_alerts_symbol
    ON mart_whale_alerts (symbol_id, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_whale_alerts_level
    ON mart_whale_alerts (alert_level, trade_time DESC);

-- ── mart_top_coins ────────────────────────────────────────
-- Kết quả phân tích top 10 coin dài hạn
-- Dùng mode=append → tích lũy lịch sử mỗi lần ETL chạy
CREATE TABLE IF NOT EXISTS mart_top_coins (
    id                  BIGSERIAL       PRIMARY KEY,
    timeframe           VARCHAR(10)     NOT NULL,   -- '1d'
    rank                INT             NOT NULL,
    symbol_id           BIGINT          NOT NULL,
    symbol_code         VARCHAR(20)     NOT NULL,
    total_volume_quote  DECIMAL(30,12),             -- tổng volume_quote tất cả nến 1d
    total_num_trades    BIGINT,                     -- tổng số lệnh tất cả nến 1d
    avg_taker_buy_ratio DECIMAL(10,6),              -- trung bình taker_buy/volume_quote
    bullish_ratio       DECIMAL(10,6),              -- % nến xanh (close > open)
    volume_trend        DECIMAL(10,4),              -- avg_volume(nửa sau) / avg_volume(nửa đầu)
    money_flow          DECIMAL(30,12),             -- từ mart_trade_metrics
    whale_buy_ratio     DECIMAL(10,6),              -- từ mart_trade_metrics
    long_term_score     DECIMAL(10,4),              -- 0.0 → 1.0
    signal              VARCHAR(20),                -- ACCUMULATING | NEUTRAL | DISTRIBUTING
    computed_at         TIMESTAMP       DEFAULT NOW(),
    CONSTRAINT uq_top_coins UNIQUE (timeframe, rank, computed_at)
);
