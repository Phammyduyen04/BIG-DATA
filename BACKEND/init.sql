-- ============================================================
-- Crypto DW Schema - PostgreSQL 16
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

CREATE INDEX IF NOT EXISTS idx_symbols_code ON dim_symbols(symbol_code);

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
