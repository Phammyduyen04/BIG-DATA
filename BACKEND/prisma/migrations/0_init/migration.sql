-- CreateTable DimSymbol
CREATE TABLE IF NOT EXISTS "dim_symbols" (
    "symbol_id" BIGSERIAL NOT NULL PRIMARY KEY,
    "symbol_code" VARCHAR(20) NOT NULL UNIQUE,
    "base_asset" VARCHAR(20),
    "quote_asset" VARCHAR(20),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateIndex idx_symbols_code
CREATE INDEX "idx_symbols_code" ON "dim_symbols"("symbol_code");

-- CreateTable FactKline
CREATE TABLE IF NOT EXISTS "fact_klines" (
    "kline_id" BIGSERIAL NOT NULL PRIMARY KEY,
    "symbol_id" BIGINT NOT NULL,
    "interval_code" VARCHAR(10) NOT NULL,
    "open_time" TIMESTAMP(3) NOT NULL,
    "close_time" TIMESTAMP(3) NOT NULL,
    "open_price" DECIMAL(30,12) NOT NULL,
    "high_price" DECIMAL(30,12) NOT NULL,
    "low_price" DECIMAL(30,12) NOT NULL,
    "close_price" DECIMAL(30,12) NOT NULL,
    "volume_base" DECIMAL(30,12),
    "volume_quote" DECIMAL(30,12),
    "num_trades" BIGINT,
    "taker_buy_quote_volume" DECIMAL(30,12),

    CONSTRAINT "uq_klines" UNIQUE("symbol_id","interval_code","open_time"),
    CONSTRAINT "fk_klines_symbol" FOREIGN KEY ("symbol_id") REFERENCES "dim_symbols"("symbol_id") ON DELETE CASCADE
);

-- CreateIndex idx_klines_lookup
CREATE INDEX "idx_klines_lookup" ON "fact_klines"("symbol_id","interval_code","open_time" DESC);

-- CreateTable FactTicker24hSnapshot
CREATE TABLE IF NOT EXISTS "fact_ticker_24h_snapshots" (
    "ticker_snapshot_id" BIGSERIAL NOT NULL PRIMARY KEY,
    "symbol_id" BIGINT NOT NULL,
    "snapshot_time" TIMESTAMP(3) NOT NULL,
    "price_change" DECIMAL(30,12),
    "price_change_percent" DECIMAL(18,8),
    "last_price" DECIMAL(30,12),
    "high_price" DECIMAL(30,12),
    "low_price" DECIMAL(30,12),
    "volume_base_24h" DECIMAL(30,12),
    "volume_quote_24h" DECIMAL(30,12),
    "trade_count" BIGINT,
    "ingested_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "uq_ticker_snapshot" UNIQUE("symbol_id","snapshot_time"),
    CONSTRAINT "fk_ticker_symbol" FOREIGN KEY ("symbol_id") REFERENCES "dim_symbols"("symbol_id") ON DELETE CASCADE
);

-- CreateIndex idx_ticker_lookup
CREATE INDEX "idx_ticker_lookup" ON "fact_ticker_24h_snapshots"("symbol_id","snapshot_time" DESC);

-- CreateTable FactRawTrade
CREATE TABLE IF NOT EXISTS "fact_raw_trades" (
    "raw_trade_sk" BIGSERIAL NOT NULL PRIMARY KEY,
    "symbol_id" BIGINT NOT NULL,
    "trade_id" BIGINT NOT NULL,
    "trade_time" TIMESTAMP(3) NOT NULL,
    "price" DECIMAL(30,12) NOT NULL,
    "qty_base" DECIMAL(30,12) NOT NULL,
    "quote_qty" DECIMAL(30,12),
    "is_buyer_maker" BOOLEAN,
    "is_best_match" BOOLEAN,
    "ingested_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "uq_raw_trades" UNIQUE("symbol_id","trade_id"),
    CONSTRAINT "fk_trades_symbol" FOREIGN KEY ("symbol_id") REFERENCES "dim_symbols"("symbol_id") ON DELETE CASCADE
);

-- CreateIndex idx_trades_lookup
CREATE INDEX "idx_trades_lookup" ON "fact_raw_trades"("symbol_id","trade_time" DESC);
