"""
Single source of truth for silver-layer Parquet schema conventions.

Imported by:
  - consumer_to_minio.py      (production landing writer)
  - scratch/emergency_ingest_v2.py  (rescue writer)
  - scripts/scan_silver_schemas.py  (diagnostic)
  - scripts/rewrite_mixed_parquet.py (backfill)

WHY: Binance API returns OHLCV as raw strings, but upstream producers may
float()-cast some values. Without an explicit PyArrow schema,
`pa.Table.from_pylist` infers column types per batch, producing a mix of
STRING and DOUBLE Parquet files. Spark's vectorized reader cannot reconcile
these — it picks one schema from a sample file and fails on the rest.

This module pins OHLCV-like fields to `str` at the writer, yielding a stable
STRING schema that the Spark ETL reader casts to decimal(30,12) losslessly
(matches the data-processing branch canonical semantics).

MAINTAINER: when adding a new topic or Binance adds a numeric payload field,
update NUMERIC_TO_STRING_FIELDS here — every consumer and tool picks it up.
"""

SILVER_PREFIX_MAP = {
    "binance.kline.1m.raw": "silver/klines/interval=1m",
    "binance.ticker.raw":   "silver/ticker",
    "binance.trade.raw":    "silver/trades",
}

# Covers both normalized (snake_case / camelCase post-_restore_contract) and
# WebSocket raw short-form field names, so it works whether the writer runs
# the contract normalization or not (rescue writer skips it).
NUMERIC_TO_STRING_FIELDS = {
    "binance.kline.1m.raw": frozenset({
        "open", "high", "low", "close",
        "volume", "quote_volume",
        "taker_buy_base_vol", "taker_buy_base_volume",
        "taker_buy_quote_vol", "taker_buy_quote_volume",
        "o", "h", "l", "c", "v", "q", "V", "Q",
    }),
    "binance.ticker.raw": frozenset({
        "priceChange", "priceChangePercent", "weightedAvgPrice",
        "prevClosePrice", "lastPrice", "lastQty",
        "bidPrice", "bidQty", "askPrice", "askQty",
        "openPrice", "highPrice", "lowPrice",
        "volume", "quoteVolume",
        "price_change", "price_change_pct", "weighted_avg_price",
        "prev_close_price", "last_price", "last_qty",
        "bid_price", "bid_qty", "ask_price", "ask_qty",
        "open_price", "high_price", "low_price", "quote_volume",
    }),
    "binance.trade.raw": frozenset({
        "price", "qty", "quoteQty", "quote_qty",
        "p", "q",
    }),
}

# Reverse map used by scripts that walk silver/ prefixes back to Kafka topics.
TOPIC_LABEL_TO_KAFKA = {
    "klines": "binance.kline.1m.raw",
    "ticker": "binance.ticker.raw",
    "trades": "binance.trade.raw",
}


def coerce_numeric_fields_to_string(topic, messages):
    """In-place: ép các field numeric thành str để schema Parquet ổn định.

    NaN/Inf floats would stringify to 'nan'/'inf' — Binance API never emits
    these, so we don't special-case them; ETL will reject via isNotNull
    filter if they ever appear.
    """
    fields = NUMERIC_TO_STRING_FIELDS.get(topic)
    if not fields:
        return
    for m in messages:
        for f in fields:
            v = m.get(f)
            if v is not None and not isinstance(v, str):
                m[f] = str(v)
