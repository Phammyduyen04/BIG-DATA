"""
ETL: dim_symbols
Nguồn: ticker_24h CSV (1 row/symbol = 100 rows) — nguồn symbol rẻ nhất
Logic: UPSERT qua staging table
"""
import os
import time
from pyspark.sql.types import StructType, StructField, StringType

from etl_utils import execute_sql, load_csv_df, CSV_SCHEMA_TICKER

# Thứ tự ưu tiên khi tách base/quote asset từ symbol code
_QUOTE_ASSETS = ["USDT", "BUSD", "FDUSD", "BTC", "ETH", "BNB"]


def _parse_assets(symbol_code: str):
    for q in _QUOTE_ASSETS:
        if symbol_code.endswith(q):
            return symbol_code[: -len(q)], q
    return symbol_code, ""

def run(spark, jdbc_url, jdbc_props, data_base_path):
    import config

    _start = time.time()
    print(f"\n{'='*70}")
    print(f"[dim_symbols] BAT DAU SYMBOL DISCOVERY (v1.5.0 - CSV Benchmark)")
    print(f"[dim_symbols] Discovery Source: ticker_24h CSV")
    print(f"{'='*70}")

    ticker_glob = (f"{data_base_path.rstrip('/')}"
                   f"/{config.PREFIX_CSV_RAW.strip('/')}"
                   f"/{config.CSV_FILENAME_TICKER}")

    # Bước 1/4: Đọc ticker CSV để lấy danh sách symbol (100 rows)
    _t = time.time()
    print(f"[dim_symbols] Buoc 1/4: Doc ticker CSV {ticker_glob} ...")
    ticker_df = load_csv_df(spark, ticker_glob, CSV_SCHEMA_TICKER)
    if ticker_df is None:
        raise RuntimeError(f"[dim_symbols] Ticker CSV khong ton tai tai: {ticker_glob}")
    symbol_rows = ticker_df.select("symbol").distinct().collect()
    symbol_codes = sorted([r.symbol for r in symbol_rows if r.symbol])
    print(f"[dim_symbols]   -> Phat hien {len(symbol_codes)} symbols ({time.time()-_t:.1f}s)")

    # Bước 2/4: Parse base/quote asset
    _t = time.time()
    print(f"[dim_symbols] Buoc 2/4: Parse base/quote asset cho {len(symbol_codes)} symbols ...")
    symbols = []
    for name in symbol_codes:
        base, quote = _parse_assets(name)
        symbols.append((name, base, quote))
    print(f"[dim_symbols]   -> Parse xong ({time.time()-_t:.1f}s): {[s[0] for s in symbols[:5]]}{'...' if len(symbols) > 5 else ''}")

    schema = StructType([
        StructField("symbol_code", StringType()),
        StructField("base_asset",  StringType()),
        StructField("quote_asset", StringType()),
    ])
    df = spark.createDataFrame(symbols, schema=schema)

    # Bước 3/4: Ghi staging + UPSERT vào dim_symbols
    _t = time.time()
    print(f"[dim_symbols] Buoc 3/4: Ghi {len(symbols)} rows vao staging_dim_symbols ...")
    df.write.jdbc(jdbc_url, "staging_dim_symbols", mode="overwrite", properties=jdbc_props)
    print(f"[dim_symbols]   -> Ghi staging xong ({time.time()-_t:.1f}s)")

    _t = time.time()
    print(f"[dim_symbols] Buoc 4/4: UPSERT vao dim_symbols + don dep staging ...")
    execute_sql(spark, jdbc_url, jdbc_props, """
        INSERT INTO dim_symbols (symbol_code, base_asset, quote_asset, created_at, updated_at)
        SELECT symbol_code, base_asset, quote_asset, NOW(), NOW()
        FROM   staging_dim_symbols
        ON CONFLICT (symbol_code) DO UPDATE SET
            base_asset  = EXCLUDED.base_asset,
            quote_asset = EXCLUDED.quote_asset,
            updated_at  = NOW()
    """)
    execute_sql(spark, jdbc_url, jdbc_props, "DROP TABLE IF EXISTS staging_dim_symbols")
    print(f"[dim_symbols]   -> UPSERT + don dep xong ({time.time()-_t:.1f}s)")

    print(f"[dim_symbols] HOAN TAT: {len(symbols)} symbols trong {time.time()-_start:.1f}s")
