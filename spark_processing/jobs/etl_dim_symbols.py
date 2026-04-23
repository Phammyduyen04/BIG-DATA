"""
ETL: dim_symbols
Nguồn: tên thư mục trong klines/ (BTCUSDT, ETHUSDT, ...)
Logic: UPSERT qua staging table
"""
import os
import time
from pyspark.sql.types import StructType, StructField, StringType

from etl_utils import execute_sql

# Thứ tự ưu tiên khi tách base/quote asset từ symbol code
_QUOTE_ASSETS = ["USDT", "BUSD", "FDUSD", "BTC", "ETH", "BNB"]


def _parse_assets(symbol_code: str):
    for q in _QUOTE_ASSETS:
        if symbol_code.endswith(q):
            return symbol_code[: -len(q)], q

def run(spark, jdbc_url, jdbc_props, data_base_path):
    from etl_utils import discover_symbols
    import config

    _start = time.time()
    print(f"\n{'='*70}")
    print(f"[dim_symbols] BẮT ĐẦU SYMBOL DISCOVERY (v1.3.0 - Pipeline Sync)")
    print(f"[dim_symbols] Discovery Source: silver/klines")
    print(f"{'='*70}")

    klines_base = f"{data_base_path.rstrip('/')}/{config.PREFIX_KLINES.strip('/')}"

    # Bước 1/4: Quét S3 tìm danh sách symbol
    _t = time.time()
    print(f"[dim_symbols] Buoc 1/4: Quet S3 layout {klines_base} ...")
    symbol_codes = discover_symbols(spark, klines_base, pattern="interval=*/date=*/symbol=*")
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
