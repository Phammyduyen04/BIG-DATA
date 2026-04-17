"""
ETL: dim_symbols
Nguồn: tên thư mục trong klines/ (BTCUSDT, ETHUSDT, ...)
Logic: UPSERT qua staging table
"""
import os
from pyspark.sql.types import StructType, StructField, StringType

from etl_utils import execute_sql

# Thứ tự ưu tiên khi tách base/quote asset từ symbol code
_QUOTE_ASSETS = ["USDT", "BUSD", "FDUSD", "BTC", "ETH", "BNB"]


def _parse_assets(symbol_code: str):
    for q in _QUOTE_ASSETS:
        if symbol_code.endswith(q):
            return symbol_code[: -len(q)], q
    return symbol_code, ""


def run(spark, jdbc_url, jdbc_props, data_base_path):
    klines_base = os.path.join(data_base_path, "klines")

    symbols = []
    for name in sorted(os.listdir(klines_base)):
        if os.path.isdir(os.path.join(klines_base, name)):
            base, quote = _parse_assets(name)
            symbols.append((name, base, quote))

    schema = StructType([
        StructField("symbol_code", StringType()),
        StructField("base_asset",  StringType()),
        StructField("quote_asset", StringType()),
    ])
    df = spark.createDataFrame(symbols, schema=schema)

    # Ghi staging (overwrite an toàn vì bảng tạm)
    df.write.jdbc(jdbc_url, "staging_dim_symbols", mode="overwrite", properties=jdbc_props)

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

    print(f"[dim_symbols] {len(symbols)} symbols: {[s[0] for s in symbols]}")
