"""
Orchestrator: chạy toàn bộ ETL + Analytics theo thứ tự

  1. dim_symbols          (phải chạy trước để có symbol_id)
  2. fact_klines (1m)     (cache df_1m để resample dùng lại)
  3. fact_klines resample (5m/15m/30m/1h/4h/1d từ df_1m cached)
  4. mart_trade_metrics   (money_flow + whale signals per symbol; MinIO giữ raw)
  5. fact_ticker_24h_snapshots
  6. analysis_top_coins   (top 10 dài hạn → mart_top_coins)

Chạy qua spark-submit:
  spark-submit --packages org.postgresql:postgresql:42.7.3 \\
               --py-files etl_utils.py,etl_dim_symbols.py,... \\
               run_all.py
"""
import os
import sys
import time

from pyspark.sql import SparkSession

# Khi chạy local (không qua --py-files), thêm thư mục hiện tại vào path
sys.path.insert(0, os.path.dirname(__file__))

import etl_dim_symbols
import etl_klines
import etl_klines_resample
import etl_trades
import etl_ticker_24h
import analysis_top_coins


def main():
    spark = (
        SparkSession.builder
        .appName("CryptoDW-ETL")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = os.environ.get("JDBC_URL",      "jdbc:postgresql://localhost:5432/crypto_dw")
    jdbc_props = {
        "user":     os.environ.get("JDBC_USER",     "dwuser"),
        "password": os.environ.get("JDBC_PASSWORD", "dwpassword"),
        "driver":   "org.postgresql.Driver",
        # Phase B: Aggressive Tuning - Boost write performance
        "batchsize": "10000",
        "reWriteBatchedInserts": "true"
    }
    data_base_path = os.environ.get("DATA_BASE_PATH", "/opt/test_data")

    print("=" * 60)
    print(f"JDBC  : {jdbc_url}")
    print(f"Data  : {data_base_path}")
    print("=" * 60)

    _pipeline_start = time.time()

    def _run_stage(label, fn, *args, **kwargs):
        print(f"\n>>> {label} — BẮT ĐẦU")
        t0 = time.time()
        result = fn(*args, **kwargs)
        elapsed = time.time() - t0
        print(f">>> {label} — HOÀN TẤT trong {elapsed:.1f}s")
        return result

    df_1m = _run_stage("[1/6] dim_symbols",
                       etl_dim_symbols.run, spark, jdbc_url, jdbc_props, data_base_path)

    df_1m = _run_stage("[2/6] fact_klines (1m)",
                       etl_klines.run, spark, jdbc_url, jdbc_props, data_base_path)

    _run_stage("[3/6] fact_klines resample (5m/15m/30m/1h/4h/1d)",
               etl_klines_resample.run, spark, jdbc_url, jdbc_props, df_1m=df_1m)
    if df_1m:
        df_1m.unpersist()

    _run_stage("[4/6] mart_trade_metrics (money flow + whale signals)",
               etl_trades.run, spark, jdbc_url, jdbc_props, data_base_path)

    _run_stage("[5/6] fact_ticker_24h_snapshots",
               etl_ticker_24h.run, spark, jdbc_url, jdbc_props, data_base_path)

    _run_stage("[6/6] analysis_top_coins (top 10 dài hạn, timeframe=1d)",
               analysis_top_coins.run, spark, jdbc_url, jdbc_props)

    total = time.time() - _pipeline_start
    print("\n" + "=" * 60)
    print(f"ETL + Analytics hoàn tất. Tổng thời gian: {total:.1f}s ({total/60:.1f} phút)")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
