"""
Phân tích Top 10 coin đáng đầu tư - timeframe 1d (v1.4.0 - Full Mode Stabilization)
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config

TIMEFRAME = "1d"
TOP_N     = 10

def run(spark, jdbc_url, jdbc_props):
    import time
    _start = time.time()
    read_props = {**jdbc_props, "fetchsize": "10000"}

    print(f"\n{'='*70}")
    print(f"[top_coins] BẮT ĐẦU RANKING (v1.4.9 - Explicit Schema)")
    print(f"{'='*70}")

    # Bước 1/6: Đọc klines 1d + trade_metrics + dim_symbols từ Postgres
    _t = time.time()
    print(f"[top_coins] Buoc 1/6: Doc klines_1d tu Postgres (JDBC) ...")
    klines_1d = spark.read.jdbc(
        jdbc_url,
        "(SELECT * FROM fact_klines WHERE interval_code = '1d') AS t",
        properties=read_props,
    ).cache()

    if klines_1d.limit(1).count() == 0:
        print(f"[top_coins] Khong co du lieu 1d klines, bo qua.")
        return

    klines_count = klines_1d.count()
    print(f"[top_coins]   -> {klines_count:,} rows klines_1d ({time.time()-_t:.1f}s)")

    _t = time.time()
    print(f"[top_coins]    Doc trade_metrics + dim_symbols ...")
    trade_metrics = (
        spark.read.jdbc(jdbc_url, "mart_trade_metrics", properties=read_props)
        .select("symbol_id", "money_flow", "whale_buy_ratio")
    )
    dim_sym = (
        spark.read.jdbc(jdbc_url, "dim_symbols", properties=read_props)
        .select("symbol_id", "symbol_code")
    )
    print(f"[top_coins]   -> Doc xong ({time.time()-_t:.1f}s)")

    # Bước 2/6: Sanity filter
    _t = time.time()
    print(f"[top_coins] Buoc 2/6: Sanity filter (loc open_time NULL + nam ngoai [2000,2100]) ...")
    klines_1d = klines_1d.filter(
        F.col("open_time").isNotNull() &
        F.year(F.col("open_time")).between(2000, 2100)
    )
    if config.DEBUG_DIAGNOSTIC:
        print(f"[top_coins]   -> after_filter_count = {klines_1d.count()}")
    print(f"[top_coins]   -> Filter xong ({time.time()-_t:.1f}s)")

    # Bước 3/6: Volume trend (window partitioned per symbol)
    _t = time.time()
    print(f"[top_coins] Buoc 3/6: Tinh volume trend (so sanh half-period volume) ...")
    w_row = Window.partitionBy("symbol_id").orderBy("open_time")
    w_sym = Window.partitionBy("symbol_id")

    klines_numbered = (
        klines_1d
        .withColumn("row_num",    F.row_number().over(w_row))
        .withColumn("total_rows", F.count("*").over(w_sym))
        .withColumn(
            "half",
            F.when(F.col("row_num") <= F.col("total_rows") / 2, F.lit("first")).otherwise(F.lit("second"))
        )
    )
    vol_trend_df = (
        klines_numbered.groupBy("symbol_id", "half")
        .agg(F.avg("volume_quote").alias("avg_vol"))
        .groupBy("symbol_id")
        .pivot("half", ["first", "second"])
        .agg(F.first("avg_vol"))
        .withColumn(
            "volume_trend",
            F.when(F.col("first") > 0, F.col("second") / F.col("first")).otherwise(F.lit(1.0))
        )
        .select("symbol_id", F.col("volume_trend").cast("decimal(10,4)"))
    )
    print(f"[top_coins]   -> Volume trend DAG ready ({time.time()-_t:.1f}s)")

    # Bước 4/6: Global aggregates per symbol
    _t = time.time()
    print(f"[top_coins] Buoc 4/6: Global aggregates (volume, trades, bullish ratio) ...")
    klines_agg = (
        klines_1d.groupBy("symbol_id")
        .agg(
            F.sum("volume_quote").alias("total_volume_quote"),
            F.sum("num_trades").alias("total_num_trades"),
            F.avg(F.when(F.col("volume_quote") > 0, F.col("taker_buy_quote_volume") / F.col("volume_quote"))).alias("avg_taker_buy_ratio"),
            F.count("*").alias("candle_count"),
            F.sum(F.when(F.col("close_price") > F.col("open_price"), F.lit(1)).otherwise(F.lit(0))).alias("bullish_candles"),
        )
        .withColumn("bullish_ratio", F.when(F.col("candle_count") > 0, F.col("bullish_candles") / F.col("candle_count")))
    )
    print(f"[top_coins]   -> Aggregates DAG ready ({time.time()-_t:.1f}s)")

    # Bước 5/6: Join + Percent Rank scoring
    _t = time.time()
    print(f"[top_coins] Buoc 5/6: Join all + Percent Rank scoring (5 factors) ...")
    combined = (
        klines_agg
        .join(vol_trend_df,  "symbol_id", "left")
        .join(trade_metrics, "symbol_id", "left")
        .join(dim_sym,       "symbol_id", "inner")
        .withColumn("whale_buy_ratio",       F.coalesce(F.col("whale_buy_ratio"), F.lit(0.5)))
        .withColumn("avg_taker_buy_ratio",   F.coalesce(F.col("avg_taker_buy_ratio"), F.lit(0.5)))
        .withColumn("bullish_ratio",         F.coalesce(F.col("bullish_ratio"), F.lit(0.5)))
        .withColumn("money_flow_filled",     F.coalesce(F.col("money_flow"), F.lit(0.0)))
    )

    def pct_rank(col_name: str) -> F.Column:
        w = Window.orderBy(F.col(col_name).asc())
        return F.percent_rank().over(w)

    scored = (
        combined
        .withColumn("pr_volume",  pct_rank("total_volume_quote"))
        .withColumn("pr_taker",   pct_rank("avg_taker_buy_ratio"))
        .withColumn("pr_flow",    pct_rank("money_flow_filled"))
        .withColumn("pr_whale",   pct_rank("whale_buy_ratio"))
        .withColumn("pr_bullish", pct_rank("bullish_ratio"))
        .withColumn(
            "long_term_score",
            (0.2*F.col("pr_volume") + 0.2*F.col("pr_taker") + 0.2*F.col("pr_flow") + 0.2*F.col("pr_whale") + 0.2*F.col("pr_bullish"))
        )
    )
    print(f"[top_coins]   -> Scoring DAG ready ({time.time()-_t:.1f}s)")

    # Bước 6/6: Final ranking + signal + ghi mart_top_coins
    _t = time.time()
    print(f"[top_coins] Buoc 6/6: Final ranking TOP {TOP_N} + ghi mart_top_coins ...")
    w_final = Window.orderBy(F.col("long_term_score").desc())
    result = (
        scored
        .coalesce(1)
        .withColumn("rank", F.row_number().over(w_final))
        .filter(F.col("rank") <= TOP_N)
        .withColumn("signal",
            F.when((F.col("whale_buy_ratio") > 0.60) & (F.col("bullish_ratio") > 0.60), F.lit("ACCUMULATING"))
            .when((F.col("whale_buy_ratio") < 0.40) | (F.col("bullish_ratio") < 0.35), F.lit("DISTRIBUTING"))
            .otherwise(F.lit("NEUTRAL"))
        )
        .select(
            F.lit(TIMEFRAME).alias("timeframe"), "rank", "symbol_id", "symbol_code",
            F.col("total_volume_quote").cast("decimal(30,12)"),
            F.col("total_num_trades").cast("bigint"),
            F.col("avg_taker_buy_ratio").cast("decimal(10,6)"),
            F.col("bullish_ratio").cast("decimal(10,6)"),
            F.col("volume_trend").cast("decimal(10,4)"),
            F.col("money_flow").cast("decimal(30,12)"),
            F.col("whale_buy_ratio").cast("decimal(10,6)"),
            F.col("long_term_score").cast("decimal(10,4)"),
            "signal", F.current_timestamp().alias("computed_at")
        )
    )

    if not result.isEmpty():
        result.show(TOP_N, truncate=False)
        written = result.count()
        result.write.jdbc(jdbc_url, "mart_top_coins", mode="append", properties={**jdbc_props, "batchsize": "100"})
        print(f"[top_coins]   -> Ghi {written} coins vao mart_top_coins ({time.time()-_t:.1f}s)")
    else:
        print(f"[top_coins]   -> Khong co ket qua de ghi")

    klines_1d.unpersist()
    print(f"[top_coins] HOAN TAT ranking trong {time.time()-_start:.1f}s")
