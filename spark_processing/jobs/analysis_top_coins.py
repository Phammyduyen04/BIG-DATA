"""
Phân tích Top 10 coin đáng đầu tư — timeframe dài hạn (1d)

Nguồn (đọc từ Postgres sau khi ETL hoàn tất):
  - fact_klines          (interval_code = '1d')
  - mart_trade_metrics   (money_flow + whale signals, do etl_trades tính sẵn)
  - dim_symbols

Đích: mart_top_coins (mode=append → tích lũy lịch sử qua từng lần ETL)

─── Score 5 chiều (mỗi chiều 20%) ──────────────────────────────────
  Chiều 1  total_volume_quote   Tổng volume tất cả nến 1d → thanh khoản đều đặn
  Chiều 2  avg_taker_buy_ratio  Tỷ lệ lệnh mua chủ động TB → áp lực mua bền vững
  Chiều 3  money_flow           Dòng tiền ròng từ trades   → tiền thực sự vào/ra
  Chiều 4  whale_buy_ratio      % volume cá mập là mua     → tổ chức đang gom/xả
  Chiều 5  bullish_ratio        % nến xanh (close > open)  → xu hướng tăng

─── Bonus / Penalty ─────────────────────────────────────────────────
  volume_trend   > 1.2   → +0.05  (volume đang tăng dần = tích lũy)
  whale_buy_ratio < 0.40 → -0.10  (cá mập đang xả mạnh = nguy hiểm)

─── Signal ──────────────────────────────────────────────────────────
  ACCUMULATING : whale_buy_ratio > 0.60 AND bullish_ratio > 0.60
  DISTRIBUTING : whale_buy_ratio < 0.40 OR  bullish_ratio < 0.35
  NEUTRAL      : còn lại

─── Null handling ───────────────────────────────────────────────────
  whale_buy_ratio NULL (không có whale trade) → thay bằng 0.5 (neutral)
  volume_trend    NULL (chỉ 1 nến)            → thay bằng 1.0 (stable)
  avg_taker_buy_ratio NULL                    → thay bằng 0.5 (neutral)
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TIMEFRAME = "1d"
TOP_N     = 10


def run(spark, jdbc_url, jdbc_props):
    read_props = {**jdbc_props, "fetchsize": "10000"}

    # ── Đọc dữ liệu từ Postgres (Source of Truth - Giữ nguyên SELECT *) ──
    klines_1d = spark.read.jdbc(
        jdbc_url,
        "(SELECT * FROM fact_klines WHERE interval_code = '1d') AS t",
        properties=read_props,
    )

    trade_metrics = (
        spark.read.jdbc(jdbc_url, "mart_trade_metrics", properties=read_props)
        .select("symbol_id", "money_flow", "whale_buy_ratio")
    )

    dim_sym = (
        spark.read.jdbc(jdbc_url, "dim_symbols", properties=read_props)
        .select("symbol_id", "symbol_code")
    )

    # ── [HARDENING] Audit & Sanity Filter (Chạy ngay sau khi read để bảo vệ Pipeline) ──
    # Thay thế .rdd.isEmpty() bằng cách kiểm tra rỗng an toàn trong JVM
    if klines_1d.limit(1).count() == 0:
        print("[top_coins] Không có dữ liệu 1d klines, bỏ qua.")
        return

    print(f"\n[top_coins] BẮT ĐẦU KIỂM TRA TÍNH HỢP LỆ (v1.1.9)...")
    raw_df = klines_1d.cache()
    raw_count = raw_df.count()
    
    # Bộ lọc bảo vệ: [2000 - 2100]. Tuyệt đối an toàn cho dữ liệu Binance.
    klines_1d = raw_df.filter(
        F.col("open_time").isNotNull() & 
        F.year(F.col("open_time")).between(2000, 2100)
    )
    clean_count = klines_1d.count()
    
    if raw_count != clean_count:
        print(f"[top_coins] WARNING: Đã lọc bỏ {raw_count - clean_count} dòng bị hỏng (Timestamp overflow).")
    else:
        print(f"[top_coins] Dữ liệu timestamp sạch ({clean_count} rows).")

    if clean_count == 0:
        print("[top_coins] Không còn dữ liệu sạch sau sanity filter, bỏ qua.")
        raw_df.unpersist()
        return

    # Giải phóng raw load sau khi đã có klines_1d sạch
    raw_df.unpersist()

    # ── Volume trend: so sánh nửa sau vs nửa đầu ─────────────
    # (Giữ nguyên Business Logic)
    w_row  = Window.partitionBy("symbol_id").orderBy("open_time")
    w_all  = Window.partitionBy("symbol_id")

    klines_numbered = (
        klines_1d
        .withColumn("row_num",    F.row_number().over(w_row))
        .withColumn("total_rows", F.count("*").over(w_all))
        .withColumn(
            "half",
            F.when(F.col("row_num") <= F.col("total_rows") / 2, F.lit("first"))
            .otherwise(F.lit("second"))
        )
    )

    vol_by_half = klines_numbered.groupBy("symbol_id", "half").agg(
        F.avg("volume_quote").alias("avg_vol")
    )

    # Pivot → mỗi symbol có 1 hàng với cột "first" và "second"
    vol_trend_df = (
        vol_by_half
        .groupBy("symbol_id")
        .pivot("half", ["first", "second"])
        .agg(F.first("avg_vol"))
        .withColumn(
            "volume_trend",
            F.when(
                F.col("first").isNotNull() & (F.col("first") > 0),
                F.col("second") / F.col("first")
            ).otherwise(F.lit(1.0))
        )
        .select("symbol_id", F.col("volume_trend").cast("decimal(10,4)").alias("volume_trend"))
    )

    # ── Aggregate chính trên 1d klines ───────────────────────
    # (Giữ nguyên Scoring Formulas)
    klines_agg = (
        klines_1d.groupBy("symbol_id")
        .agg(
            F.sum("volume_quote").alias("total_volume_quote"),
            F.sum("num_trades").alias("total_num_trades"),
            F.avg(
                F.when(
                    F.col("volume_quote") > 0,
                    F.col("taker_buy_quote_volume") / F.col("volume_quote")
                )
            ).alias("avg_taker_buy_ratio"),
            F.count("*").alias("candle_count"),
            F.sum(
                F.when(F.col("close_price") > F.col("open_price"), F.lit(1))
                .otherwise(F.lit(0))
            ).alias("bullish_candles"),
        )
        .withColumn(
            "bullish_ratio",
            F.when(
                F.col("candle_count") > 0,
                F.col("bullish_candles") / F.col("candle_count")
            ).otherwise(F.lit(None).cast("decimal(10,6)"))
        )
    )

    # ── Join tất cả nguồn ─────────────────────────────────────
    combined = (
        klines_agg
        .join(vol_trend_df,   "symbol_id", "left")
        .join(trade_metrics,  "symbol_id", "left")
        .join(dim_sym,        "symbol_id", "inner")
    )

    # ── Null handling trước khi rank ──────────────────────────
    combined = (
        combined
        .withColumn(
            "whale_buy_ratio_filled",
            F.coalesce(F.col("whale_buy_ratio"), F.lit(0.5))
        )
        .withColumn(
            "avg_taker_buy_ratio_filled",
            F.coalesce(F.col("avg_taker_buy_ratio"), F.lit(0.5))
        )
        .withColumn(
            "bullish_ratio_filled",
            F.coalesce(F.col("bullish_ratio"), F.lit(0.5))
        )
        .withColumn(
            "money_flow_filled",
            F.coalesce(F.col("money_flow"), F.lit(0.0))
        )
    )

    # ── Percent-rank mỗi chiều ──────────────────────────────
    def pct_rank(col_name: str) -> F.Column:
        w = Window.orderBy(F.col(col_name).asc())
        return F.percent_rank().over(w)

    combined = (
        combined
        .withColumn("pr_volume",  pct_rank("total_volume_quote"))
        .withColumn("pr_taker",   pct_rank("avg_taker_buy_ratio_filled"))
        .withColumn("pr_flow",    pct_rank("money_flow_filled"))
        .withColumn("pr_whale",   pct_rank("whale_buy_ratio_filled"))
        .withColumn("pr_bullish", pct_rank("bullish_ratio_filled"))
    )

    # ── Base score = trung bình cộng 5 chiều ─────────────────
    combined = combined.withColumn(
        "base_score",
        (
            0.20 * F.col("pr_volume")  +
            0.20 * F.col("pr_taker")   +
            0.20 * F.col("pr_flow")    +
            0.20 * F.col("pr_whale")   +
            0.20 * F.col("pr_bullish")
        )
    )

    # ── Bonus / Penalty ───────────────────────────────────────
    combined = (
        combined
        .withColumn(
            "long_term_score",
            F.when(
                F.col("volume_trend").isNotNull() & (F.col("volume_trend") > 1.2),
                F.col("base_score") + 0.05
            ).otherwise(F.col("base_score"))
        )
        .withColumn(
            "long_term_score",
            F.when(
                F.col("whale_buy_ratio").isNotNull() & (F.col("whale_buy_ratio") < 0.40),
                F.col("long_term_score") - 0.10
            ).otherwise(F.col("long_term_score"))
        )
        .withColumn(
            "long_term_score",
            F.greatest(F.lit(0.0), F.least(F.lit(1.0), F.col("long_term_score")))
        )
    )

    # ── Signal (Giữ nguyên Signal Rules) ──────────────────────
    combined = combined.withColumn(
        "signal",
        F.when(
            (F.col("whale_buy_ratio") > 0.60) & (F.col("bullish_ratio") > 0.60),
            F.lit("ACCUMULATING")
        ).when(
            (F.col("whale_buy_ratio") < 0.40) | (F.col("bullish_ratio") < 0.35),
            F.lit("DISTRIBUTING")
        ).otherwise(F.lit("NEUTRAL"))
    )

    # ── Rank và Chọn đầu ra (Giữ nguyên Semantics) ───────────
    w_final = Window.orderBy(F.col("long_term_score").desc())
    result = (
        combined
        .withColumn("rank", F.row_number().over(w_final))
        .filter(F.col("rank") <= TOP_N)
        .select(
            F.lit(TIMEFRAME).alias("timeframe"),
            F.col("rank"),
            F.col("symbol_id"),
            F.col("symbol_code"),
            F.col("total_volume_quote").cast("decimal(30,12)"),
            F.col("total_num_trades").cast("bigint"),
            F.col("avg_taker_buy_ratio").cast("decimal(10,6)"),
            F.col("bullish_ratio").cast("decimal(10,6)"),
            F.col("volume_trend").cast("decimal(10,4)"),
            F.col("money_flow").cast("decimal(30,12)"),
            F.col("whale_buy_ratio").cast("decimal(10,6)"),
            F.col("long_term_score").cast("decimal(10,4)"),
            F.col("signal"),
            F.current_timestamp().alias("computed_at"),
        )
    )

    # In kết quả ra log để dễ kiểm tra
    print(f"\n{'='*70}")
    print(f"TOP {TOP_N} COIN DÀI HẠN (timeframe={TIMEFRAME})")
    print(f"{'='*70}")
    result.show(TOP_N, truncate=False)

    # Append vào mart_top_coins
    result.write.jdbc(
        jdbc_url, "mart_top_coins",
        mode="append",
        properties={**jdbc_props, "batchsize": "100"},
    )

    n = result.count()
    print(f"[top_coins] Ghi {n} coin vào mart_top_coins (timeframe={TIMEFRAME})")
    print(
        "[top_coins] Query kết quả mới nhất:\n"
        "  SELECT * FROM mart_top_coins\n"
        "  WHERE timeframe='1d'\n"
        "    AND computed_at = (SELECT MAX(computed_at) FROM mart_top_coins WHERE timeframe='1d')\n"
        "  ORDER BY rank;"
    )
