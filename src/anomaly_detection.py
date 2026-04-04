"""
Anomaly Detection — Phase 4 of the DSS e-commerce pipeline.

Computes daily purchase counts (system-wide and per-category), builds a
7-day trailing rolling baseline (mean + stddev), and flags days where the
z-score exceeds ±2.  Flagged rows receive a human-readable SPIKE/DROP label.

Usage:
    python src/anomaly_detection.py
    python src/anomaly_detection.py --input gs://bucket/processed --output gs://bucket/processed/anomalies
"""

import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as _abs, avg, col, concat, count,
    lit, round as _round,
    stddev, to_date, unix_timestamp, when,
)
from pyspark.sql.window import Window


SECONDS_7D = 7 * 86400   # 604 800 — left bound of rolling window
SECONDS_1D = 1 * 86400   #  86 400 — right bound (exclude today from baseline)
Z_THRESHOLD = 2.0


# ---------------------------------------------------------------------------
# Shared rolling-window logic
# ---------------------------------------------------------------------------

def _apply_rolling_zscore(df, count_col, partition_cols=None):
    """
    Given a DataFrame with a numeric count_col and a 'date' column, computes:
      rolling_avg, rolling_stddev, z_score, is_anomaly, explanation

    partition_cols: list of column names to partition the window by (per-category)
                    or None for system-wide.
    """
    df = df.withColumn("date_ts", unix_timestamp(to_date(col("date"))))

    if partition_cols:
        rolling_win = (
            Window.partitionBy(*partition_cols)
            .orderBy("date_ts")
            .rangeBetween(-SECONDS_7D, -SECONDS_1D)
        )
    else:
        rolling_win = (
            Window.orderBy("date_ts")
            .rangeBetween(-SECONDS_7D, -SECONDS_1D)
        )

    df = df.withColumn("rolling_avg",    _round(avg(count_col).over(rolling_win),    2))
    df = df.withColumn("rolling_stddev", _round(stddev(count_col).over(rolling_win), 2))

    # z_score is null when stddev is 0 or null (flat baseline — can't distinguish noise)
    df = df.withColumn(
        "z_score",
        when(
            col("rolling_stddev").isNotNull() & (col("rolling_stddev") > 0),
            _round((col(count_col) - col("rolling_avg")) / col("rolling_stddev"), 3),
        ).otherwise(None),
    )

    df = df.withColumn(
        "is_anomaly",
        when(col("z_score").isNotNull() & (_abs(col("z_score")) > Z_THRESHOLD), True)
        .otherwise(False),
    )

    # Human-readable explanation: "SPIKE: 45.2% above 7-day baseline (1200 vs expected 826)"
    pct_above = _round(
        (col(count_col) - col("rolling_avg")) / col("rolling_avg") * 100, 1
    )
    pct_below = _round(
        _abs(col(count_col) - col("rolling_avg")) / col("rolling_avg") * 100, 1
    )
    expected = _round(col("rolling_avg"), 0).cast("long")
    df = df.withColumn(
        "explanation",
        when(col("is_anomaly") & (col("z_score") > 0),
             concat(
                 lit("SPIKE: "), pct_above.cast("string"),
                 lit("% above 7-day baseline ("),
                 col(count_col).cast("string"),
                 lit(" vs expected "), expected.cast("string"), lit(")"),
             ))
        .when(col("is_anomaly") & (col("z_score") < 0),
              concat(
                  lit("DROP: "), pct_below.cast("string"),
                  lit("% below 7-day baseline ("),
                  col(count_col).cast("string"),
                  lit(" vs expected "), expected.cast("string"), lit(")"),
              ))
        .otherwise(None),
    )

    return df.drop("date_ts")


# ---------------------------------------------------------------------------
# System-level daily anomalies
# ---------------------------------------------------------------------------

def compute_daily_anomalies(purchases_df):
    """
    Aggregate to (date, purchase_count) across all categories, then apply
    7-day rolling z-score.
    """
    daily = (
        purchases_df
        .withColumn("date", to_date("event_time"))
        .groupBy("date")
        .agg(count("*").alias("purchase_count"))
        .orderBy("date")
    )
    return _apply_rolling_zscore(daily, "purchase_count", partition_cols=None)


# ---------------------------------------------------------------------------
# Per-category daily anomalies
# ---------------------------------------------------------------------------

def compute_category_anomalies(purchases_df):
    """
    Aggregate to (date, category_code, purchase_count), then apply a
    per-category 7-day rolling z-score.
    """
    cat_daily = (
        purchases_df
        .withColumn("date", to_date("event_time"))
        .fillna({"category_code": "unknown"})
        .groupBy("date", "category_code")
        .agg(count("*").alias("purchase_count"))
        .orderBy("date", "category_code")
    )
    return _apply_rolling_zscore(cat_daily, "purchase_count", partition_cols=["category_code"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(input_dir: str, output_dir: str, spark: SparkSession):
    total_start = time.time()

    events_path = os.path.join(input_dir, "enriched_events")

    # ------------------------------------------------------------------
    # Step 1 — Read and filter purchase events
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [1/5] Reading enriched_events and isolating purchases ...")
    events = spark.read.parquet(events_path)
    purchases = events.filter(col("event_type") == "purchase").cache()
    purchase_count = purchases.count()
    print(f"    {purchase_count:,} purchase events  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Step 2 — System-level daily anomalies
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [2/5] Computing system-level daily anomalies ...")
    daily_anomalies = compute_daily_anomalies(purchases).cache()
    n_days    = daily_anomalies.count()
    n_flagged = daily_anomalies.filter("is_anomaly").count()
    print(f"    {n_days} days processed, {n_flagged} anomalies flagged  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Step 3 — Per-category daily anomalies
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [3/5] Computing per-category daily anomalies ...")
    cat_anomalies = compute_category_anomalies(purchases).cache()
    n_cat_rows    = cat_anomalies.count()
    n_cat_flagged = cat_anomalies.filter("is_anomaly").count()
    print(f"    {n_cat_rows} (date, category) rows, {n_cat_flagged} anomalies flagged  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Step 4 — Console previews
    # ------------------------------------------------------------------
    flagged_daily = daily_anomalies.filter("is_anomaly")
    flagged_cat   = cat_anomalies.filter("is_anomaly")

    print("\n--- System-Level Anomalies ---")
    if n_flagged > 0:
        flagged_daily.select(
            "date", "purchase_count", "rolling_avg", "rolling_stddev", "z_score", "explanation"
        ).show(20, truncate=False)
    else:
        print("    No system-level anomalies detected (need ≥8 days of data for non-null z-scores).")

    print("\n--- Per-Category Anomalies ---")
    if n_cat_flagged > 0:
        flagged_cat.select(
            "date", "category_code", "purchase_count", "rolling_avg", "rolling_stddev", "z_score", "explanation"
        ).show(20, truncate=False)
    else:
        print("    No per-category anomalies detected.")

    # ------------------------------------------------------------------
    # Step 5 — Write Parquet
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [5/5] Writing Parquet outputs ...")

    daily_out = os.path.join(output_dir, "daily_anomalies")
    cat_out   = os.path.join(output_dir, "category_anomalies")

    daily_anomalies.write.mode("overwrite").parquet(daily_out)
    print(f"    daily_anomalies     → {daily_out}")

    cat_anomalies.write.mode("overwrite").parquet(cat_out)
    print(f"    category_anomalies  → {cat_out}")

    print(f"    Write complete  ({time.time() - t0:.1f}s)")

    total = time.time() - total_start
    print("\n=============================================")
    print(f" ANOMALY DETECTION COMPLETE  —  {total:.1f}s total")
    print(f"   Days analysed         : {n_days}")
    print(f"   System anomalies      : {n_flagged}")
    print(f"   Category rows         : {n_cat_rows}")
    print(f"   Category anomalies    : {n_cat_flagged}")
    print(f"   Z-score threshold     : ±{Z_THRESHOLD}")
    print("=============================================\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _default_input():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "data", "processed")


def _default_output():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "data", "processed", "anomalies")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DSS Anomaly Detection")
    parser.add_argument(
        "--input",
        default=_default_input(),
        help="Directory containing enriched_events/ Parquet (local or gs://)",
    )
    parser.add_argument(
        "--output",
        default=_default_output(),
        help="Output directory for anomaly Parquet files (local or gs://)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("DSS_Anomaly_Detection")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("\n=============================================")
    print(" DSS ANOMALY DETECTION PIPELINE")
    print("=============================================")
    print(f" Input  : {args.input}")
    print(f" Output : {args.output}")

    run(args.input, args.output, spark)

    spark.stop()
