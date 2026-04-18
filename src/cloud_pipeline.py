"""
DSS Cloud Pipeline — self-contained PySpark script for Dataproc.

Inlines all 5 pipeline stages:
  1. ETL Enrichment       — device/referrer synthesis, orders, catalog
  2. Sessionization       — 30-min inactivity window
  3. Funnel Analysis      — view→cart→purchase by (category, device, referrer, day/week)
  4. Last-Touch Attribution — 24-hr lookback, non-direct referrer
  5. Anomaly Detection    — 7-day rolling z-score, SPIKE/DROP explanations

Usage:
    spark-submit cloud_pipeline.py \
        --input  gs://YOUR_BUCKET/data/2019-Oct.csv \
        --output gs://YOUR_BUCKET/results/run1 \
        [--sample-fraction 0.2]

Sample fractions:
    ~1 GB  → 0.2
    ~5 GB  → 1.0  (full dataset, no sampling)
    ~10 GB → 2.0  (triggers union-doubling: reads dataset twice with prefixed IDs)
"""

import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as _abs, avg, col, concat, concat_ws, count,
    crc32, first, lag, lit, max as _max, min as _min,
    monotonically_increasing_id, regexp_replace,
    round as _round, row_number, stddev,
    sum as _sum, to_date, to_timestamp, trunc,
    unix_timestamp, when,
)
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SECONDS_30_MIN = 1800
SECONDS_24_HR  = 86400
SECONDS_7D     = 7 * 86400
SECONDS_1D     = 1 * 86400
Z_THRESHOLD    = 2.0


# ---------------------------------------------------------------------------
# Stage 1 — ETL Enrichment
# ---------------------------------------------------------------------------

def _add_device(df):
    bucket = crc32(col("user_session")) % 100
    return df.withColumn(
        "device",
        when(bucket < 55, "mobile")
        .when(bucket < 85, "desktop")
        .otherwise("tablet"),
    )


def _add_referrer(df):
    bucket = crc32(col("user_session")) % 100
    return df.withColumn(
        "referrer",
        when(bucket < 40, "direct")
        .when(bucket < 65, "google")
        .when(bucket < 80, "facebook")
        .when(bucket < 88, "instagram")
        .when(bucket < 95, "email")
        .otherwise("affiliate"),
    )


def run_etl(spark, input_path, sample_fraction):
    pipeline_start = time.time()
    timings = {}

    t0 = time.time()
    print("\n>>> [ETL 1/5] Reading raw events ...")
    raw = spark.read.csv(input_path, header=True, inferSchema=False)
    raw = raw.withColumn(
        "event_time",
        to_timestamp(regexp_replace(col("event_time"), r" UTC$", "")),
    ).withColumn("price", col("price").cast("double"))

    if sample_fraction < 1.0:
        print(f"    Sampling {sample_fraction*100:.0f}% of rows ...")
        raw = raw.sample(withReplacement=False, fraction=sample_fraction, seed=42)
    elif sample_fraction > 1.0:
        print("    Doubling dataset (union trick for ~10 GB variant) ...")
        copy2 = (
            raw
            .withColumn("user_id",      concat(lit("r2_"), col("user_id")))
            .withColumn("user_session", concat(lit("r2_"), col("user_session")))
        )
        raw = raw.union(copy2)

    row_count = raw.count()
    timings["read_rows"] = time.time() - t0
    print(f"    Loaded {row_count:,} rows  ({timings['read_rows']:.1f}s)")

    t0 = time.time()
    print("\n>>> [ETL 2/5] Synthesising device & referrer columns ...")
    enriched = _add_device(_add_referrer(raw))
    timings["enrich"] = time.time() - t0
    print(f"    Done  ({timings['enrich']:.1f}s)")

    t0 = time.time()
    print("\n>>> [ETL 3/5] Deriving orders from purchase events ...")
    orders = (
        enriched
        .filter(col("event_type") == "purchase")
        .select(
            monotonically_increasing_id().alias("order_id"),
            col("user_id"),
            col("event_time").alias("order_time"),
            col("price").alias("total_amount"),
            col("user_session"),
        )
    )
    order_count = orders.count()
    timings["orders"] = time.time() - t0
    print(f"    {order_count:,} orders  ({timings['orders']:.1f}s)")

    t0 = time.time()
    print("\n>>> [ETL 4/5] Deriving product catalog ...")
    catalog = (
        enriched
        .select("product_id", "category_code", "brand", "price")
        .dropDuplicates(["product_id"])
        .fillna({"category_code": "unknown", "brand": "unknown"})
    )
    catalog_count = catalog.count()
    timings["catalog"] = time.time() - t0
    print(f"    {catalog_count:,} products  ({timings['catalog']:.1f}s)")

    timings["etl_total"] = time.time() - pipeline_start
    print(f"\n    ETL total: {timings['etl_total']:.1f}s")

    return enriched, orders, catalog, row_count, order_count, catalog_count, timings


# ---------------------------------------------------------------------------
# Stage 2 — Sessionization (shared helper used by funnel + attribution)
# ---------------------------------------------------------------------------

def _derive_sessions(events_df):
    """Assign session_id via 30-min inactivity window per user."""
    user_win = Window.partitionBy("user_id").orderBy("event_time")
    df = events_df.withColumn("_prev_ts", lag("event_time").over(user_win))
    df = df.withColumn(
        "_gap",
        unix_timestamp("event_time") - unix_timestamp("_prev_ts"),
    )
    df = df.withColumn(
        "_is_new",
        when(col("_gap") > SECONDS_30_MIN, 1)
        .when(col("_prev_ts").isNull(), 1)
        .otherwise(0),
    )
    df = df.withColumn("_session_idx", _sum("_is_new").over(user_win))
    df = df.withColumn(
        "session_id",
        concat_ws("_", col("user_id"), col("_session_idx")),
    )
    return df.drop("_prev_ts", "_gap", "_is_new", "_session_idx")


def run_sessionization(enriched_df):
    timings = {}
    t0 = time.time()
    print("\n>>> [SESSION] Deriving sessions (30-min inactivity window) ...")
    sessionized = _derive_sessions(enriched_df).cache()
    session_count = sessionized.count()
    timings["sessionization"] = time.time() - t0
    print(f"    {session_count:,} events with session_id  ({timings['sessionization']:.1f}s)")
    return sessionized, session_count, timings


# ---------------------------------------------------------------------------
# Stage 3 — Funnel Analysis
# ---------------------------------------------------------------------------

def _compute_funnel_rates(session_flags_df, grain_col):
    """Aggregate session-level funnel flags to conversion rates for a time grain."""
    return (
        session_flags_df
        .groupBy(grain_col, "category_code", "device", "referrer")
        .agg(
            _sum("has_view").alias("total_sessions_with_view"),
            _sum("has_cart").alias("total_sessions_with_cart"),
            _sum("has_purchase").alias("total_sessions_with_purchase"),
        )
        .withColumn(
            "view_to_cart_pct",
            when(col("total_sessions_with_view") == 0, 0.0).otherwise(
                _round(col("total_sessions_with_cart") /
                       col("total_sessions_with_view") * 100, 2)
            ),
        )
        .withColumn(
            "cart_to_buy_pct",
            when(col("total_sessions_with_cart") == 0, 0.0).otherwise(
                _round(col("total_sessions_with_purchase") /
                       col("total_sessions_with_cart") * 100, 2)
            ),
        )
        .filter(col("category_code") != "unknown")
        .orderBy(grain_col, ascending=False)
    )


def run_funnel_analysis(sessionized_df):
    timings = {}

    t0 = time.time()
    print("\n>>> [FUNNEL 1/2] Building session-level funnel flags ...")
    # Tag each event type and add time grains
    df = (
        sessionized_df
        .withColumn("date", to_date("event_time"))
        .withColumn("week", trunc(col("event_time"), "week"))
        .withColumn("is_view",     when(col("event_type") == "view",     1).otherwise(0))
        .withColumn("is_cart",     when(col("event_type") == "cart",     1).otherwise(0))
        .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0))
    )

    # Session-level flags (one row per session × date × category × device × referrer)
    session_flags = (
        df.groupBy("session_id", "date", "week", "category_code", "device", "referrer")
        .agg(
            _max("is_view").alias("has_view"),
            _max("is_cart").alias("has_cart"),
            _max("is_purchase").alias("has_purchase"),
        )
        .cache()
    )
    n_sessions = session_flags.count()
    timings["funnel_flags"] = time.time() - t0
    print(f"    {n_sessions:,} session-grain rows  ({timings['funnel_flags']:.1f}s)")

    t0 = time.time()
    print("\n>>> [FUNNEL 2/2] Computing daily + weekly conversion rates ...")
    daily  = _compute_funnel_rates(session_flags, "date")
    weekly = _compute_funnel_rates(session_flags, "week")
    n_daily  = daily.count()
    n_weekly = weekly.count()
    timings["funnel_rates"] = time.time() - t0
    print(f"    {n_daily:,} daily rows  |  {n_weekly:,} weekly rows  ({timings['funnel_rates']:.1f}s)")

    return daily, weekly, n_daily, n_weekly, timings


# ---------------------------------------------------------------------------
# Stage 4 — Last-Touch Attribution
# ---------------------------------------------------------------------------

def build_session_referrers(events_df):
    sessionized = _derive_sessions(events_df)
    return (
        sessionized
        .groupBy("user_id", "session_id")
        .agg(
            _min("event_time").alias("session_start"),
            first("referrer").alias("referrer"),
            first("device").alias("device"),
        )
    )


def attribute_orders(orders_df, session_referrers_df):
    candidates = session_referrers_df.filter(col("referrer") != "direct")
    joined = (
        orders_df.alias("o")
        .join(candidates.alias("s"), on="user_id", how="left")
        .filter(col("s.session_start") <= col("o.order_time"))
        .filter(
            (unix_timestamp("o.order_time") - unix_timestamp("s.session_start"))
            <= SECONDS_24_HR
        )
    )
    rank_win = Window.partitionBy("o.order_id").orderBy(col("s.session_start").desc())
    ranked = joined.withColumn("_rank", row_number().over(rank_win))
    return (
        ranked.filter(col("_rank") == 1)
        .select(
            col("o.order_id"),
            col("o.user_id"),
            col("o.order_time"),
            col("o.total_amount").alias("order_amount"),
            col("s.referrer").alias("attributed_referrer"),
            col("s.session_start").alias("attribution_session_start"),
            col("s.device").alias("attribution_device"),
        )
    )


def run_attribution(enriched, orders, order_count):
    timings = {}

    t0 = time.time()
    print("\n>>> [ATTR 1/2] Building session_referrers (30-min inactivity window) ...")
    session_refs = build_session_referrers(enriched)
    session_count = session_refs.count()
    non_direct    = session_refs.filter(col("referrer") != "direct").count()
    timings["session_refs"] = time.time() - t0
    print(f"    {session_count:,} sessions  ({non_direct:,} non-direct)  ({timings['session_refs']:.1f}s)")

    t0 = time.time()
    print("\n>>> [ATTR 2/2] Running last-touch attribution (24-hr lookback) ...")
    attributed = attribute_orders(orders, session_refs)
    attributed_count = attributed.count()
    timings["attribution"] = time.time() - t0
    print(f"    {attributed_count:,} attributed  ({order_count - attributed_count:,} unattributed)  ({timings['attribution']:.1f}s)")

    return attributed, attributed_count, timings


# ---------------------------------------------------------------------------
# Stage 5 — Anomaly Detection
# ---------------------------------------------------------------------------

def _apply_rolling_zscore(df, count_col, partition_cols=None):
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

    pct_above = _round((col(count_col) - col("rolling_avg")) / col("rolling_avg") * 100, 1)
    pct_below = _round(_abs(col(count_col) - col("rolling_avg")) / col("rolling_avg") * 100, 1)
    expected  = _round(col("rolling_avg"), 0).cast("long")

    df = df.withColumn(
        "explanation",
        when(col("is_anomaly") & (col("z_score") > 0),
             concat(lit("SPIKE: "), pct_above.cast("string"),
                    lit("% above 7-day baseline ("), col(count_col).cast("string"),
                    lit(" vs expected "), expected.cast("string"), lit(")")))
        .when(col("is_anomaly") & (col("z_score") < 0),
              concat(lit("DROP: "), pct_below.cast("string"),
                     lit("% below 7-day baseline ("), col(count_col).cast("string"),
                     lit(" vs expected "), expected.cast("string"), lit(")")))
        .otherwise(None),
    )
    return df.drop("date_ts")


def run_anomaly_detection(enriched):
    timings = {}

    purchases = enriched.filter(col("event_type") == "purchase").cache()
    purchase_count = purchases.count()

    t0 = time.time()
    print("\n>>> [ANOM 1/2] Computing system-level daily anomalies ...")
    daily = (
        purchases
        .withColumn("date", to_date("event_time"))
        .groupBy("date")
        .agg(count("*").alias("purchase_count"))
        .orderBy("date")
    )
    daily_anomalies = _apply_rolling_zscore(daily, "purchase_count").cache()
    n_days    = daily_anomalies.count()
    n_flagged = daily_anomalies.filter("is_anomaly").count()
    timings["daily_anomalies"] = time.time() - t0
    print(f"    {n_days} days, {n_flagged} anomalies flagged  ({timings['daily_anomalies']:.1f}s)")

    t0 = time.time()
    print("\n>>> [ANOM 2/2] Computing per-category daily anomalies ...")
    cat_daily = (
        purchases
        .withColumn("date", to_date("event_time"))
        .fillna({"category_code": "unknown"})
        .groupBy("date", "category_code")
        .agg(count("*").alias("purchase_count"))
        .orderBy("date", "category_code")
    )
    cat_anomalies = _apply_rolling_zscore(
        cat_daily, "purchase_count", partition_cols=["category_code"]
    ).cache()
    n_cat_rows    = cat_anomalies.count()
    n_cat_flagged = cat_anomalies.filter("is_anomaly").count()
    timings["cat_anomalies"] = time.time() - t0
    print(f"    {n_cat_rows} (date,category) rows, {n_cat_flagged} anomalies  ({timings['cat_anomalies']:.1f}s)")

    return daily_anomalies, cat_anomalies, n_days, n_flagged, n_cat_rows, n_cat_flagged, timings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="DSS Cloud Pipeline")
    parser.add_argument("--input",  required=True,
                        help="Path to raw events CSV (gs:// or local)")
    parser.add_argument("--output", required=True,
                        help="Output directory for Parquet results (gs:// or local)")
    parser.add_argument("--sample-fraction", type=float, default=1.0,
                        help="0.2≈1GB | 1.0=5GB (full) | 2.0=10GB (union-doubled)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("DSS_Cloud_Pipeline")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    pipeline_start = time.time()

    print("\n" + "=" * 60)
    print(" DSS CLOUD PIPELINE  —  5 Stages")
    print("=" * 60)
    print(f" Input           : {args.input}")
    print(f" Output          : {args.output}")
    print(f" Sample fraction : {args.sample_fraction}")
    print("=" * 60)

    out = args.output.rstrip("/")

    # ── Stage 1: ETL ─────────────────────────────────────────────────────
    enriched, orders, catalog, row_count, order_count, catalog_count, etl_timings = \
        run_etl(spark, args.input, args.sample_fraction)

    t0 = time.time()
    print("\n>>> [WRITE] Saving ETL outputs ...")
    enriched.write.mode("overwrite").parquet(f"{out}/enriched_events")
    orders.write.mode("overwrite").parquet(f"{out}/orders")
    catalog.write.mode("overwrite").parquet(f"{out}/catalog")
    write_etl_time = time.time() - t0
    print(f"    ETL Parquet written  ({write_etl_time:.1f}s)")

    # Re-read from Parquet — downstream stages benefit from columnar pruning
    enriched_pq = spark.read.parquet(f"{out}/enriched_events").cache()
    orders_pq   = spark.read.parquet(f"{out}/orders").cache()

    # ── Stage 2 + 3: Sessionization + Funnel Analysis ─────────────────────
    sessionized, session_event_count, sess_timings = run_sessionization(enriched_pq)

    daily_funnel, weekly_funnel, n_daily_funnel, n_weekly_funnel, funnel_timings = \
        run_funnel_analysis(sessionized)

    t0 = time.time()
    print("\n>>> [WRITE] Saving funnel outputs ...")
    daily_funnel.write.mode("overwrite").parquet(f"{out}/funnel_metrics_daily")
    weekly_funnel.write.mode("overwrite").parquet(f"{out}/funnel_metrics_weekly")
    write_funnel_time = time.time() - t0
    print(f"    Funnel Parquet written  ({write_funnel_time:.1f}s)")

    # ── Stage 4: Attribution ──────────────────────────────────────────────
    attributed, attributed_count, attr_timings = \
        run_attribution(enriched_pq, orders_pq, order_count)

    t0 = time.time()
    print("\n>>> [WRITE] Saving attribution outputs ...")
    attributed.write.mode("overwrite").parquet(f"{out}/attributed_orders")
    write_attr_time = time.time() - t0
    print(f"    Attribution Parquet written  ({write_attr_time:.1f}s)")

    # ── Stage 5: Anomaly Detection ────────────────────────────────────────
    daily_anom, cat_anom, n_days, n_flagged, n_cat_rows, n_cat_flagged, anom_timings = \
        run_anomaly_detection(enriched_pq)

    t0 = time.time()
    print("\n>>> [WRITE] Saving anomaly detection outputs ...")
    daily_anom.write.mode("overwrite").parquet(f"{out}/daily_anomalies")
    cat_anom.write.mode("overwrite").parquet(f"{out}/category_anomalies")
    write_anom_time = time.time() - t0
    print(f"    Anomaly Parquet written  ({write_anom_time:.1f}s)")

    # ── Final summary ─────────────────────────────────────────────────────
    total_time = time.time() - pipeline_start

    funnel_total  = funnel_timings["funnel_flags"] + funnel_timings["funnel_rates"]
    attr_total    = attr_timings["session_refs"] + attr_timings["attribution"]
    anom_total    = anom_timings["daily_anomalies"] + anom_timings["cat_anomalies"]

    print("\n" + "=" * 60)
    print(" PIPELINE SUMMARY")
    print("=" * 60)
    print(f" {'Metric':<38} {'Value':>12}")
    print(f" {'-'*38} {'-'*12}")
    print(f" {'Input rows':<38} {row_count:>12,}")
    print(f" {'Orders derived':<38} {order_count:>12,}")
    print(f" {'Catalog products':<38} {catalog_count:>12,}")
    print(f" {'Sessions (events)':<38} {session_event_count:>12,}")
    print(f" {'Funnel daily rows':<38} {n_daily_funnel:>12,}")
    print(f" {'Funnel weekly rows':<38} {n_weekly_funnel:>12,}")
    print(f" {'Orders attributed':<38} {attributed_count:>12,}")
    print(f" {'Days analysed':<38} {n_days:>12}")
    print(f" {'System anomalies flagged':<38} {n_flagged:>12}")
    print(f" {'Category (date,cat) rows':<38} {n_cat_rows:>12,}")
    print(f" {'Category anomalies flagged':<38} {n_cat_flagged:>12}")
    print(f" {'-'*38} {'-'*12}")
    print(f" {'Stage 1  ETL compute':<38} {etl_timings['etl_total']:>11.1f}s")
    print(f" {'  read + sample/double':<38} {etl_timings['read_rows']:>11.1f}s")
    print(f" {'  enrich (device+referrer)':<38} {etl_timings['enrich']:>11.1f}s")
    print(f" {'  orders derivation':<38} {etl_timings['orders']:>11.1f}s")
    print(f" {'  catalog derivation':<38} {etl_timings['catalog']:>11.1f}s")
    print(f" {'  ETL Parquet write':<38} {write_etl_time:>11.1f}s")
    print(f" {'Stage 2  Sessionization':<38} {sess_timings['sessionization']:>11.1f}s")
    print(f" {'Stage 3  Funnel Analysis':<38} {funnel_total:>11.1f}s")
    print(f" {'  session-level flags':<38} {funnel_timings['funnel_flags']:>11.1f}s")
    print(f" {'  daily + weekly rates':<38} {funnel_timings['funnel_rates']:>11.1f}s")
    print(f" {'  funnel Parquet write':<38} {write_funnel_time:>11.1f}s")
    print(f" {'Stage 4  Attribution':<38} {attr_total:>11.1f}s")
    print(f" {'  session_referrers build':<38} {attr_timings['session_refs']:>11.1f}s")
    print(f" {'  last-touch join':<38} {attr_timings['attribution']:>11.1f}s")
    print(f" {'  attribution Parquet write':<38} {write_attr_time:>11.1f}s")
    print(f" {'Stage 5  Anomaly Detection':<38} {anom_total:>11.1f}s")
    print(f" {'  daily anomalies':<38} {anom_timings['daily_anomalies']:>11.1f}s")
    print(f" {'  category anomalies':<38} {anom_timings['cat_anomalies']:>11.1f}s")
    print(f" {'  anomaly Parquet write':<38} {write_anom_time:>11.1f}s")
    print(f" {'-'*38} {'-'*12}")
    print(f" {'TOTAL PIPELINE TIME':<38} {total_time:>11.1f}s")
    print("=" * 60)
    print(f" Output path: {out}")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
