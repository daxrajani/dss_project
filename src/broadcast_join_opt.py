"""
broadcast_join_opt.py — Broadcast join optimization for attribution.

Demonstrates a concrete Spark tuning technique:
  - BASELINE : standard shuffle join between orders and session_referrers
  - OPTIMIZED: broadcast join (orders is small relative to session_referrers)

Reports:
  - Execution plan (physical plan) for both strategies
  - Wall-clock time before and after
  - Speedup ratio

Usage:
    python src/broadcast_join_opt.py
    python src/broadcast_join_opt.py --input data/data_set/2019-Oct.csv --sample-fraction 0.2
"""

import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast, col, concat_ws, crc32, first, lag,
    min as _min, monotonically_increasing_id,
    regexp_replace, row_number, sum as _sum,
    to_timestamp, unix_timestamp, when,
)
from pyspark.sql.window import Window


SECONDS_30_MIN = 1800
SECONDS_24_HR  = 86400


# ---------------------------------------------------------------------------
# Shared setup helpers
# ---------------------------------------------------------------------------

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


def _derive_sessions(events_df):
    user_win = Window.partitionBy("user_id").orderBy("event_time")
    df = events_df.withColumn("_prev_ts", lag("event_time").over(user_win))
    df = df.withColumn("_gap", unix_timestamp("event_time") - unix_timestamp("_prev_ts"))
    df = df.withColumn(
        "_is_new",
        when(col("_gap") > SECONDS_30_MIN, 1).when(col("_prev_ts").isNull(), 1).otherwise(0),
    )
    df = df.withColumn("_session_idx", _sum("_is_new").over(user_win))
    df = df.withColumn("session_id", concat_ws("_", col("user_id"), col("_session_idx")))
    return df.drop("_prev_ts", "_gap", "_is_new", "_session_idx")


def prepare_inputs(spark, input_path, sample_fraction):
    raw = spark.read.csv(input_path, header=True, inferSchema=False)
    raw = raw.withColumn(
        "event_time",
        to_timestamp(regexp_replace(col("event_time"), r" UTC$", "")),
    ).withColumn("price", col("price").cast("double"))
    raw = raw.sample(withReplacement=False, fraction=sample_fraction, seed=42)
    enriched = _add_referrer(raw).cache()
    enriched.count()

    orders = (
        enriched.filter(col("event_type") == "purchase")
        .select(
            monotonically_increasing_id().alias("order_id"),
            col("user_id"), col("event_time").alias("order_time"),
            col("price").alias("total_amount"),
        )
        .cache()
    )
    orders.count()

    session_refs = (
        _derive_sessions(enriched)
        .groupBy("user_id", "session_id")
        .agg(_min("event_time").alias("session_start"), first("referrer").alias("referrer"))
        .filter(col("referrer") != "direct")
        .cache()
    )
    session_refs.count()

    return orders, session_refs


# ---------------------------------------------------------------------------
# Attribution logic — parameterised on whether to broadcast
# ---------------------------------------------------------------------------

def run_attribution(orders, session_refs, use_broadcast=False):
    if use_broadcast:
        # orders is typically ~2-10% of session_refs — safe to broadcast
        orders_side = broadcast(orders)
    else:
        orders_side = orders

    joined = (
        orders_side.alias("o")
        .join(session_refs.alias("s"), on="user_id", how="left")
        .filter(col("s.session_start") <= col("o.order_time"))
        .filter(
            (unix_timestamp("o.order_time") - unix_timestamp("s.session_start"))
            <= SECONDS_24_HR
        )
    )
    rank_win = Window.partitionBy("o.order_id").orderBy(col("s.session_start").desc())
    result = (
        joined.withColumn("_rank", row_number().over(rank_win))
        .filter(col("_rank") == 1)
        .select("o.order_id", "o.user_id", "o.order_time", "s.referrer")
    )
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Broadcast Join Optimization")
    parser.add_argument("--input", default=None)
    parser.add_argument("--sample-fraction", type=float, default=0.2)
    args = parser.parse_args()

    if args.input is None:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        args.input = os.path.join(base, "data", "data_set", "2019-Oct.csv")

    spark = (
        SparkSession.builder
        .appName("DSS_BroadcastJoin_Opt")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "50")
        # Disable auto-broadcast so baseline is a true shuffle join
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "=" * 60)
    print(" DSS BROADCAST JOIN OPTIMIZATION")
    print("=" * 60)
    print(f" Input           : {args.input}")
    print(f" Sample fraction : {args.sample_fraction}")
    print(f" Auto-broadcast  : DISABLED (fair baseline comparison)")

    orders, session_refs = prepare_inputs(spark, args.input, args.sample_fraction)

    n_orders   = orders.count()
    n_sessions = session_refs.count()
    print(f"\n Orders       : {n_orders:,} rows")
    print(f" Session refs : {n_sessions:,} rows")
    print(f" Size ratio   : orders are {n_orders/n_sessions*100:.1f}% of session_refs")

    # ── Baseline: shuffle join ────────────────────────────────────────────
    print("\n" + "-" * 60)
    print(" BASELINE — Shuffle Join (SortMergeJoin)")
    print("-" * 60)
    result_baseline = run_attribution(orders, session_refs, use_broadcast=False)
    print("\n Physical plan (baseline):")
    result_baseline.explain(mode="formatted")

    t0 = time.time()
    n_baseline = result_baseline.count()
    time_baseline = time.time() - t0
    print(f"\n Result: {n_baseline:,} attributed orders  |  Time: {time_baseline:.1f}s")

    # ── Optimized: broadcast join ─────────────────────────────────────────
    print("\n" + "-" * 60)
    print(" OPTIMIZED — Broadcast Join (orders broadcast to all workers)")
    print("-" * 60)
    result_broadcast = run_attribution(orders, session_refs, use_broadcast=True)
    print("\n Physical plan (broadcast):")
    result_broadcast.explain(mode="formatted")

    t0 = time.time()
    n_broadcast = result_broadcast.count()
    time_broadcast = time.time() - t0
    print(f"\n Result: {n_broadcast:,} attributed orders  |  Time: {time_broadcast:.1f}s")

    # ── Summary ───────────────────────────────────────────────────────────
    speedup = time_baseline / time_broadcast if time_broadcast > 0 else 0
    print("\n" + "=" * 60)
    print(" OPTIMIZATION RESULTS")
    print("=" * 60)
    print(f" {'Strategy':<25} {'Time':>10}  {'Orders':>12}  {'Speedup':>8}")
    print(f" {'-'*25} {'-'*10}  {'-'*12}  {'-'*8}")
    print(f" {'Shuffle join (baseline)':<25} {time_baseline:>9.1f}s  {n_baseline:>12,}  {'1.00x':>8}")
    print(f" {'Broadcast join':<25} {time_broadcast:>9.1f}s  {n_broadcast:>12,}  {speedup:>7.2f}x")
    print("=" * 60)
    print("\n NOTE: On a distributed cluster the broadcast benefit is larger")
    print("       because it eliminates shuffle across the network entirely.")
    print()

    spark.stop()


if __name__ == "__main__":
    main()
