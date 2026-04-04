"""
Last-Touch Attribution — Phase 3 of the DSS e-commerce pipeline.

For each order, finds the most recent non-direct referrer session that started
within 24 hours before the order time (last-touch model).

Session derivation matches ecommerce_pipeline.py Phase A exactly:
  - 30-minute inactivity threshold per user_id
  - session_id = user_id + "_" + cumulative_session_index

Usage:
    python src/attribution.py
    python src/attribution.py --input gs://bucket/processed --output gs://bucket/processed/attribution
"""

import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, unix_timestamp, lag, when,
    sum as _sum, min as _min, first,
    concat_ws, row_number, count,
    round as _round,
)
from pyspark.sql.window import Window


SECONDS_30_MIN = 1800
SECONDS_24_HR  = 86400


# ---------------------------------------------------------------------------
# Step helpers
# ---------------------------------------------------------------------------

def _derive_sessions(events_df):
    """
    Replicate Phase-A sessionization on enriched_events.
    Produces a 'session_id' column (user_id_N) via 30-min inactivity window.
    """
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


def build_session_referrers(events_df):
    """
    One row per (user_id, session_id) capturing:
      session_start      — earliest event_time in the session
      referrer           — referrer for this session (deterministic per user_session)
      device             — device for this session

    Because device/referrer are synthesised from crc32(user_session) in ETL,
    every event in a user_session shares the same values — first() is correct.
    """
    sessionized = _derive_sessions(events_df)

    session_win = Window.partitionBy("session_id").orderBy("event_time")

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
    """
    Last-touch attribution:
      1. Filter out direct sessions.
      2. Cross-join orders with candidate sessions on user_id.
      3. Keep only sessions that started within 24 hours before the order.
      4. Pick the most recent such session via row_number() desc.
    """
    candidates = session_referrers_df.filter(col("referrer") != "direct")

    joined = (
        orders_df.alias("o")
        .join(candidates.alias("s"), on="user_id", how="left")
        .filter(
            # session must have started before the order
            col("s.session_start") <= col("o.order_time")
        )
        .filter(
            # and within 24 hours before the order
            (unix_timestamp("o.order_time") - unix_timestamp("s.session_start"))
            <= SECONDS_24_HR
        )
    )

    rank_win = Window.partitionBy("o.order_id").orderBy(
        col("s.session_start").desc()
    )

    ranked = joined.withColumn("_rank", row_number().over(rank_win))

    return (
        ranked
        .filter(col("_rank") == 1)
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


def build_attribution_summary(attributed_orders_df):
    """
    Aggregated revenue breakdown per attributed referrer channel.
    """
    return (
        attributed_orders_df
        .groupBy("attributed_referrer")
        .agg(
            count("order_id").alias("order_count"),
            _round(_sum_col("order_amount"), 2).alias("total_revenue"),
            _round(
                _sum_col("order_amount") / count("order_id"), 2
            ).alias("avg_order_value"),
        )
        .orderBy("total_revenue", ascending=False)
    )


def _sum_col(name):
    from pyspark.sql.functions import sum as _s
    return _s(name)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(input_dir: str, output_dir: str, spark: SparkSession):
    total_start = time.time()

    events_path  = os.path.join(input_dir, "enriched_events")
    orders_path  = os.path.join(input_dir, "orders")

    # ------------------------------------------------------------------
    # Step 1 — Read inputs
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [1/5] Reading enriched_events ...")
    events = spark.read.parquet(events_path)
    event_count = events.count()
    print(f"    {event_count:,} events  ({time.time() - t0:.1f}s)")

    t0 = time.time()
    print("\n>>> [2/5] Reading orders ...")
    orders = spark.read.parquet(orders_path).cache()
    order_count = orders.count()
    print(f"    {order_count:,} orders  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Step 2 — Build session referrers
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [3/5] Building session_referrers (30-min inactivity window) ...")
    session_refs = build_session_referrers(events)
    session_count = session_refs.count()
    non_direct    = session_refs.filter(col("referrer") != "direct").count()
    print(f"    {session_count:,} sessions  ({non_direct:,} non-direct)  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Step 3 — Last-touch attribution
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [4/5] Running last-touch attribution (24-hr lookback) ...")
    attributed = attribute_orders(orders, session_refs)
    attributed_count = attributed.count()
    unattributed = order_count - attributed_count
    print(f"    {attributed_count:,} orders attributed  ({unattributed:,} unattributed / direct-only)  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Step 4 — Attribution summary
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [5/5] Building attribution summary ...")
    summary = build_attribution_summary(attributed)
    print(f"    Done  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Step 5 — Write outputs
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> Writing Parquet outputs ...")

    attributed_out = os.path.join(output_dir, "attributed_orders")
    summary_out    = os.path.join(output_dir, "attribution_summary")

    attributed.write.mode("overwrite").parquet(attributed_out)
    print(f"    attributed_orders    → {attributed_out}")

    summary.write.mode("overwrite").parquet(summary_out)
    print(f"    attribution_summary  → {summary_out}")

    print(f"    Write complete  ({time.time() - t0:.1f}s)")

    # Preview
    print("\n--- Attribution Summary Preview ---")
    summary.show(truncate=False)

    total = time.time() - total_start
    print("=============================================")
    print(f" ATTRIBUTION COMPLETE  —  {total:.1f}s total")
    print(f"   Orders attributed : {attributed_count:,} / {order_count:,}")
    print(f"   Attribution rate  : {attributed_count / order_count * 100:.1f}%")
    print("=============================================\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _default_input():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "data", "processed")


def _default_output():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "data", "processed", "attribution")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DSS Last-Touch Attribution")
    parser.add_argument(
        "--input",
        default=_default_input(),
        help="Directory containing enriched_events/ and orders/ Parquet (local or gs://)",
    )
    parser.add_argument(
        "--output",
        default=_default_output(),
        help="Output directory for attribution Parquet (local or gs://)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("DSS_Attribution")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("\n=============================================")
    print(" DSS LAST-TOUCH ATTRIBUTION PIPELINE")
    print("=============================================")
    print(f" Input  : {args.input}")
    print(f" Output : {args.output}")

    run(args.input, args.output, spark)

    spark.stop()
