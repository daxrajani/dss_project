"""
cloud_broadcast_join.py — Cloud version of broadcast join optimization.

Runs on GCP Dataproc. Compares shuffle join vs broadcast join for attribution.
Results written to GCS output path.

Usage (via gcloud):
    gcloud dataproc jobs submit pyspark gs://dss-project-dax/scripts/cloud_broadcast_join.py \
        --cluster=<CLUSTER> --region=us-central1 \
        -- --input gs://dss-project-dax/data/full/2019-Oct.csv \
           --output gs://dss-project-dax/results/broadcast_join \
           --sample-fraction 0.3
"""

import argparse
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

    return orders, session_refs, enriched


def run_attribution(orders, session_refs, use_broadcast=False):
    orders_side = broadcast(orders) if use_broadcast else orders

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
    return (
        joined.withColumn("_rank", row_number().over(rank_win))
        .filter(col("_rank") == 1)
        .select("o.order_id", "o.user_id", "o.order_time", "s.referrer")
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default="gs://dss-project-dax/data/full/2019-Oct.csv")
    parser.add_argument("--output", default="gs://dss-project-dax/results/broadcast_join")
    parser.add_argument("--sample-fraction", type=float, default=0.3)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("DSS_BroadcastJoin_Cloud")
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # disable auto for fair baseline
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "=" * 60)
    print(" DSS BROADCAST JOIN OPTIMIZATION  (Cloud)")
    print("=" * 60)
    print(f" Input           : {args.input}")
    print(f" Output          : {args.output}")
    print(f" Sample fraction : {args.sample_fraction}")
    print(f" Auto-broadcast  : DISABLED (fair baseline)")

    orders, session_refs, enriched = prepare_inputs(spark, args.input, args.sample_fraction)

    n_orders   = orders.count()
    n_sessions = session_refs.count()
    print(f"\n Orders       : {n_orders:,}")
    print(f" Session refs : {n_sessions:,}")
    print(f" Size ratio   : orders are {n_orders/n_sessions*100:.1f}% of session_refs")

    # ── Baseline: shuffle join ─────────────────────────────────────────────
    print("\n--- BASELINE: Shuffle Join ---")
    result_baseline = run_attribution(orders, session_refs, use_broadcast=False)
    result_baseline.explain(mode="formatted")
    t0 = time.time()
    n_baseline = result_baseline.count()
    time_baseline = time.time() - t0
    print(f" Result: {n_baseline:,} attributed orders  |  Time: {time_baseline:.1f}s")

    # ── Optimized: broadcast join ──────────────────────────────────────────
    print("\n--- OPTIMIZED: Broadcast Join ---")
    result_broadcast = run_attribution(orders, session_refs, use_broadcast=True)
    result_broadcast.explain(mode="formatted")
    t0 = time.time()
    n_broadcast = result_broadcast.count()
    time_broadcast = time.time() - t0
    print(f" Result: {n_broadcast:,} attributed orders  |  Time: {time_broadcast:.1f}s")

    # ── Summary ────────────────────────────────────────────────────────────
    speedup = time_baseline / time_broadcast if time_broadcast > 0 else 0
    print("\n" + "=" * 60)
    print(" OPTIMIZATION RESULTS")
    print("=" * 60)
    print(f" {'Strategy':<28} {'Time':>10}  {'Orders':>12}  {'Speedup':>8}")
    print(f" {'-'*28} {'-'*10}  {'-'*12}  {'-'*8}")
    print(f" {'Shuffle join (baseline)':<28} {time_baseline:>9.1f}s  {n_baseline:>12,}  {'1.00x':>8}")
    print(f" {'Broadcast join':<28} {time_broadcast:>9.1f}s  {n_broadcast:>12,}  {speedup:>7.2f}x")
    print("=" * 60)
    print(" NOTE: On a distributed cluster broadcast eliminates network shuffle entirely.")

    # ── Write summary to GCS ───────────────────────────────────────────────
    import csv, io
    summary_rows = [
        ["strategy", "time_s", "orders", "speedup"],
        ["shuffle_join_baseline", f"{time_baseline:.2f}", str(n_baseline), "1.00"],
        ["broadcast_join",        f"{time_broadcast:.2f}", str(n_broadcast), f"{speedup:.2f}"],
    ]
    result_broadcast.write.mode("overwrite").parquet(args.output + "/attributed_orders")

    # Write CSV summary via spark
    summary_data = [
        ("shuffle_join_baseline", time_baseline, n_baseline, 1.0),
        ("broadcast_join",        time_broadcast, n_broadcast, speedup),
    ]
    spark.createDataFrame(
        summary_data, ["strategy", "time_s", "orders", "speedup"]
    ).coalesce(1).write.mode("overwrite").option("header", True).csv(args.output + "/summary")

    print(f"\n Results written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
