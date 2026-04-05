"""
skew_analysis.py — Data skew detection and mitigation for the DSS pipeline.

Demonstrates a real Spark performance problem (hot-key skew in attribution)
and measures the before/after impact of salting.

What this does:
  1. Detects top skewed users/categories by event count (hot keys)
  2. Measures attribution join time WITHOUT salting (default shuffle)
  3. Applies salting to the user_id join key (N buckets)
  4. Measures attribution join time WITH salting
  5. Reports partition size distribution and speedup

Usage:
    python src/skew_analysis.py
    python src/skew_analysis.py --input data/data_set/2019-Oct.csv --salt-buckets 20
"""

import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat, concat_ws, count, crc32, first, floor,
    lag, lit, min as _min, monotonically_increasing_id,
    rand, regexp_replace, round as _round, row_number,
    sum as _sum, to_timestamp, unix_timestamp, when,
)
from pyspark.sql.window import Window


SECONDS_30_MIN = 1800
SECONDS_24_HR  = 86400


# ---------------------------------------------------------------------------
# Helpers (inline — no local imports)
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


def load_and_enrich(spark, input_path, sample_fraction=0.2):
    raw = spark.read.csv(input_path, header=True, inferSchema=False)
    raw = raw.withColumn(
        "event_time",
        to_timestamp(regexp_replace(col("event_time"), r" UTC$", "")),
    ).withColumn("price", col("price").cast("double"))
    raw = raw.sample(withReplacement=False, fraction=sample_fraction, seed=42)
    return _add_referrer(raw)


# ---------------------------------------------------------------------------
# Step 1 — Skew detection
# ---------------------------------------------------------------------------

def detect_skew(enriched_df, top_n=20):
    print("\n>>> [SKEW 1] Detecting hot keys — event count per user ...")
    user_counts = (
        enriched_df
        .groupBy("user_id")
        .agg(count("*").alias("event_count"))
        .orderBy("event_count", ascending=False)
    )

    top_users = user_counts.limit(top_n).collect()

    total_events = enriched_df.count()
    top1_count   = top_users[0]["event_count"] if top_users else 0
    top10_count  = sum(r["event_count"] for r in top_users[:10])

    print(f"\n    Total events : {total_events:,}")
    print(f"    Top-1 user   : {top1_count:,} events ({top1_count/total_events*100:.2f}% of all events)")
    print(f"    Top-10 users : {top10_count:,} events ({top10_count/total_events*100:.2f}% of all events)")
    print(f"\n    Top {top_n} users by event count:")
    print(f"    {'Rank':<6} {'user_id':<20} {'events':>10}")
    print(f"    {'-'*6} {'-'*20} {'-'*10}")
    for i, row in enumerate(top_users, 1):
        print(f"    {i:<6} {str(row['user_id']):<20} {row['event_count']:>10,}")

    print("\n>>> [SKEW 2] Detecting hot keys — event count per category ...")
    cat_counts = (
        enriched_df
        .fillna({"category_code": "unknown"})
        .groupBy("category_code")
        .agg(count("*").alias("event_count"))
        .orderBy("event_count", ascending=False)
        .limit(10)
        .collect()
    )
    print(f"\n    Top 10 categories:")
    print(f"    {'category_code':<40} {'events':>10} {'share':>8}")
    print(f"    {'-'*40} {'-'*10} {'-'*8}")
    for row in cat_counts:
        share = row["event_count"] / total_events * 100
        print(f"    {str(row['category_code']):<40} {row['event_count']:>10,} {share:>7.2f}%")

    return user_counts


# ---------------------------------------------------------------------------
# Step 2 — Attribution WITHOUT salting (baseline)
# ---------------------------------------------------------------------------

def attribution_no_salt(enriched_df):
    print("\n>>> [SKEW 3] Attribution WITHOUT salting (baseline) ...")
    t0 = time.time()

    orders = (
        enriched_df
        .filter(col("event_type") == "purchase")
        .select(
            monotonically_increasing_id().alias("order_id"),
            col("user_id"), col("event_time").alias("order_time"),
            col("price").alias("total_amount"),
        )
    )

    sessionized = _derive_sessions(enriched_df)
    session_refs = (
        sessionized
        .groupBy("user_id", "session_id")
        .agg(_min("event_time").alias("session_start"), first("referrer").alias("referrer"))
        .filter(col("referrer") != "direct")
    )

    joined = (
        orders.alias("o")
        .join(session_refs.alias("s"), on="user_id", how="left")
        .filter(col("s.session_start") <= col("o.order_time"))
        .filter((unix_timestamp("o.order_time") - unix_timestamp("s.session_start")) <= SECONDS_24_HR)
    )

    rank_win = Window.partitionBy("o.order_id").orderBy(col("s.session_start").desc())
    result = (
        joined.withColumn("_rank", row_number().over(rank_win))
        .filter(col("_rank") == 1)
        .select("o.order_id", "o.user_id", "o.order_time", "s.referrer")
    )

    n = result.count()
    elapsed = time.time() - t0
    print(f"    {n:,} orders attributed  (no-salt time: {elapsed:.1f}s)")
    return elapsed, n


# ---------------------------------------------------------------------------
# Step 3 — Attribution WITH salting
# ---------------------------------------------------------------------------

def attribution_with_salt(enriched_df, n_buckets=20):
    print(f"\n>>> [SKEW 4] Attribution WITH salting ({n_buckets} buckets) ...")
    t0 = time.time()

    orders = (
        enriched_df
        .filter(col("event_type") == "purchase")
        .select(
            monotonically_increasing_id().alias("order_id"),
            col("user_id"), col("event_time").alias("order_time"),
            col("price").alias("total_amount"),
        )
        # Explode orders across all salt buckets (replicate)
        .crossJoin(
            spark_ref[0].range(n_buckets).select(col("id").cast("int").alias("_salt"))
        )
        .withColumn("salted_user_id", concat(col("user_id"), lit("_"), col("_salt")))
    )

    sessionized = _derive_sessions(enriched_df)
    session_refs = (
        sessionized
        .groupBy("user_id", "session_id")
        .agg(_min("event_time").alias("session_start"), first("referrer").alias("referrer"))
        .filter(col("referrer") != "direct")
        # Assign a random salt bucket to each session (distribute)
        .withColumn("_salt", (rand() * n_buckets).cast("int"))
        .withColumn("salted_user_id", concat(col("user_id"), lit("_"), col("_salt")))
    )

    joined = (
        orders.alias("o")
        .join(session_refs.alias("s"), on="salted_user_id", how="left")
        .filter(col("s.session_start") <= col("o.order_time"))
        .filter((unix_timestamp("o.order_time") - unix_timestamp("s.session_start")) <= SECONDS_24_HR)
    )

    rank_win = Window.partitionBy("o.order_id").orderBy(col("s.session_start").desc())
    result = (
        joined.withColumn("_rank", row_number().over(rank_win))
        .filter(col("_rank") == 1)
        .select("o.order_id", "o.user_id", "o.order_time", "s.referrer")
    )

    n = result.count()
    elapsed = time.time() - t0
    print(f"    {n:,} orders attributed  (salted time: {elapsed:.1f}s)")
    return elapsed, n


# Spark session reference for crossJoin in salting
spark_ref = [None]


# ---------------------------------------------------------------------------
# Step 4 — Partition size distribution
# ---------------------------------------------------------------------------

def partition_size_report(df, label):
    print(f"\n>>> [SKEW 5] Partition size distribution — {label}")
    sizes = (
        df.withColumn("_pid", col("spark_partition_id()"))
        .groupBy("_pid")
        .count()
        .orderBy("count", ascending=False)
    ) if False else None  # placeholder — use RDD glom for accurate counts

    # More accurate: use RDD
    part_sizes = df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    part_sizes.sort(reverse=True)
    if part_sizes:
        print(f"    Partitions   : {len(part_sizes)}")
        print(f"    Max size     : {max(part_sizes):,} rows")
        print(f"    Min size     : {min(part_sizes):,} rows")
        print(f"    Avg size     : {sum(part_sizes)//len(part_sizes):,} rows")
        print(f"    Skew ratio   : {max(part_sizes)/max(sum(part_sizes)//len(part_sizes),1):.2f}x  (max/avg)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="DSS Skew Analysis")
    parser.add_argument("--input", default=None, help="Raw events CSV path")
    parser.add_argument("--sample-fraction", type=float, default=0.2)
    parser.add_argument("--salt-buckets", type=int, default=20)
    args = parser.parse_args()

    if args.input is None:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        args.input = os.path.join(base, "data", "data_set", "2019-Oct.csv")

    spark = (
        SparkSession.builder
        .appName("DSS_Skew_Analysis")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "50")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark_ref[0] = spark

    print("\n" + "=" * 60)
    print(" DSS SKEW ANALYSIS")
    print("=" * 60)
    print(f" Input           : {args.input}")
    print(f" Sample fraction : {args.sample_fraction}")
    print(f" Salt buckets    : {args.salt_buckets}")

    enriched = load_and_enrich(spark, args.input, args.sample_fraction).cache()
    enriched.count()  # materialise cache

    # 1. Detect skew
    detect_skew(enriched)

    # 2. Partition distribution before salting
    partition_size_report(enriched.repartition(50, "user_id"), "user_id partitioned (no salt)")

    # 3. Attribution without salting
    time_no_salt, n_no_salt = attribution_no_salt(enriched)

    # 4. Attribution with salting
    time_salted, n_salted = attribution_with_salt(enriched, args.salt_buckets)

    # 5. Summary
    speedup = time_no_salt / time_salted if time_salted > 0 else 0
    print("\n" + "=" * 60)
    print(" SKEW MITIGATION SUMMARY")
    print("=" * 60)
    print(f" {'Without salting':<30} {time_no_salt:>8.1f}s  →  {n_no_salt:,} orders")
    print(f" {'With salting':<30} {time_salted:>8.1f}s  →  {n_salted:,} orders")
    print(f" {'Speedup':<30} {speedup:>8.2f}x")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
