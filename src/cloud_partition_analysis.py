"""
cloud_partition_analysis.py — Cloud version of Parquet partitioning analysis.

Compares flat vs date-partitioned Parquet for purchase event data.
Runs on GCP Dataproc, reads/writes to GCS.

Usage (via gcloud):
    gcloud dataproc jobs submit pyspark gs://YOUR_BUCKET/scripts/cloud_partition_analysis.py \
        --cluster=<CLUSTER> --region=us-central1 \
        -- --input  gs://YOUR_BUCKET/data/2019-Oct.csv \
           --output gs://YOUR_BUCKET/results/partition_analysis \
           --sample-fraction 0.15
"""

import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, to_timestamp


def main():
    parser = argparse.ArgumentParser()
    _bucket = os.environ.get("GCS_BUCKET", "gs://dss-project-dax")
    parser.add_argument("--input",  default=os.environ.get("GCS_INPUT",  f"{_bucket}/data/2019-Oct.csv"))
    parser.add_argument("--output", default=os.environ.get("GCS_OUTPUT", f"{_bucket}/results/partition_analysis"))
    parser.add_argument("--sample-fraction", type=float, default=0.15)
    args = parser.parse_args()

    out_flat = args.output + "/flat"
    out_part = args.output + "/partitioned"

    spark = (
        SparkSession.builder
        .appName("DSS_Partition_Analysis_Cloud")
        .config("spark.sql.shuffle.partitions", "50")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "=" * 60)
    print(" DSS PARQUET PARTITIONING ANALYSIS  (Cloud)")
    print("=" * 60)
    print(f" Input           : {args.input}")
    print(f" Output          : {args.output}")
    print(f" Sample fraction : {args.sample_fraction}")

    # ── Load purchase events ───────────────────────────────────────────────
    raw = (
        spark.read.csv(args.input, header=True, inferSchema=False)
        .withColumn("event_time", to_timestamp(
            regexp_replace(col("event_time"), r" UTC$", "")))
        .withColumn("price", col("price").cast("double"))
        .sample(withReplacement=False, fraction=args.sample_fraction, seed=42)
        .filter(col("event_type") == "purchase")
        .withColumn("date", to_date("event_time"))
        .fillna({"category_code": "unknown"})
        .select("date", "category_code", "user_id", "price")
    )
    raw.cache()
    n_rows = raw.count()
    print(f"\n Purchase events loaded : {n_rows:,}")

    query_day   = "2019-10-15"
    query_start = "2019-10-10"
    query_end   = "2019-10-17"

    # ── Write FLAT Parquet ─────────────────────────────────────────────────
    print("\n>>> Writing flat (unpartitioned) Parquet to GCS ...")
    t0 = time.time()
    raw.write.mode("overwrite").parquet(out_flat)
    write_flat = time.time() - t0
    print(f"    Written in {write_flat:.1f}s")

    # ── Write DATE-PARTITIONED Parquet ─────────────────────────────────────
    print("\n>>> Writing date-partitioned Parquet to GCS ...")
    t0 = time.time()
    raw.write.mode("overwrite").partitionBy("date").parquet(out_part)
    write_part = time.time() - t0
    print(f"    Written in {write_part:.1f}s")

    # ── Read back for query comparison ────────────────────────────────────
    df_flat = spark.read.parquet(out_flat)
    df_part = spark.read.parquet(out_part)

    print(f"\n--- QUERY 1: Single-day filter ({query_day}) ---")
    t0 = time.time()
    n_flat1 = df_flat.filter(col("date") == query_day).count()
    q1_flat = time.time() - t0

    t0 = time.time()
    n_part1 = df_part.filter(col("date") == query_day).count()
    q1_part = time.time() - t0

    speedup1 = q1_flat / q1_part if q1_part > 0 else 0
    print(f"  Flat        : {q1_flat:.2f}s  →  {n_flat1:,} rows")
    print(f"  Partitioned : {q1_part:.2f}s  →  {n_part1:,} rows")
    print(f"  Speedup     : {speedup1:.2f}x")

    print(f"\n--- QUERY 2: 7-day range filter ({query_start} to {query_end}) ---")
    t0 = time.time()
    n_flat2 = df_flat.filter(
        (col("date") >= query_start) & (col("date") <= query_end)
    ).count()
    q2_flat = time.time() - t0

    t0 = time.time()
    n_part2 = df_part.filter(
        (col("date") >= query_start) & (col("date") <= query_end)
    ).count()
    q2_part = time.time() - t0

    speedup2 = q2_flat / q2_part if q2_part > 0 else 0
    print(f"  Flat        : {q2_flat:.2f}s  →  {n_flat2:,} rows")
    print(f"  Partitioned : {q2_part:.2f}s  →  {n_part2:,} rows")
    print(f"  Speedup     : {speedup2:.2f}x")

    # ── Summary ────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(" PARTITIONING SUMMARY")
    print("=" * 60)
    print(f" {'Metric':<35} {'Flat':>10} {'Partitioned':>13}")
    print(f" {'-'*35} {'-'*10} {'-'*13}")
    print(f" {'Write time (s)':<35} {write_flat:>9.1f}s {write_part:>12.1f}s")
    print(f" {'Single-day query (s)':<35} {q1_flat:>9.2f}s {q1_part:>12.2f}s")
    print(f" {'7-day range query (s)':<35} {q2_flat:>9.2f}s {q2_part:>12.2f}s")
    print(f" {'Single-day speedup':<35} {'1.00x':>10} {speedup1:>12.2f}x")
    print(f" {'7-day speedup':<35} {'1.00x':>10} {speedup2:>12.2f}x")
    print("=" * 60)
    print(" NOTE: Partition pruning lets Spark skip entire date directories,")
    print("       reducing I/O proportional to the fraction of dates queried.")

    # ── Write CSV summary ──────────────────────────────────────────────────
    summary_data = [
        ("write_flat_s",       write_flat,  None),
        ("write_partitioned_s",write_part,  None),
        ("q1_flat_s",          q1_flat,     speedup1),
        ("q1_partitioned_s",   q1_part,     speedup1),
        ("q2_flat_s",          q2_flat,     speedup2),
        ("q2_partitioned_s",   q2_part,     speedup2),
    ]
    spark.createDataFrame(
        summary_data, ["metric", "value", "speedup"]
    ).coalesce(1).write.mode("overwrite").option("header", True).csv(args.output + "/summary")

    print(f"\n Results written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
