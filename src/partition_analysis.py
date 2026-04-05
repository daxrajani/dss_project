"""
partition_analysis.py — Parquet partitioning benefits demonstration.

Compares two storage layouts for the anomaly-detection output:
  BASELINE  : flat Parquet (no partition columns)
  OPTIMIZED : date-partitioned Parquet (partitionBy("date"))

Measures the time to execute a single-day and a 7-day date-range filter
on each layout. On the partitioned layout Spark uses partition pruning to
skip irrelevant files entirely — no full scan needed.

Usage:
    python src/partition_analysis.py
    python src/partition_analysis.py --input data/data_set/2019-Oct.csv --sample-fraction 0.1
"""

import argparse
import os
import shutil
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, regexp_replace, to_date, to_timestamp,
)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Parquet Partitioning Analysis")
    parser.add_argument("--input", default=None)
    parser.add_argument("--sample-fraction", type=float, default=0.1)
    args = parser.parse_args()

    if args.input is None:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        args.input = os.path.join(base, "data", "data_set", "2019-Oct.csv")

    base_dir  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_flat  = os.path.join(base_dir, "data", "processed", "part_test_flat")
    out_part  = os.path.join(base_dir, "data", "processed", "part_test_partitioned")

    # Clean up previous runs
    for d in [out_flat, out_part]:
        if os.path.exists(d):
            shutil.rmtree(d)

    spark = (
        SparkSession.builder
        .appName("DSS_Partition_Analysis")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "20")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "=" * 60)
    print(" DSS PARQUET PARTITIONING ANALYSIS")
    print("=" * 60)
    print(f" Input           : {args.input}")
    print(f" Sample fraction : {args.sample_fraction}")

    # ── Load + prepare purchase event daily counts ─────────────────────────
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

    # Determine date range for queries
    date_stats = raw.agg({"date": "min"}).collect()[0]
    # Use Oct 15 and Oct 16 as representative query dates
    query_day   = "2019-10-15"
    query_start = "2019-10-10"
    query_end   = "2019-10-17"

    # ── Write FLAT (unpartitioned) Parquet ─────────────────────────────────
    print(f"\n>>> Writing flat (unpartitioned) Parquet ...")
    t0 = time.time()
    raw.write.mode("overwrite").parquet(out_flat)
    write_flat = time.time() - t0
    flat_files = sum(1 for _, _, files in os.walk(out_flat) for f in files if f.endswith(".parquet"))
    print(f"    Written in {write_flat:.1f}s  |  {flat_files} Parquet files")

    # ── Write DATE-PARTITIONED Parquet ─────────────────────────────────────
    print(f"\n>>> Writing date-partitioned Parquet ...")
    t0 = time.time()
    raw.write.mode("overwrite").partitionBy("date").parquet(out_part)
    write_part = time.time() - t0
    part_dirs = [d for d in os.listdir(out_part) if d.startswith("date=")]
    print(f"    Written in {write_part:.1f}s  |  {len(part_dirs)} date partitions")

    # ── Read back for fair query comparison ────────────────────────────────
    df_flat = spark.read.parquet(out_flat)
    df_part = spark.read.parquet(out_part)

    print(f"\n" + "-" * 60)
    print(f" QUERY 1 — Single day filter  ({query_day})")
    print("-" * 60)

    # Flat
    t0 = time.time()
    n_flat = df_flat.filter(col("date") == query_day).count()
    q1_flat = time.time() - t0

    # Partitioned
    t0 = time.time()
    n_part = df_part.filter(col("date") == query_day).count()
    q1_part = time.time() - t0

    speedup1 = q1_flat / q1_part if q1_part > 0 else 0
    print(f"  Flat        : {q1_flat:.2f}s  →  {n_flat:,} rows")
    print(f"  Partitioned : {q1_part:.2f}s  →  {n_part:,} rows")
    print(f"  Speedup     : {speedup1:.2f}x  (partition pruning skips all other date dirs)")

    print(f"\n" + "-" * 60)
    print(f" QUERY 2 — 7-day range filter  ({query_start} to {query_end})")
    print("-" * 60)

    # Flat
    t0 = time.time()
    n_flat2 = df_flat.filter(
        (col("date") >= query_start) & (col("date") <= query_end)
    ).count()
    q2_flat = time.time() - t0

    # Partitioned
    t0 = time.time()
    n_part2 = df_part.filter(
        (col("date") >= query_start) & (col("date") <= query_end)
    ).count()
    q2_part = time.time() - t0

    speedup2 = q2_flat / q2_part if q2_part > 0 else 0
    print(f"  Flat        : {q2_flat:.2f}s  →  {n_flat2:,} rows")
    print(f"  Partitioned : {q2_part:.2f}s  →  {n_part2:,} rows")
    print(f"  Speedup     : {speedup2:.2f}x  (only 7 of {len(part_dirs)} partition dirs scanned)")

    print(f"\n" + "=" * 60)
    print(" PARTITIONING SUMMARY")
    print("=" * 60)
    print(f" {'Metric':<35} {'Flat':>10} {'Partitioned':>13}")
    print(f" {'-'*35} {'-'*10} {'-'*13}")
    print(f" {'Write time (s)':<35} {write_flat:>9.1f}s {write_part:>12.1f}s")
    print(f" {'Single-day query (s)':<35} {q1_flat:>9.2f}s {q1_part:>12.2f}s")
    print(f" {'7-day range query (s)':<35} {q2_flat:>9.2f}s {q2_part:>12.2f}s")
    print(f" {'Single-day speedup':<35} {'1.00x':>10} {speedup1:>12.2f}x")
    print(f" {'7-day speedup':<35} {'1.00x':>10} {speedup2:>12.2f}x")
    print(f" {'Partition dirs':<35} {'N/A':>10} {len(part_dirs):>13}")
    print("=" * 60)
    print(f"\n NOTE: Partitioned layout enables Spark 'partition pruning' —")
    print(f"       the query planner skips entire date directories,")
    print(f"       reducing I/O proportionally to the fraction of dates queried.")
    print()

    # Clean up temp directories
    shutil.rmtree(out_flat, ignore_errors=True)
    shutil.rmtree(out_part, ignore_errors=True)

    spark.stop()


if __name__ == "__main__":
    main()
