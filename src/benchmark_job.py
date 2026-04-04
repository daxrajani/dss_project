"""
benchmark_job.py — Complete DSS pipeline benchmark.

Runs all 5 stages in order using a shared SparkSession and prints a
timing summary table at the end.

Stage order:
  1. ETL Enrichment     (etl_enrichment.py)
  2. Sessionization     (30-min inactivity window, reuses _derive_sessions)
  3. Funnel Analysis    (view→cart→purchase conversion rates)
  4. Attribution        (attribution.py)
  5. Anomaly Detection  (anomaly_detection.py)

Usage:
    python src/benchmark_job.py
    python src/benchmark_job.py --input gs://bucket/raw/2019-Oct.csv --output gs://bucket/processed
"""

import argparse
import os
import sys
import time

# Allow sibling modules to be imported when this script is run directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max as _max, round as _round, sum as _sum,
    to_date, trunc, when,
)
from pyspark.sql.window import Window

import etl_enrichment
import attribution as attr_module
import anomaly_detection as anomaly_module
from attribution import _derive_sessions   # reuse — avoids duplicating window logic


# ---------------------------------------------------------------------------
# Stage 2: Sessionization
# ---------------------------------------------------------------------------

def run_sessionization(spark, processed_dir):
    """Read enriched_events and materialise sessions via 30-min inactivity window."""
    events_path = os.path.join(processed_dir, "enriched_events")
    df = spark.read.parquet(events_path)

    sessionized = _derive_sessions(df).cache()
    n = sessionized.count()   # triggers and caches the computation
    print(f"    {n:,} events across derived sessions")
    return sessionized


# ---------------------------------------------------------------------------
# Stage 3: Funnel Analysis
# ---------------------------------------------------------------------------

def _compute_funnel_rates(session_funnel_df, grain_col):
    """
    Aggregate session-level funnel flags to conversion rates.
    grain_col is the time column name ('date' for daily, 'week' for weekly).
    """
    return (
        session_funnel_df
        .groupBy(grain_col, "category_code", "device", "referrer")
        .agg(
            _sum("has_view").alias("total_sessions_with_view"),
            _sum("has_cart").alias("total_sessions_with_cart"),
            _sum("has_purchase").alias("total_sessions_with_purchase"),
        )
        .withColumn(
            "view_to_cart_pct",
            when(col("total_sessions_with_view") == 0, 0.0).otherwise(
                _round(col("total_sessions_with_cart") / col("total_sessions_with_view") * 100, 2)
            ),
        )
        .withColumn(
            "cart_to_buy_pct",
            when(col("total_sessions_with_cart") == 0, 0.0).otherwise(
                _round(col("total_sessions_with_purchase") / col("total_sessions_with_cart") * 100, 2)
            ),
        )
        .filter(col("category_code") != "unknown")
        .orderBy(grain_col, ascending=False)
    )


def run_funnel_analysis(sessionized_df, processed_dir):
    """
    Compute view→cart→purchase conversion rates per
    (category_code, device, referrer) at both daily and weekly granularity.
    Writes funnel_metrics_daily/ and funnel_metrics_weekly/ as Parquet.
    """
    df = (
        sessionized_df
        .withColumn("date", to_date("event_time"))
        .withColumn("week", trunc(col("event_time"), "week"))
        .withColumn("is_view",     when(col("event_type") == "view",     1).otherwise(0))
        .withColumn("is_cart",     when(col("event_type") == "cart",     1).otherwise(0))
        .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0))
    )

    # Session-level flags — computed once, reused for both granularities
    session_flags = df.groupBy(
        "session_id", "date", "week", "category_code", "device", "referrer"
    ).agg(
        _max("is_view").alias("has_view"),
        _max("is_cart").alias("has_cart"),
        _max("is_purchase").alias("has_purchase"),
    ).cache()
    session_flags.count()  # materialise cache

    # Daily
    daily = _compute_funnel_rates(session_flags, "date")
    daily_out = os.path.join(processed_dir, "funnel_metrics_daily")
    daily.write.mode("overwrite").parquet(daily_out)
    print(f"    funnel_metrics_daily  → {daily_out}")

    # Weekly
    weekly = _compute_funnel_rates(session_flags, "week")
    weekly_out = os.path.join(processed_dir, "funnel_metrics_weekly")
    weekly.write.mode("overwrite").parquet(weekly_out)
    print(f"    funnel_metrics_weekly → {weekly_out}")


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

def _run_stage(label, timings, fn, *args, **kwargs):
    """Execute fn, record elapsed seconds under label, return fn's result."""
    sep = "─" * 52
    print(f"\n┌{sep}┐")
    print(f"│  STAGE {len(timings) + 1}/5 — {label:<38}│")
    print(f"└{sep}┘")
    t0 = time.time()
    result = fn(*args, **kwargs)
    elapsed = time.time() - t0
    timings[label] = elapsed
    print(f"    ✓ {label} done in {elapsed:.2f}s")
    return result


def _print_timing_table(timings):
    total   = sum(timings.values())
    name_w  = max(len(k) for k in timings)
    pad     = max(name_w, 18)

    border_top    = "╔" + "═" * (pad + 4) + "╦" + "═" * 14 + "╦" + "═" * 11 + "╗"
    border_mid    = "╠" + "═" * (pad + 4) + "╬" + "═" * 14 + "╬" + "═" * 11 + "╣"
    border_bottom = "╚" + "═" * (pad + 4) + "╩" + "═" * 14 + "╩" + "═" * 11 + "╝"
    header        = f"║  {'Stage':<{pad + 2}}║  {'Time (s)':>10}  ║  {'Share':>6}   ║"

    print("\n")
    print(border_top)
    print(header)
    print(border_mid)
    for name, t in timings.items():
        pct = t / total * 100
        print(f"║  {name:<{pad + 2}}║  {t:>10.2f}  ║  {pct:>5.1f}%   ║")
    print(border_mid)
    print(f"║  {'TOTAL':<{pad + 2}}║  {total:>10.2f}  ║  {'100.0%':>8}  ║")
    print(border_bottom)
    print()


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(input_path: str, output_dir: str, spark: SparkSession):
    timings: dict = {}

    attribution_out = os.path.join(output_dir, "attribution")
    anomaly_out     = os.path.join(output_dir, "anomalies")

    # 1. ETL Enrichment
    _run_stage(
        "ETL Enrichment", timings,
        etl_enrichment.run, input_path, output_dir, spark,
    )

    # 2. Sessionization
    sessionized = _run_stage(
        "Sessionization", timings,
        run_sessionization, spark, output_dir,
    )

    # 3. Funnel Analysis
    _run_stage(
        "Funnel Analysis", timings,
        run_funnel_analysis, sessionized, output_dir,
    )

    # 4. Attribution
    _run_stage(
        "Attribution", timings,
        attr_module.run, output_dir, attribution_out, spark,
    )

    # 5. Anomaly Detection
    _run_stage(
        "Anomaly Detection", timings,
        anomaly_module.run, output_dir, anomaly_out, spark,
    )

    _print_timing_table(timings)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _default_input():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    kaggle = os.path.join(base, "data", "data_set", "2019-Oct.csv")
    return kaggle if os.path.exists(kaggle) else os.path.join(base, "data", "raw", "events.csv")


def _default_output():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "data", "processed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DSS Complete Pipeline Benchmark")
    parser.add_argument(
        "--input",  default=_default_input(),
        help="Raw events CSV path (local or gs://)",
    )
    parser.add_argument(
        "--output", default=_default_output(),
        help="Processed output base directory (local or gs://)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("DSS_Pipeline_Benchmark")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "═" * 54)
    print("  DSS COMPLETE PIPELINE BENCHMARK")
    print("═" * 54)
    print(f"  Input  : {args.input}")
    print(f"  Output : {args.output}")
    print("═" * 54)

    pipeline_start = time.time()
    run_pipeline(args.input, args.output, spark)
    print(f"  Wall-clock total: {time.time() - pipeline_start:.2f}s")

    spark.stop()
