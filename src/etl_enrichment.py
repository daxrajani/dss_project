"""
ETL Enrichment — Phase 1 of the DSS e-commerce pipeline.

Reads raw REES46 events, synthesizes device/referrer via deterministic CRC32
hashing on user_session, derives orders and catalog tables, and writes all
three datasets as Parquet to data/processed/.

Usage:
    python src/etl_enrichment.py
    python src/etl_enrichment.py --input gs://bucket/raw/2019-Oct.csv --output gs://bucket/processed
"""

import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, crc32, when, to_timestamp, regexp_replace,
    monotonically_increasing_id,
)


# ---------------------------------------------------------------------------
# CRC32-based synthetic column helpers
# ---------------------------------------------------------------------------

def _add_device(df):
    """
    Deterministic device assignment from crc32(user_session) % 100.
      0–54  → mobile   (55%)
     55–84  → desktop  (30%)
     85–99  → tablet   (15%)
    """
    bucket = crc32(col("user_session")) % 100
    return df.withColumn(
        "device",
        when(bucket < 55, "mobile")
        .when(bucket < 85, "desktop")
        .otherwise("tablet"),
    )


def _add_referrer(df):
    """
    Deterministic referrer assignment from crc32(user_session) % 100.
      0–39  → direct    (40%)
     40–64  → google    (25%)
     65–79  → facebook  (15%)
     80–87  → instagram  (8%)
     88–94  → email      (7%)
     95–99  → affiliate  (5%)
    """
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


# ---------------------------------------------------------------------------
# Main ETL
# ---------------------------------------------------------------------------

def run(input_path: str, output_dir: str, spark: SparkSession):
    total_start = time.time()

    # ------------------------------------------------------------------
    # Step 1 — Read raw events
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [1/6] Reading raw events ...")

    if input_path.endswith(".csv") or input_path.endswith(".csv.gz"):
        raw = spark.read.csv(input_path, header=True, inferSchema=False)
    else:
        raw = spark.read.parquet(input_path)

    # Normalise timestamp: strip trailing " UTC" then cast
    raw = raw.withColumn(
        "event_time",
        to_timestamp(regexp_replace(col("event_time"), r" UTC$", "")),
    ).withColumn("price", col("price").cast("double"))

    row_count = raw.count()
    print(f"    Loaded {row_count:,} rows  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Step 2 — Synthesise device
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [2/6] Synthesising device column (CRC32 hash) ...")
    enriched = _add_device(raw)
    print(f"    Done  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Step 3 — Synthesise referrer
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [3/6] Synthesising referrer column (CRC32 hash) ...")
    enriched = _add_referrer(enriched)
    print(f"    Done  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Step 4 — Derive orders
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [4/6] Deriving orders from purchase events ...")
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
    print(f"    {order_count:,} orders derived  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Step 5 — Derive catalog
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [5/6] Deriving product catalog ...")
    catalog = (
        enriched
        .select(
            col("product_id"),
            col("category_code"),
            col("brand"),
            col("price"),
        )
        .dropDuplicates(["product_id"])
        .fillna({"category_code": "unknown", "brand": "unknown"})
    )
    catalog_count = catalog.count()
    print(f"    {catalog_count:,} distinct products  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Step 6 — Write Parquet
    # ------------------------------------------------------------------
    t0 = time.time()
    print("\n>>> [6/6] Writing Parquet outputs ...")

    events_out  = os.path.join(output_dir, "enriched_events")
    orders_out  = os.path.join(output_dir, "orders")
    catalog_out = os.path.join(output_dir, "catalog")

    enriched.write.mode("overwrite").parquet(events_out)
    print(f"    enriched_events → {events_out}")

    orders.write.mode("overwrite").parquet(orders_out)
    print(f"    orders          → {orders_out}")

    catalog.write.mode("overwrite").parquet(catalog_out)
    print(f"    catalog         → {catalog_out}")

    print(f"    Write complete  ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    total = time.time() - total_start
    print("\n=============================================")
    print(f" ETL ENRICHMENT COMPLETE  —  {total:.1f}s total")
    print(f"   Events  : {row_count:,}")
    print(f"   Orders  : {order_count:,}")
    print(f"   Catalog : {catalog_count:,} products")
    print("=============================================\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _default_input():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidate = os.path.join(base, "data", "data_set", "2019-Oct.csv")
    if os.path.exists(candidate):
        return candidate
    return os.path.join(base, "data", "raw", "events.csv")


def _default_output():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "data", "processed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DSS ETL Enrichment")
    parser.add_argument(
        "--input",
        default=_default_input(),
        help="Path to raw events CSV or Parquet (local or gs://)",
    )
    parser.add_argument(
        "--output",
        default=_default_output(),
        help="Output directory for Parquet files (local or gs://)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("DSS_ETL_Enrichment")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("\n=============================================")
    print(" DSS ETL ENRICHMENT PIPELINE")
    print("=============================================")
    print(f" Input  : {args.input}")
    print(f" Output : {args.output}")

    run(args.input, args.output, spark)

    spark.stop()
