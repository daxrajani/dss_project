"""
streaming_job.py — Real-time purchase anomaly detection with Spark Structured Streaming.

Reads a stream of e-commerce events (CSV files dropped into a watch directory),
computes a rolling purchase count per category in a 5-minute sliding window,
and flags categories where the purchase rate exceeds a threshold.

Architecture:
    stream_producer.py  →  data/raw/stream_input/  →  streaming_job.py  →  console + Parquet

How to run (two terminals):
    Terminal 1:  python src/streaming_job.py
    Terminal 2:  python src/stream_producer.py      (generates event CSV files every 3s)

What it demonstrates:
    - Spark Structured Streaming with file-based source
    - Tumbling window aggregation (purchase counts per category per window)
    - Real-time anomaly flag (purchase spike > threshold)
    - Stateful streaming with checkpointing

Options:
    --test-timeout SECONDS   Stop automatically after N seconds (0 = run forever)
    --input-dir PATH         Override the default stream_input directory
"""

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, to_timestamp, window, when,
    regexp_replace,
)
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType,
)


# ---------------------------------------------------------------------------
# Schema — matches stream_producer.py output
# ---------------------------------------------------------------------------

EVENT_SCHEMA = StructType([
    StructField("event_time",   StringType(),  True),
    StructField("event_type",   StringType(),  True),
    StructField("user_id",      StringType(),  True),
    StructField("product_id",   StringType(),  True),
    StructField("category_code",StringType(),  True),
    StructField("price",        DoubleType(),  True),
    StructField("referrer",     StringType(),  True),
])

# Flag a category window as anomalous if purchase count exceeds this threshold
SPIKE_THRESHOLD = 5


# ---------------------------------------------------------------------------
# Streaming pipeline
# ---------------------------------------------------------------------------

def run_streaming_job(test_timeout=0, input_dir_override=None):
    base_dir      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_dir     = input_dir_override or os.path.join(base_dir, "data", "raw", "stream_input")
    output_dir    = os.path.join(base_dir, "data", "processed", "stream_output")
    checkpoint_dir= os.path.join(base_dir, "data", "processed", "stream_checkpoints")

    os.makedirs(input_dir,      exist_ok=True)
    os.makedirs(output_dir,     exist_ok=True)
    os.makedirs(checkpoint_dir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("DSS_Streaming_Anomaly")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print(" DSS REAL-TIME STREAMING ANOMALY DETECTION")
    print("=" * 60)
    print(f" Watching : {input_dir}")
    print(f" Output   : {output_dir}")
    print(f" Window   : 5-minute tumbling")
    print(f" Threshold: purchases > {SPIKE_THRESHOLD} per window = SPIKE alert")
    print("=" * 60)
    print(" Run `python src/stream_producer.py` in another terminal to generate events.\n")

    # ── Read stream ───────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .schema(EVENT_SCHEMA)
        .option("header", "true")
        .option("maxFilesPerTrigger", "1")
        .csv(input_dir)
    )

    # Parse event_time and keep only purchase events
    events = (
        raw_stream
        .withColumn(
            "event_time",
            to_timestamp(regexp_replace(col("event_time"), r" UTC$", "")),
        )
        .filter(col("event_type") == "purchase")
        .fillna({"category_code": "unknown"})
        .withWatermark("event_time", "10 minutes")   # handle late data up to 10 min
    )

    # ── Tumbling 5-minute window aggregation per category ─────────────────
    windowed = (
        events
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("category_code"),
        )
        .agg(count("*").alias("purchase_count"))
    )

    # ── Anomaly flag ──────────────────────────────────────────────────────
    with_flag = windowed.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category_code"),
        col("purchase_count"),
        when(col("purchase_count") > SPIKE_THRESHOLD,
             "⚠ SPIKE").otherwise("OK").alias("status"),
    )

    # ── Write to console (real-time view) ─────────────────────────────────
    console_query = (
        with_flag.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", os.path.join(checkpoint_dir, "console"))
        .trigger(processingTime="5 seconds")
        .start()
    )

    # ── Write to Parquet (persistent output) ──────────────────────────────
    parquet_query = (
        with_flag.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output_dir)
        .option("checkpointLocation", os.path.join(checkpoint_dir, "parquet"))
        .trigger(processingTime="5 seconds")
        .start()
    )

    if test_timeout > 0:
        print(f">>> Stream started (test mode: auto-stop in {test_timeout}s).\n")
    else:
        print(">>> Stream started. Press Ctrl+C to stop.\n")
    try:
        if test_timeout > 0:
            console_query.awaitTermination(test_timeout * 1000)
        else:
            console_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n>>> Stream gracefully terminated by user.")
    finally:
        console_query.stop()
        parquet_query.stop()
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DSS Streaming Anomaly Detection")
    parser.add_argument("--test-timeout", type=int, default=0,
                        help="Auto-stop after N seconds (0 = run until Ctrl+C)")
    parser.add_argument("--input-dir", default=None,
                        help="Override stream_input directory path")
    args = parser.parse_args()
    run_streaming_job(test_timeout=args.test_timeout, input_dir_override=args.input_dir)
