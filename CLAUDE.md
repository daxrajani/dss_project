# DSS Project — E-commerce Analytics with Apache Spark

## Overview
COEN 6731 Distributed Systems course project, Concordia University Winter 2026.
Team: Dax Rajani (40294118), Harsh Ahuja (40301748).
PySpark pipeline for sessionization, funnel analysis, attribution, anomaly detection.

## Dataset
REES46 "eCommerce behavior data from multi category store" (~40M+ rows).
Columns: event_time, event_type (view/cart/purchase), product_id, category_id, category_code, brand, price, user_id, user_session.
Missing fields that must be synthesized: device, referrer. No separate orders table — derive from purchase events.

## Pipeline Modules
1. ETL & Enrichment — Add synthetic device/referrer via deterministic hashing on user_session, derive orders and catalog tables (DONE)
2. Sessionization — 30-min inactivity threshold using window functions (DONE)
3. Funnel Analysis — View->Cart->Purchase conversion rates by category/device/referrer/day/week (DONE)
4. Last-Touch Attribution — Temporal join: for each order, find most recent non-direct referrer session within 24hr lookback (DONE)
5. Anomaly Detection — 7-day rolling avg baseline, z-score > 2 flags anomalies, per-day and per-category (DONE)

## Commands
- bash run_benchmark.sh — Full setup and run
- python src/generator.py — Generate/prepare dataset
- python src/benchmark_job.py — Run the Spark pipeline

## Tech
- PySpark, Python 3, pandas, pyarrow
- Local mode + GCP Dataproc for cloud scaling study
- Parquet format for intermediate data
- Every pipeline stage must have timing instrumentation (time.time())

## Code Structure
- src/ — all PySpark scripts
- data/raw/ — input data
- data/processed/ — output data
- Scripts must accept --input and --output CLI args for local/cloud flexibility

## Cloud
- GCP Dataproc: 1 master + 2/4/6 workers (n1-standard-4)
- Data on GCS bucket
- Benchmarks: vary cluster size (2/4/6 workers) x data size (1GB/5GB/10GB)

## Attribution Logic Detail
- Synthesize referrer per session: direct(40%), google(25%), facebook(15%), instagram(8%), email(7%), affiliate(5%)
- Use crc32 hash of user_session for deterministic assignment
- Join orders with session_referrers where referrer != "direct" and session_start within 24hr before order_time
- Pick most recent via row_number() window ordered by session_start desc

## Anomaly Detection Logic Detail
- Compute daily purchase counts
- 7-day trailing rolling avg and stddev using rangeBetween window
- Flag days where abs(z_score) > 2
- Generate text explanations (SPIKE/DROP with % deviation)
- Also compute per-category anomalies
