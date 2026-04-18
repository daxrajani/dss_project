# Scalable E-commerce Analytics Pipeline (PySpark)

**Authors:** Dax Rajani (40294118) & Harsh Ahuja (40301748)
**Institution:** Concordia University — COEN 6731 Distributed Software Systems, Winter 2026

---

## Project Overview

A fully implemented distributed data processing pipeline built with Apache Spark (PySpark).
Processes the REES46 e-commerce dataset (40M+ rows, 5.3 GB) through five stages:
ETL enrichment → sessionization → funnel analysis → last-touch attribution → anomaly detection.

Cloud scaling benchmarks (2/4/6 workers × 1 GB/5 GB/10 GB) are run on GCP Dataproc.

---

## Pipeline Stages

| Stage | Script | Description |
|---|---|---|
| 1. ETL Enrichment | `src/etl_enrichment.py` | Reads raw CSV, synthesizes `device`/`referrer` via CRC32 hash on `user_session`, derives `orders` and `catalog` tables, writes Parquet |
| 2. Sessionization | `src/benchmark_job.py` | 30-min inactivity threshold per user using window functions |
| 3. Funnel Analysis | `src/benchmark_job.py` | view → cart → purchase conversion rates by `(category, device, referrer)` at daily and weekly granularity |
| 4. Attribution | `src/attribution.py` | Last-touch model: most recent non-direct referrer session within 24 hr before each order |
| 5. Anomaly Detection | `src/anomaly_detection.py` | 7-day rolling z-score baseline; flags SPIKE/DROP days with human-readable explanations, system-wide and per-category |

---

## Dataset

**REES46 eCommerce behavior data** — Kaggle, October 2019
- 42,448,764 events | 742,849 orders | 166,794 products
- Columns: `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`
- Missing fields synthesized deterministically: `device` (CRC32 % 100 → mobile/desktop/tablet) and `referrer` (CRC32 % 100 → direct/google/facebook/instagram/email/affiliate)

---

## Reproducing the Pipeline

### Prerequisites
- Linux / WSL (Ubuntu recommended)
- Python 3.10+
- Java 11 (required for PySpark 4.x)

### Local Run (full pipeline)
```bash
bash run_benchmark.sh
# or with explicit input:
bash run_benchmark.sh --input data/data_set/2019-Oct.csv --output data/processed
```

### Cloud Run (GCP Dataproc)
```bash
# ETL
gcloud dataproc jobs submit pyspark src/etl_enrichment.py \
  --cluster=YOUR_CLUSTER \
  -- --input gs://YOUR_BUCKET/raw/2019-Oct.csv --output gs://YOUR_BUCKET/processed

# Attribution
gcloud dataproc jobs submit pyspark src/attribution.py \
  --cluster=YOUR_CLUSTER \
  -- --input gs://YOUR_BUCKET/processed --output gs://YOUR_BUCKET/processed/attribution

# Anomaly Detection
gcloud dataproc jobs submit pyspark src/anomaly_detection.py \
  --cluster=YOUR_CLUSTER \
  -- --input gs://YOUR_BUCKET/processed --output gs://YOUR_BUCKET/processed/anomalies
```

### Quick Smoke Test (small data, ~60 seconds total)
```bash
source dss_env/bin/activate
python src/etl_enrichment.py --input data/raw/test_slice.csv --output data/processed
python src/attribution.py
python src/anomaly_detection.py
```

---

## Output Structure

```
data/processed/
  enriched_events/      ← Parquet: 42M rows with device + referrer
  orders/               ← Parquet: 742K purchase orders
  catalog/              ← Parquet: 166K distinct products
  funnel_metrics_daily/ ← Parquet: conversion rates by day
  funnel_metrics_weekly/← Parquet: conversion rates by week
  attribution/
    attributed_orders/  ← Parquet: per-order attribution
    attribution_summary/← Parquet: revenue by referrer channel
  anomalies/
    daily_anomalies/    ← Parquet: system-level spike/drop days
    category_anomalies/ ← Parquet: per-category spike/drop days
```

---

## Availability

Source code: https://github.com/daxrajani/dss_project
