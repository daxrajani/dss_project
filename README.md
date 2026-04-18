# Scalable E-commerce Analytics Pipeline (PySpark / GCP Dataproc)

**Authors:** Dax Rajani (40294118) · Harsh Ahuja (40301748)
**Course:** COEN 6731 Distributed Software Systems — Concordia University, Winter 2026
**Repo:** https://github.com/daxrajani/dss_project

---

## Overview

A distributed data processing pipeline built with Apache Spark (PySpark) that processes the REES46 e-commerce dataset (42M+ rows, 5.3 GB) through five analytical stages. Cloud scaling benchmarks were run on GCP Dataproc across 2/4/5 workers × 1 GB/5 GB/10 GB data sizes.

---

## Pipeline Stages

| Stage | Script | What it does |
|---|---|---|
| 1. ETL Enrichment | `src/etl_enrichment.py` | Reads raw CSV, synthesizes `device` + `referrer` via CRC32 hash, derives `orders` and `catalog` tables, writes Parquet |
| 2. Sessionization | `src/benchmark_job.py` | 30-min inactivity threshold per user using window functions |
| 3. Funnel Analysis | `src/benchmark_job.py` | view → cart → purchase conversion rates by category/device/referrer at daily + weekly granularity |
| 4. Attribution | `src/attribution.py` | Last-touch model: most recent non-direct referrer session within 24 hr before each order |
| 5. Anomaly Detection | `src/anomaly_detection.py` | 7-day rolling z-score baseline; flags SPIKE/DROP days with explanations |

All five stages are also inlined in `src/cloud_pipeline.py` — a self-contained Dataproc script that runs the full pipeline in a single job.

---

## Dataset

**REES46 eCommerce behavior data** — Kaggle, October 2019
- 42,448,764 events · 742,849 orders · 166,794 products
- Columns: `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`
- Synthesized fields: `device` (mobile 55% / desktop 30% / tablet 15%) and `referrer` (direct 40% / google 25% / facebook 15% / instagram 8% / email 7% / affiliate 5%) — both derived deterministically via `crc32(user_session) % 100`

---

## Repository Structure

```
dss_project/
├── demo.sh                         # Professor demo script (cloud / stream / results modes)
├── dashboard.sh                    # Launch the live web dashboard
├── requirements.txt                # Python dependencies
│
├── src/
│   ├── cloud_pipeline.py           # Self-contained Dataproc script — all 5 stages
│   ├── etl_enrichment.py           # Stage 1: ETL
│   ├── benchmark_job.py            # Stages 2+3: Sessionization + Funnel (also GCP-uploadable)
│   ├── attribution.py              # Stage 4: Last-touch attribution
│   ├── anomaly_detection.py        # Stage 5: Anomaly detection
│   │
│   ├── cloud_benchmark.sh          # Submit cloud_pipeline.py to Dataproc
│   ├── cloud_submit.sh             # Submit benchmark_job.py to Dataproc
│   ├── run_scaling_study.sh        # Run all 9 benchmark combinations (2/4/5w × 1/5/10gb)
│   ├── run_all_benchmarks.sh       # Alias for run_scaling_study.sh
│   │
│   ├── cloud_broadcast_join.py     # Optimization: broadcast join vs shuffle join
│   ├── cloud_partition_analysis.py # Optimization: flat vs date-partitioned Parquet
│   ├── cloud_skew_analysis.py      # Optimization: hot-key skew + salting mitigation
│   │
│   ├── streaming_job.py            # Spark Structured Streaming — real-time anomaly detection
│   ├── stream_producer.py          # Produces synthetic event batches for streaming demo
│   │
│   ├── generate_all_charts.py      # Regenerate all benchmark charts from results/
│   └── dashboard/                  # Flask web dashboard (GCP monitoring + analytics)
│       ├── app.py
│       └── templates/index.html
│
└── results/                        # Pre-computed benchmark logs and charts
    ├── benchmark_summary.csv
    ├── cloud_*.log                 # Per-run Dataproc logs (2/4/5w × 1/5/10gb)
    ├── broadcast_join_results.log
    ├── partition_analysis_results.log
    ├── skew_analysis_results.log
    └── charts/                     # PNG charts for all experiments
```

---

## Prerequisites

1. **GCP account** with Dataproc and GCS APIs enabled
2. **gcloud CLI** authenticated:
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```
3. **GCS bucket** with dataset and scripts uploaded (see Setup below)
4. **Python 3.10+** and a virtual environment for the dashboard/streaming demo

---

## Setup — Upload Scripts and Data to GCS

```bash
# Set your GCP config
export GCP_PROJECT=your-project-id
export GCS_BUCKET=gs://your-bucket
export GCP_REGION=us-central1

# Upload all pipeline scripts to GCS
gsutil -m cp src/cloud_pipeline.py        $GCS_BUCKET/scripts/
gsutil -m cp src/etl_enrichment.py        $GCS_BUCKET/scripts/
gsutil -m cp src/benchmark_job.py         $GCS_BUCKET/scripts/
gsutil -m cp src/attribution.py           $GCS_BUCKET/scripts/
gsutil -m cp src/anomaly_detection.py     $GCS_BUCKET/scripts/
gsutil -m cp src/cloud_broadcast_join.py  $GCS_BUCKET/scripts/
gsutil -m cp src/cloud_partition_analysis.py $GCS_BUCKET/scripts/
gsutil -m cp src/cloud_skew_analysis.py   $GCS_BUCKET/scripts/

# Upload dataset (if not already on GCS)
gsutil -m cp 2019-Oct.csv $GCS_BUCKET/data/2019-Oct.csv
```

---

## Running the Pipeline on GCP Dataproc

### Single run (recommended — creates/reuses cluster automatically)

```bash
export GCP_PROJECT=your-project-id
export GCS_BUCKET=gs://your-bucket
export GCP_REGION=us-central1

bash src/cloud_benchmark.sh 5 5gb
# Args: <workers: 2|4|5>  <data: 1gb|5gb|10gb>
```

The script will:
- **Create** the cluster if it doesn't exist
- **Start** it if it's stopped (saves ~3 min vs recreating)
- **Submit** `cloud_pipeline.py` as a PySpark job
- **Stop** the cluster when done (not delete — reuse next time)
- Save the log to `results/cloud_5w_5gb.log`

### Full scaling study — all 9 combinations (2/4/5 workers × 1/5/10 GB)

```bash
bash src/run_scaling_study.sh
```

---

## Demo Script

```bash
bash demo.sh cloud      # Submit live pipeline to GCP Dataproc
bash demo.sh stream     # Real-time streaming anomaly detection (local, 60s)
bash demo.sh results    # Print benchmark results table + optimization summary
bash demo.sh all        # Cloud + results (default)
```

---

## Live Dashboard

The dashboard shows real-time GCP cluster metrics, allows cluster resizing, job submission, and displays pipeline analytics.

```bash
pip install -r requirements.txt
python src/dashboard/app.py
# Open http://localhost:5000
```

The dashboard auto-detects your GCP project and cluster from `gcloud config`. No env vars required if `gcloud` is authenticated.

---

## Optimization Experiments (GCP)

Each script compares a baseline vs an optimized approach on Dataproc:

```bash
# Broadcast join vs shuffle join
gcloud dataproc jobs submit pyspark $GCS_BUCKET/scripts/cloud_broadcast_join.py \
  --cluster=CLUSTER --region=us-central1 \
  -- --input $GCS_BUCKET/data/2019-Oct.csv --output $GCS_BUCKET/results/broadcast_join

# Partition pruning (flat vs date-partitioned Parquet)
gcloud dataproc jobs submit pyspark $GCS_BUCKET/scripts/cloud_partition_analysis.py \
  --cluster=CLUSTER --region=us-central1 \
  -- --input $GCS_BUCKET/data/2019-Oct.csv --output $GCS_BUCKET/results/partition_analysis

# Skew analysis + salting mitigation
gcloud dataproc jobs submit pyspark $GCS_BUCKET/scripts/cloud_skew_analysis.py \
  --cluster=CLUSTER --region=us-central1 \
  -- --input $GCS_BUCKET/data/2019-Oct.csv --output $GCS_BUCKET/results/skew_analysis
```

---

## Pre-computed Results

All benchmark results are already captured in `results/`:

| Experiment | File |
|---|---|
| Scaling study (2/4/5w × 1/5/10gb) | `results/cloud_*w_*.log` + `results/benchmark_summary.csv` |
| Broadcast join optimization | `results/broadcast_join_results.log` |
| Partition analysis | `results/partition_analysis_results.log` |
| Skew analysis | `results/skew_analysis_results.log` |
| Charts | `results/charts/*.png` |

View a summary instantly:
```bash
bash demo.sh results
```

---

## Output Structure (after pipeline run)

```
GCS_BUCKET/results/demo_run/
  enriched_events/        ← 42M rows with device + referrer
  orders/                 ← 742K purchase orders
  catalog/                ← 166K distinct products
  funnel_metrics_daily/   ← Conversion rates by day
  funnel_metrics_weekly/  ← Conversion rates by week
  attribution/
    attributed_orders/    ← Per-order attribution
    attribution_summary/  ← Revenue by referrer channel
  anomalies/
    daily_anomalies/      ← System-level SPIKE/DROP days
    category_anomalies/   ← Per-category anomalies
```
