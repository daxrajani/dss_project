# Scalable E-commerce Analytics Pipeline with PySpark and GCP Dataproc

**Authors:** Dax Rajani (40294118) and Harsh Ahuja (40301748)  
**Course:** COEN 6731 Distributed Software Systems, Concordia University, Winter 2026

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/daxrajani/dss_project)

The GCP project and bucket are already configured. The only thing needed for cloud commands is `gcloud auth login`. Cluster creation, region selection, and job submission are all handled automatically.

---

## Screenshots

| | |
|---|---|
| ![Pipeline running](screenshots/dashboard_pipeline_execution.png) | ![VM Metrics](screenshots/dashboard_vm_matrix_whole_pipeline_execution.png) |
| *Live pipeline execution, 5 workers, 8.5M rows* | *Real-time CPU and disk metrics across all 6 VMs* |
| ![Fault tolerance](screenshots/dashboard_pipeline_execution_killed_worker_0.png) | ![Worker recovered](screenshots/dashboard_pipeline_execution_restarted_worker_0.png) |
| *Worker w-0 killed mid-pipeline, Spark detects and recovers* | *w-0 restarted, pipeline continues automatically* |
| ![Performance](screenshots/performance_analytics_results_when_w0_killed_restarted.png) | ![Pipeline success](screenshots/pipeline_finished_successfully.png) |
| *Stage timings: Total 556.9s, ETL 101.2s, Sessionization 83.0s* | *Pipeline completed with all metrics in left sidebar* |
| ![Funnel](screenshots/analytics_results_funnel.png) | ![Attribution](screenshots/analytics_results_attribution.png) |
| *Funnel: 3.3M views, 140K carts, 109K purchases* | *Attribution: Facebook $12.9M, Google $4.1M revenue* |
| ![Anomalies](screenshots/analytics_results_anomaly.png) | |
| *Anomaly detection: 8 anomalies flagged (3 spikes and 5 drops)* | |

---

## Overview

A distributed data processing pipeline built on Apache Spark that processes the REES46 e-commerce dataset (42M+ rows, 5.3 GB) through five analytical stages on GCP Dataproc. The project includes a live web dashboard with real-time cluster monitoring, job submission, VM metrics, and interactive analytics charts.

### Key Numbers from Live GCP Runs

| Metric | Value |
|---|---|
| Dataset | 42,448,764 events, Oct 2019 |
| Orders derived | 742,849 |
| Distinct products | 166,794 |
| Sessions identified | ~4,000,000+ |
| Orders attributed | 525,580 (5GB run) |
| Anomaly days flagged | 8 / 31 days |
| Best throughput | 125,961 rows/sec (5 workers x 10GB) |

---

## Pipeline Stages

| # | Stage | Description |
|---|---|---|
| 1 | **ETL and Enrichment** | Reads CSV, synthesizes `device` and `referrer` via CRC32 hash on `user_session`, derives `orders` and `catalog` tables, writes Parquet |
| 2 | **Sessionization** | 30 minute inactivity threshold per user using Spark window functions |
| 3 | **Funnel Analysis** | View to Cart to Purchase conversion rates by category, device, referrer at daily and weekly granularity |
| 4 | **Last-Touch Attribution** | Temporal join: for each order, finds the most recent non-direct referrer session within a 24 hour lookback window |
| 5 | **Anomaly Detection** | 7-day rolling z-score baseline that flags SPIKE and DROP days with text explanations, per day and per category |

All five stages are in `src/cloud_pipeline.py`, a single self-contained Dataproc job.

---

## Scaling Study Results

Full benchmark across 2, 4, and 5 workers with 1 GB, 5 GB, and 10 GB data (9 runs total).

| Workers | Data | Rows | Total Time | Rows/sec |
|---|---|---|---|---|
| 2 | 1 GB | 8.5M | 673.6s | 12,607 |
| 2 | 5 GB | 42.4M | 1,021.8s | 41,543 |
| 2 | 10 GB | 84.9M | 1,378.9s | 61,569 |
| 4 | 1 GB | 8.5M | 282.4s | 30,070 |
| 4 | 5 GB | 42.4M | 569.0s | 74,602 |
| 4 | 10 GB | 84.9M | 799.8s | 106,148 |
| **5** | **1 GB** | **8.5M** | **256.9s** | **33,055** |
| **5** | **5 GB** | **42.4M** | **409.7s** | **103,609** |
| **5** | **10 GB** | **84.9M** | **674.0s** | **125,961** |

Going from 2 to 5 workers gives a 2.0 to 2.6x speedup. Scaling from 1 GB to 10 GB on 5 workers adds only 2.6x more time for 10x more data, which shows strong sublinear scaling.

---

## Optimization Experiments

| Experiment | Baseline | Optimized | Speedup |
|---|---|---|---|
| Broadcast join vs shuffle join | 11.33s | 7.15s | **1.58x** |
| Partition pruning, single day | 4.25s | 1.37s | **3.11x** |
| Partition pruning, 7-day range | 3.09s | 1.78s | **1.73x** |
| Skew salting | 34.6s | 79.1s | 0.44x (salting is slower because the data is already uniform) |

Pre-computed results are in `results/`. Charts are in `results/charts/`.

---

## Repository Structure

```
dss_project/
├── demo.sh                      # Main demo script (cloud/stream/results/perf/analyze)
├── dashboard.sh                 # Launch the live web dashboard
├── requirements.txt             # Python dependencies
│
├── src/
│   ├── cloud_pipeline.py        # Self-contained Dataproc job, all 5 stages
│   ├── etl_enrichment.py        # Stage 1 standalone
│   ├── benchmark_job.py         # Stages 2 and 3 standalone
│   ├── attribution.py           # Stage 4 standalone
│   ├── anomaly_detection.py     # Stage 5 standalone
│   │
│   ├── cloud_benchmark.sh       # Submit cloud_pipeline.py to Dataproc
│   ├── run_scaling_study.sh     # Run all 9 benchmark combinations
│   │
│   ├── cloud_broadcast_join.py  # Optimization: broadcast join experiment
│   ├── cloud_partition_analysis.py  # Optimization: partition pruning experiment
│   ├── cloud_skew_analysis.py   # Optimization: data skew and salting experiment
│   │
│   ├── streaming_job.py         # Spark Structured Streaming, real-time anomaly detection
│   ├── stream_producer.py       # Synthetic event producer for streaming demo
│   │
│   ├── dashboard_charts.py      # Generate PNG analytics charts from GCS parquet
│   ├── generate_perf_charts.py  # Generate performance and scaling charts from benchmark logs
│   ├── analyze_gcs_results.py   # Download GCS results and print summary tables
│   │
│   └── dashboard/               # Flask live demo dashboard
│       ├── app.py               # Backend: cluster API, job submission, chart serving
│       └── templates/index.html # Frontend: nodes, live log, VM metrics, analytics
│
├── results/
│   ├── benchmark_summary.csv    # All 9 scaling runs
│   ├── cloud_*w_*.log           # Per-run Dataproc logs
│   ├── broadcast_join_results.log
│   ├── partition_analysis_results.log
│   ├── skew_analysis_results.log
│   └── charts/                  # PNG charts for all experiments
│
└── screenshots/                 # Live demo screenshots
```

---

## Quick Start

Click the Codespaces badge at the top. The environment builds automatically in about 2 minutes with Python 3.11, Java 17, and all required packages installed.

---

## Running the Demo

### Option A: Live Web Dashboard

**Step 1:** Install dependencies

```bash
pip install -r requirements.txt
```

**Step 2:** Launch the dashboard

```bash
bash dashboard.sh
```

Then open http://localhost:5000 in your browser. The dashboard will show the Dataproc cluster nodes and you can run the full pipeline from there.

What the dashboard provides:

1. Shows all 6 Dataproc VMs (1 master and 5 workers) with live status
2. Click **Run Full Pipeline** to start the cluster if needed, submit the job, and stream live logs
3. Watch the 5 pipeline stages complete with timing per stage
4. After the job finishes, click **Generate Charts** to produce analytics charts
5. Switch between the Funnel, Attribution, and Anomalies tabs to explore results
6. The VM Metrics tab shows real-time CPU utilization and disk throughput
7. The Kill VM button on any worker lets you demonstrate Spark fault tolerance

### Option B: Command Line

```bash
# Authenticate with GCP (one time only)
gcloud auth login

bash demo.sh cloud      # Submit live pipeline to GCP Dataproc, 5 workers, 1GB, about 4 minutes
bash demo.sh stream     # Real-time streaming anomaly detection, runs locally, no GCP needed, about 60 seconds
bash demo.sh results    # Print pre-computed benchmark table and optimization results
bash demo.sh perf       # Regenerate all performance charts from saved logs
bash demo.sh analyze    # Download GCS output and generate analytics charts locally
```

The GCP project (`project-a0b2f7d8-d4bf-4557-923`) and bucket (`gs://dss-project-dax`) are pre-configured. No environment variables are needed.

---

## Fault Tolerance Demo

The dashboard has a Kill VM button on each worker node. To demonstrate fault tolerance during a live pipeline run:

1. Click Kill VM on `w-0` to shut down the VM mid-computation
2. Watch the live log show the lost executor and Spark detecting the failure
3. Spark automatically reassigns tasks to the remaining workers
4. The pipeline completes successfully, taking slightly longer
5. Click Start on `w-0` to bring it back online

Screenshots of this in action are in `screenshots/`.

---

## Real-Time Streaming Demo

```bash
bash demo.sh stream
```

The `stream_producer.py` script drops synthetic CSV batches every 2 seconds into a watch directory. The `streaming_job.py` script uses Spark Structured Streaming to process each micro-batch, detecting SPIKE anomalies when purchase counts exceed a threshold. A spike is injected every 5th batch automatically. The demo runs for 60 seconds and exits cleanly.

---

## GCP Configuration

Everything is pre-configured and requires no manual setup.

| Config | Value |
|---|---|
| GCP Project | `project-a0b2f7d8-d4bf-4557-923` |
| GCS Bucket | `gs://dss-project-dax` |
| Input data | `gs://dss-project-dax/data/full/2019-Oct.csv` |
| Pipeline scripts | `gs://dss-project-dax/scripts/` |
| Demo cluster | `dss-demo-5w-1gb` (1 master and 5 workers, n2-standard-2) |
| Region | Auto-selected from 9 fallback regions |

The cluster lifecycle is fully automated. If the cluster is not found it gets created with region and machine-type fallback across e2, n2, and n1 across 9 regions. If the cluster is stopped it starts back up, which is faster than recreating it. If it is already running it gets reused immediately. After the job completes the cluster is stopped but kept for the next run to avoid recreation time.

---

## Running the Full Scaling Study

```bash
bash src/run_scaling_study.sh
```

Runs all 9 combinations (2, 4, and 5 workers with 1, 5, and 10 GB) sequentially. Results are saved to `results/` and charts are generated automatically on completion.

---

## Dataset

**REES46 eCommerce behavior data** from Kaggle, October 2019  
42,448,764 events, 5.3 GB  
Columns: `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`

Synthesized fields using deterministic assignment via `crc32(user_session) % 100`:

- `device`: mobile 55%, desktop 30%, tablet 15%
- `referrer`: direct 40%, google 25%, facebook 15%, instagram 8%, email 7%, affiliate 5%
