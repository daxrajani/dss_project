# Scalable E-commerce Analytics Pipeline with PySpark and GCP Dataproc

**Authors:** Dax Rajani (40294118) and Harsh Ahuja (40301748)  
**Course:** COEN 6731 Distributed Software Systems, Concordia University, Winter 2026

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/daxrajani/dss_project)

This project builds a distributed data processing pipeline using Apache Spark on Google Cloud Dataproc. It processes 42 million e-commerce events through five analytical stages and includes a live web dashboard for running and monitoring the pipeline in real time. The GCP project, bucket, and cluster are already configured. No manual cloud setup is needed.

> **All commands in this README are meant to be run inside the Codespaces terminal.** Open the project in Codespaces using the badge above, wait for it to finish building, then open the terminal from the menu (Terminal > New Terminal) and run the commands from there. Every session requires running `source ~/google-cloud-sdk/path.bash.inc` first to load the Google Cloud SDK into the terminal before any `gcloud` or `bash demo.sh cloud` commands will work.

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

The pipeline processes the REES46 e-commerce dataset (42M+ rows, 5.3 GB) through five analytical stages on GCP Dataproc. It includes a live web dashboard for cluster monitoring, pipeline execution, VM metrics, and analytics visualization.

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
| 3 | **Funnel Analysis** | View to Cart to Purchase conversion rates by category, device, and referrer at daily and weekly granularity |
| 4 | **Last-Touch Attribution** | For each order, finds the most recent non-direct referrer session within a 24 hour lookback window using a temporal join |
| 5 | **Anomaly Detection** | 7-day rolling z-score baseline that flags SPIKE and DROP days with text explanations, computed per day and per category |

All five stages are contained in `src/cloud_pipeline.py` as a single self-contained Dataproc job.

---

## Running the Demo

There are two ways to run this project. The dashboard option gives a full live experience with the GCP cluster. The command line option includes demos that run entirely locally with no cloud access needed.

---

### Option A: Live Web Dashboard (Recommended)

**Step 1:** Click the Codespaces badge at the top of this page to open the project in GitHub Codespaces. The environment takes about 5 to 8 minutes to build on first launch. It installs Python, Java, the Google Cloud SDK, and all required packages automatically.

**Step 2:** Once the Codespace is ready, open the terminal and run the following command to load the Google Cloud SDK into your current session:

```bash
source ~/google-cloud-sdk/path.bash.inc
```

**Step 3:** Start the dashboard:

```bash
bash dashboard.sh
```

**Step 4:** Open the forwarded port 5000 in your browser when Codespaces prompts you, or go to the Ports tab and click the link next to port 5000.

The dashboard will show all 6 Dataproc VMs (1 master and 5 workers) with live status. From there you can:

1. Click **Run Full Pipeline** to submit the job and watch live logs stream in
2. See each of the 5 stages complete with timing
3. Click **Generate Charts** after the job finishes to produce analytics charts
4. Switch between the Funnel, Attribution, and Anomalies tabs to explore results
5. Open the VM Metrics tab to see real-time CPU and disk throughput across all VMs
6. Click **Kill VM** on any worker during a run to demonstrate Spark fault tolerance

---

### Option B: Command Line (No Cloud Setup Needed for Most Commands)

**Step 1:** Open the project in Codespaces using the badge above, or clone the repo locally and install dependencies:

```bash
pip install -r requirements.txt
```

**Step 2:** Load the Google Cloud SDK into your terminal session:

```bash
source ~/google-cloud-sdk/path.bash.inc
```

**Step 3:** Run whichever demo you need:

```bash
bash demo.sh results
```
Prints the full benchmark summary table and optimization experiment results. No GCP access needed. Runs instantly from pre-computed data.

```bash
bash demo.sh stream
```
Runs a real-time streaming anomaly detection demo entirely on your local machine using Spark Structured Streaming. No GCP access needed. Runs for about 60 seconds.

```bash
bash demo.sh perf
```
Regenerates all 6 performance and scaling charts from the saved benchmark logs. No GCP access needed.

```bash
bash demo.sh cloud
```
Submits the full pipeline to GCP Dataproc (5 workers, 1GB sample, about 4 minutes). Requires GCP access which is pre-configured for this project.

```bash
bash demo.sh analyze
```
Downloads the latest pipeline output from GCS and generates analytics charts locally.

---

## Fault Tolerance Demo

During a live pipeline run on the dashboard, click **Kill VM** on any worker node. The VM shuts down mid-computation and you can watch Spark detect the failure in the live log, reassign tasks to the remaining workers, and complete the pipeline successfully. Clicking **Start** on the same worker brings it back online. Screenshots of this in action are in `screenshots/`.

---

## Real-Time Streaming Demo

```bash
source ~/google-cloud-sdk/path.bash.inc
bash demo.sh stream
```

The `stream_producer.py` script writes synthetic CSV event batches every 2 seconds into a watch directory. The `streaming_job.py` script reads each batch using Spark Structured Streaming, computes purchase counts per category per 5-minute window, and flags SPIKE anomalies when counts exceed a threshold. A spike is injected automatically every 5th batch. The demo runs for 60 seconds and exits cleanly with no cleanup needed.

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

Going from 2 to 5 workers gives a 2.0 to 2.6x speedup. Scaling from 1 GB to 10 GB on 5 workers adds only 2.6x more time for 10x more data, showing strong sublinear scaling.

---

## Optimization Experiments

| Experiment | Baseline | Optimized | Speedup |
|---|---|---|---|
| Broadcast join vs shuffle join | 11.33s | 7.15s | **1.58x** |
| Partition pruning, single day | 4.25s | 1.37s | **3.11x** |
| Partition pruning, 7-day range | 3.09s | 1.78s | **1.73x** |
| Skew salting | 34.6s | 79.1s | 0.44x (data is already uniform so salting adds overhead) |

Pre-computed results are in `results/`. Charts are in `results/charts/`.

---

## Repository Structure

```
dss_project/
├── demo.sh                          # Main demo script (cloud/stream/results/perf/analyze)
├── dashboard.sh                     # Launch the live web dashboard
├── requirements.txt                 # Python dependencies
│
├── src/
│   ├── cloud_pipeline.py            # Self-contained Dataproc job, all 5 stages
│   ├── etl_enrichment.py            # Stage 1 standalone
│   ├── benchmark_job.py             # Stages 2 and 3 standalone
│   ├── attribution.py               # Stage 4 standalone
│   ├── anomaly_detection.py         # Stage 5 standalone
│   │
│   ├── cloud_benchmark.sh           # Submit cloud_pipeline.py to Dataproc
│   ├── run_scaling_study.sh         # Run all 9 benchmark combinations
│   │
│   ├── cloud_broadcast_join.py      # Broadcast join optimization experiment
│   ├── cloud_partition_analysis.py  # Partition pruning optimization experiment
│   ├── cloud_skew_analysis.py       # Data skew and salting experiment
│   │
│   ├── streaming_job.py             # Spark Structured Streaming, real-time anomaly detection
│   ├── stream_producer.py           # Synthetic event producer for streaming demo
│   │
│   ├── dashboard_charts.py          # Generate analytics charts from GCS parquet
│   ├── generate_perf_charts.py      # Generate performance and scaling charts from logs
│   ├── analyze_gcs_results.py       # Download GCS results and print summary tables
│   │
│   └── dashboard/
│       ├── app.py                   # Flask backend: cluster API, job submission, chart serving
│       └── templates/index.html     # Frontend: nodes, live log, VM metrics, analytics tabs
│
├── results/
│   ├── benchmark_summary.csv        # All 9 scaling runs
│   ├── cloud_*w_*.log               # Per-run Dataproc logs
│   ├── broadcast_join_results.log
│   ├── partition_analysis_results.log
│   ├── skew_analysis_results.log
│   └── charts/                      # PNG charts for all experiments
│
└── screenshots/                     # Live demo screenshots
```

---

## Running the Full Scaling Study

```bash
source ~/google-cloud-sdk/path.bash.inc
bash src/run_scaling_study.sh
```

Runs all 9 combinations (2, 4, and 5 workers with 1, 5, and 10 GB) sequentially. Results are saved to `results/` and charts are generated automatically on completion.

---

## GCP Configuration

Everything is pre-configured. No changes are needed to run the demo.

| Setting | Value |
|---|---|
| GCP Project | `project-a0b2f7d8-d4bf-4557-923` |
| GCS Bucket | `gs://dss-project-dax` |
| Input data | `gs://dss-project-dax/data/full/2019-Oct.csv` |
| Pipeline scripts | `gs://dss-project-dax/scripts/` |
| Demo cluster | `dss-demo-5w-1gb` (1 master and 5 workers, n2-standard-2) |
| Region | Auto-selected across 9 fallback regions |

The cluster lifecycle is fully automated. If the cluster does not exist it gets created with automatic region and machine type fallback. If it is stopped it gets started. If it is already running it gets reused. After each job the cluster is stopped but not deleted so the next run starts faster.

---

## Dataset

**REES46 eCommerce behavior data** from Kaggle, October 2019  
42,448,764 events, 5.3 GB  
Columns: `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`

Two fields are synthesized deterministically using `crc32(user_session) % 100` since they are not present in the original dataset:

- `device`: mobile 55%, desktop 30%, tablet 15%
- `referrer`: direct 40%, google 25%, facebook 15%, instagram 8%, email 7%, affiliate 5%
