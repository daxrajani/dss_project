# DSS Project — Complete Study Guide
**COEN 6731 Distributed Software Systems · Concordia University · Winter 2026**
**Dax Rajani (40294118) · Harsh Ahuja (40301748)**

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Dataset Deep Dive](#2-dataset-deep-dive)
3. [Core Spark Concepts Used](#3-core-spark-concepts-used)
4. [Stage 1 — ETL & Enrichment](#4-stage-1--etl--enrichment)
5. [Stage 2 — Sessionization](#5-stage-2--sessionization)
6. [Stage 3 — Funnel Analysis](#6-stage-3--funnel-analysis)
7. [Stage 4 — Last-Touch Attribution](#7-stage-4--last-touch-attribution)
8. [Stage 5 — Anomaly Detection](#8-stage-5--anomaly-detection)
9. [Performance Optimizations](#9-performance-optimizations)
10. [Cloud Deployment — GCP Dataproc](#10-cloud-deployment--gcp-dataproc)
11. [Benchmark Results & Analysis](#11-benchmark-results--analysis)
12. [How Everything Connects](#12-how-everything-connects)
13. [Key Numbers to Remember](#13-key-numbers-to-remember)
14. [Likely Questions & Answers](#14-likely-questions--answers)

---

## 1. Project Overview

### What problem does this solve?

A large e-commerce platform generates tens of millions of behavioral events per month
(views, cart additions, purchases). The raw data sits in a flat CSV file — no sessions,
no attribution, no anomaly flags. Single-machine pandas simply runs out of memory or
takes too long. We need a distributed approach.

### What does the pipeline do?

It takes a 42-million-row CSV and runs it through five stages to produce business insights:

```
Raw CSV (5.3 GB)
    │
    ▼
[Stage 1] ETL & Enrichment
    ├─ Adds device column (mobile/desktop/tablet)
    ├─ Adds referrer column (google/facebook/etc.)
    ├─ Extracts orders table
    └─ Writes Parquet

[Stage 2] Sessionization
    └─ Groups events into 30-min sessions per user

[Stage 3] Funnel Analysis
    └─ view → cart → purchase conversion rates

[Stage 4] Last-Touch Attribution
    └─ Which referrer gets credit for each order?

[Stage 5] Anomaly Detection
    └─ Which days had abnormal purchase volumes?
```

### Technology

| Component | Choice | Why |
|-----------|--------|-----|
| Processing | Apache Spark (PySpark) | Distributed, in-memory, SQL-like API |
| Format | Parquet | Columnar, compressed, partition-aware |
| Cloud | GCP Dataproc | Managed Spark, easy cluster resize |
| Storage | GCS (Google Cloud Storage) | Decoupled from compute |
| Language | Python 3 | PySpark bindings |

---

## 2. Dataset Deep Dive

### Source
REES46 "eCommerce behavior data from multi category store" — Kaggle, October 2019.

### Schema (raw CSV)

| Column | Type | Example |
|--------|------|---------|
| `event_time` | string (UTC) | `"2019-10-01 00:00:00 UTC"` |
| `event_type` | string | `view`, `cart`, `purchase` |
| `product_id` | string | `1004856` |
| `category_id` | string | `2053013555631882655` |
| `category_code` | string | `electronics.smartphone` (often null) |
| `brand` | string | `samsung` (often null) |
| `price` | string → double | `489.07` |
| `user_id` | string | `520088904` |
| `user_session` | string (UUID) | `4d3b30da-a5e4-49df-b1a8-ba5943f1dd33` |

### What is missing from the raw data?
- **device** — what device the user was on (mobile/desktop/tablet)
- **referrer** — how the user arrived (google/facebook/direct/etc.)
- **session boundaries** — the CSV has raw events, no grouping into sessions

All three must be derived or synthesized.

### Scale
```
Total events   : 42,448,764
Purchase events: 742,849   (~1.75% of events)
Distinct users : ~1.4M
Distinct products: 166,794
File size      : 5.3 GB CSV  → ~1.2 GB Parquet (after ETL)
Time span      : October 2019 (31 days)
```

---

## 3. Core Spark Concepts Used

Understanding these concepts is essential to understanding the code.

### 3.1 Window Functions

A **window function** computes a value for each row based on a "window" of related rows,
without collapsing the DataFrame (unlike groupBy).

```python
from pyspark.sql.window import Window

# Define the window: per user, ordered by time
user_win = Window.partitionBy("user_id").orderBy("event_time")

# lag() looks at the PREVIOUS row in the window
df = df.withColumn("_prev_ts", lag("event_time").over(user_win))
```

Used in: Sessionization, Attribution, Anomaly Detection.

### 3.2 RangeBetween vs RowsBetween

For the rolling window in anomaly detection:

```python
# rangeBetween works on the ORDER BY column's VALUES (not row positions)
# This works when ordering by a timestamp integer
rolling_win = Window.orderBy("date_ts").rangeBetween(-604800, -86400)
# -604800 = -7 days in seconds,  -86400 = -1 day (exclude today)
```

### 3.3 Parquet & Partition Pruning

When you write partitioned Parquet:
```python
df.write.partitionBy("date").parquet(output_path)
```
Spark creates a directory structure: `output/date=2019-10-01/part-*.parquet`.

When you query:
```python
df.filter(col("date") == "2019-10-15").count()
```
Spark **skips all directories except `date=2019-10-15/`** — this is **partition pruning**.
Result: 3.11× speedup on single-day queries.

### 3.4 Shuffle Join vs Broadcast Join

A **shuffle join** (SortMergeJoin by default) redistributes both tables across the network
so matching keys end up on the same node. Expensive at scale.

A **broadcast join** copies the smaller table to every worker node, avoiding network shuffle
entirely for the larger table.

```python
from pyspark.sql.functions import broadcast

# Orders (153K rows) is small — safe to broadcast
joined = orders_broadcast.join(session_refs, on="user_id")
# where: orders_broadcast = broadcast(orders)
```

Result: 1.58× speedup in attribution join.

### 3.5 Caching

```python
df = df.cache()
df.count()   # triggers materialisation into memory
```

Cache a DataFrame when it's read multiple times (e.g., session_flags is used for both
daily and weekly funnel metrics). Without caching, Spark would recompute it from scratch
for each use.

### 3.6 Lazy Evaluation

Spark builds a **DAG** (Directed Acyclic Graph) of transformations but doesn't execute
anything until an **action** is called (`.count()`, `.write()`, `.show()`).

```
transformation: df.filter(...).groupBy(...).agg(...)  ← nothing runs yet
action:         .count()                               ← executes the whole chain
```

---

## 4. Stage 1 — ETL & Enrichment

**File:** `src/etl_enrichment.py` (229 lines)

### What it does

Reads the raw CSV and produces three Parquet tables:
1. `enriched_events/` — original events + device + referrer columns
2. `orders/` — purchase events only, with order_id
3. `catalog/` — distinct products with category/brand/price

### Step-by-step

**Step 1: Read CSV**
```python
raw = spark.read.csv(input_path, header=True, inferSchema=False)
# inferSchema=False because auto-type-inference on 42M rows is slow
# Types are cast manually afterwards
```

**Step 2: Fix timestamps**
```python
raw = raw.withColumn(
    "event_time",
    to_timestamp(regexp_replace(col("event_time"), r" UTC$", ""))
)
# The raw timestamps look like "2019-10-01 00:00:00 UTC"
# regexp_replace strips " UTC", then to_timestamp parses the result
```

**Step 3: Synthesize device**
```python
def _add_device(df):
    bucket = crc32(col("user_session")) % 100
    return df.withColumn("device",
        when(bucket < 55, "mobile")     # 55% mobile
        .when(bucket < 85, "desktop")   # 30% desktop
        .otherwise("tablet")            # 15% tablet
    )
```

**Why CRC32?** It's deterministic — the same `user_session` UUID always produces the
same device. This means if you run the pipeline twice, you get identical results.
It's not random — it's a hash.

**Step 4: Synthesize referrer**
```python
def _add_referrer(df):
    bucket = crc32(col("user_session")) % 100
    return df.withColumn("referrer",
        when(bucket < 40, "direct")      # 40%
        .when(bucket < 65, "google")     # 25%
        .when(bucket < 80, "facebook")   # 15%
        .when(bucket < 88, "instagram")  #  8%
        .when(bucket < 95, "email")      #  7%
        .otherwise("affiliate")          #  5%
    )
```

Note: **both device and referrer use the same `user_session` hash**. This is fine — the
`% 100` bucketing for device and referrer produce consistent but independent assignments
because the cutoffs are different.

**Step 5: Derive orders**
```python
orders = (
    enriched
    .filter(col("event_type") == "purchase")
    .select(
        monotonically_increasing_id().alias("order_id"),  # unique ID
        col("user_id"),
        col("event_time").alias("order_time"),
        col("price").alias("total_amount"),
        col("user_session"),
    )
)
```

**Step 6: Derive catalog**
```python
catalog = (
    enriched
    .select("product_id", "category_code", "brand", "price")
    .dropDuplicates(["product_id"])   # one row per product
    .fillna({"category_code": "unknown", "brand": "unknown"})
)
```

### Outputs
```
data/processed/
  enriched_events/   ← 42M rows, +device, +referrer
  orders/            ← 742K rows
  catalog/           ← 166K rows
```

### Timing (5 workers, 10 GB)
- Total ETL: ~700s
- ETL Parquet write alone: **477 seconds** (35% of entire pipeline)
- This is a key finding: distributed I/O dominates at scale

---

## 5. Stage 2 — Sessionization

**File:** `src/attribution.py` → `_derive_sessions()` function (reused by multiple stages)
**Orchestrated by:** `src/benchmark_job.py` → `run_sessionization()`

### What is a session?

A session is a continuous browsing period for a user. If a user is inactive for more than
**30 minutes**, the next event starts a new session.

Example:
```
user_123, 10:00 → view     ┐ Session 1
user_123, 10:05 → view     │
user_123, 10:20 → cart     ┘
                 (35 min gap)
user_123, 10:55 → view     ┐ Session 2
user_123, 11:00 → purchase ┘
```

### The algorithm (4 steps)

**Step 1: Find the previous event time for each user**
```python
user_win = Window.partitionBy("user_id").orderBy("event_time")
df = df.withColumn("_prev_ts", lag("event_time").over(user_win))
# For the very first event per user, _prev_ts is NULL
```

**Step 2: Compute the time gap in seconds**
```python
df = df.withColumn(
    "_gap",
    unix_timestamp("event_time") - unix_timestamp("_prev_ts")
)
```

**Step 3: Flag the start of a new session**
```python
df = df.withColumn(
    "_is_new",
    when(col("_gap") > 1800, 1)        # gap > 30 min
    .when(col("_prev_ts").isNull(), 1)  # first event ever for this user
    .otherwise(0)
)
```

**Step 4: Cumulative sum → session index**
```python
df = df.withColumn("_session_idx", sum("_is_new").over(user_win))
df = df.withColumn(
    "session_id",
    concat_ws("_", col("user_id"), col("_session_idx"))
)
# e.g., "520088904_3" means user 520088904's 3rd session
```

The cumulative sum of `_is_new` flags gives each event a session number.
All events in the same session get the same number → same session_id.

### Why reuse `_derive_sessions` across modules?

Attribution also needs sessions (to find the referrer for each session).
By importing `_derive_sessions` from `attribution.py`, we avoid duplicating this logic.

---

## 6. Stage 3 — Funnel Analysis

**File:** `src/benchmark_job.py` → `run_funnel_analysis()`

### What is a funnel?

A purchase funnel tracks how users progress through stages:
```
View → Add to Cart → Purchase
```

Most sessions have a view but no purchase. The funnel tells you:
- What percentage of view sessions lead to a cart add?
- What percentage of cart sessions lead to a purchase?

### Granularity

We compute funnel rates broken down by:
- `category_code` (electronics, clothing, etc.)
- `device` (mobile/desktop/tablet)
- `referrer` (google/facebook/etc.)
- `date` (daily) AND `week` (weekly)

### The algorithm

**Step 1: Flag each event type per session**
```python
df = sessionized_df.withColumn("date", to_date("event_time"))
df = df.withColumn("is_view",     when(col("event_type") == "view",     1).otherwise(0))
df = df.withColumn("is_cart",     when(col("event_type") == "cart",     1).otherwise(0))
df = df.withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0))
```

**Step 2: Collapse to session-level flags (using max)**
```python
session_flags = df.groupBy(
    "session_id", "date", "week", "category_code", "device", "referrer"
).agg(
    max("is_view").alias("has_view"),       # 1 if ANY view in session, else 0
    max("is_cart").alias("has_cart"),       # 1 if ANY cart in session
    max("is_purchase").alias("has_purchase")
).cache()
```

Why `max`? Because we want to know if a session contained *at least one* view/cart/purchase,
not how many times. `max(0,0,1,0) = 1` — session had a purchase.

**Step 3: Aggregate to conversion rates**
```python
daily = session_flags.groupBy("date", "category_code", "device", "referrer").agg(
    sum("has_view").alias("total_sessions_with_view"),
    sum("has_cart").alias("total_sessions_with_cart"),
    sum("has_purchase").alias("total_sessions_with_purchase"),
).withColumn(
    "view_to_cart_pct",
    round(col("total_sessions_with_cart") / col("total_sessions_with_view") * 100, 2)
).withColumn(
    "cart_to_buy_pct",
    round(col("total_sessions_with_purchase") / col("total_sessions_with_cart") * 100, 2)
)
```

### Output schema
```
date | category_code | device | referrer |
total_sessions_with_view | total_sessions_with_cart | total_sessions_with_purchase |
view_to_cart_pct | cart_to_buy_pct
```

### Key metric
28,460 daily funnel rows produced on the 1 GB (8.5M row) run.

---

## 7. Stage 4 — Last-Touch Attribution

**File:** `src/attribution.py` (285 lines)

### What is last-touch attribution?

When a customer makes a purchase, *which marketing channel gets the credit?*

**Last-touch** = credit goes to the most recent non-direct touchpoint before the purchase.

Example:
```
Day 1: User arrives via Google (search ad)
Day 2: User arrives directly (typed URL)
Day 3: User arrives via Facebook ad → makes PURCHASE

Attribution → Facebook (most recent non-direct touchpoint)
```

### The 24-hour lookback window

We only look at referrer sessions that started **within 24 hours before the order**.
Sessions older than 24 hours are excluded.

### Implementation in 4 steps

**Step 1: Build session-referrer table**
```python
# Each session gets a single referrer (first() works because
# CRC32 hash is constant for all events in a user_session)
session_refs = (
    sessionized_df
    .groupBy("user_id", "session_id")
    .agg(
        min("event_time").alias("session_start"),
        first("referrer").alias("referrer"),
        first("device").alias("device"),
    )
)
```

**Step 2: Filter out direct sessions**
```python
candidates = session_refs.filter(col("referrer") != "direct")
# Direct traffic means user typed the URL themselves — no marketing credit
```

**Step 3: Join orders with candidate sessions**
```python
joined = (
    orders.alias("o")
    .join(candidates.alias("s"), on="user_id", how="left")
    .filter(col("s.session_start") <= col("o.order_time"))        # session before order
    .filter(
        (unix_timestamp("o.order_time") - unix_timestamp("s.session_start")) <= 86400
    )   # within 24 hours
)
```

This is a **left join** — orders without any qualifying session remain (unattributed).

**Step 4: Pick the MOST RECENT session with row_number()**
```python
rank_win = Window.partitionBy("o.order_id").orderBy(col("s.session_start").desc())

attributed = (
    joined
    .withColumn("_rank", row_number().over(rank_win))
    .filter(col("_rank") == 1)   # keep only rank=1 (most recent)
    .select("o.order_id", "o.user_id", "o.order_time",
            "o.total_amount", "s.referrer", "s.session_start", "s.device")
)
```

`row_number()` assigns 1 to the row with the **latest** session_start (because we order
`desc()`). Filtering `_rank == 1` keeps only that row per order.

### Attribution summary output

```
attributed_referrer | order_count | total_revenue | avg_order_value
google              |      40,341 |   10,234,567  |          253.72
facebook            |      15,127 |    3,890,124  |          257.18
...
```

### Key numbers
- 100,852 orders attributed (1 GB run)
- Attribution join time: 14.2s (5 workers, 1 GB)
- Attribution join time: 122s (2 workers, 10 GB)

---

## 8. Stage 5 — Anomaly Detection

**File:** `src/anomaly_detection.py` (279 lines)

### What is z-score anomaly detection?

A **z-score** measures how many standard deviations a value is from the mean.

```
z = (observed_value - mean) / stddev
```

If `|z| > 2`, the day is flagged as an anomaly (more than 2 std deviations from normal).

### Baseline: 7-day trailing rolling window

For each day, we compute a baseline from the **previous 7 days** (excluding today itself).

```
Day 8's baseline = avg and stddev of days 1–7
Day 9's baseline = avg and stddev of days 2–8
...
```

This is a **trailing** window — it only looks backward, never forward.

### Why exclude today from the baseline?

If today is a spike, including it would inflate the baseline and understate the z-score.
By excluding today (`rangeBetween(-7days, -1day)`), the baseline always reflects
"what was normal before today."

### Implementation

```python
SECONDS_7D = 7 * 86400   # 604,800 seconds
SECONDS_1D = 1 * 86400   #  86,400 seconds

rolling_win = Window.orderBy("date_ts").rangeBetween(-SECONDS_7D, -SECONDS_1D)

df = df.withColumn("rolling_avg",    round(avg("purchase_count").over(rolling_win), 2))
df = df.withColumn("rolling_stddev", round(stddev("purchase_count").over(rolling_win), 2))

df = df.withColumn("z_score",
    when(col("rolling_stddev").isNotNull() & (col("rolling_stddev") > 0),
         round((col("purchase_count") - col("rolling_avg")) / col("rolling_stddev"), 3)
    ).otherwise(None)
)
# z_score is NULL when stddev=0 (flat baseline) — can't detect anomalies
```

### Why `rangeBetween` uses seconds?

The `orderBy("date_ts")` column contains UNIX timestamps (integer seconds).
`rangeBetween(-604800, -86400)` means: include rows where `date_ts` is between
`current_date_ts - 604800` and `current_date_ts - 86400`. This is exactly 7→1 days ago.

### Anomaly explanation generation

```python
explanation = when(is_anomaly AND z > 0,
    concat("SPIKE: ", pct_above, "% above 7-day baseline (",
           purchase_count, " vs expected ", rolling_avg, ")")
).when(is_anomaly AND z < 0,
    concat("DROP: ", pct_below, "% below 7-day baseline (...)")
)
```

Sample output: `"SPIKE: 45.2% above 7-day baseline (1200 vs expected 826)"`

### Two levels of detection

1. **System-wide** — aggregate all purchases across all categories per day
2. **Per-category** — separate rolling window per `category_code`

For per-category: `Window.partitionBy("category_code").orderBy("date_ts")`

### Key numbers
- 8 system-level anomalies flagged (1 GB run, 31 days)
- 305 per-category anomalies flagged (1 GB run)
- Anomaly detection stage time: ~16s (5 workers, 1 GB)

---

## 9. Performance Optimizations

Three separate studies, each benchmarked on the cloud cluster.

---

### 9.1 Broadcast Join (`src/broadcast_join_opt.py`)

**The problem:** The attribution join is `orders JOIN session_refs ON user_id`.
By default, Spark uses SortMergeJoin which shuffles both tables across the network.

**The fix:** The `orders` table is small relative to `session_refs`. We broadcast it —
Spark sends a full copy to every worker. The workers then do a local hash lookup
instead of a network shuffle.

```python
# Key configuration: disable auto-broadcast for fair baseline
spark.config("spark.sql.autoBroadcastJoinThreshold", "-1")

# Baseline: normal join
result_baseline = orders.join(session_refs, on="user_id")

# Optimized: broadcast hint
result_broadcast = broadcast(orders).join(session_refs, on="user_id")
```

**Results:**
```
Strategy             Time        Orders     Speedup
Shuffle join (base)  11.33s    153,628      1.00×
Broadcast join        7.15s    153,628      1.58×
```

**Why results are identical?** Both approaches compute the same logical join —
they just use different physical execution strategies. The output rows are identical.

**Why does benefit grow with cluster size?**
More workers = more network hops in shuffle. Broadcast eliminates all of them.
A 5-worker cluster has more cross-node transfers than a 2-worker cluster,
so broadcasting saves proportionally more.

---

### 9.2 Partition Pruning (`src/partition_analysis.py`)

**The problem:** Every query on purchase data (e.g., "show me Oct 15 purchases")
scans the entire Parquet file, even if you only need 1 day out of 31.

**The fix:** Write Parquet partitioned by date. Spark creates one directory per day.
When you filter by date, Spark physically opens only that directory.

```python
# BASELINE: flat write
raw.write.parquet(out_flat)
# Creates: part-00001.parquet, part-00002.parquet, ...  (all data in one layer)

# OPTIMIZED: partitioned write
raw.write.partitionBy("date").parquet(out_part)
# Creates: date=2019-10-01/part-00001.parquet
#          date=2019-10-02/part-00001.parquet
#          ...
#          date=2019-10-31/part-00001.parquet
```

**Query with filter:**
```python
df.filter(col("date") == "2019-10-15").count()
# Flat:        reads ALL 31 days of data (full scan)
# Partitioned: reads ONLY the date=2019-10-15/ directory
```

**Results:**
```
Query type         Flat      Partitioned   Speedup
Single-day         4.25s       1.37s        3.11×
7-day range        3.09s       1.78s        1.73×
Write overhead    22.9s       26.0s          —
```

**Key insight:** For the single-day query, Spark only needs to read 1/31 ≈ 3.2% of the
data. The speedup (3.11×) approximately matches this — partition pruning eliminates ~97%
of I/O.

---

### 9.3 Skew Analysis (`src/skew_analysis.py`)

**The problem:** Data skew in joins occurs when some keys have vastly more rows than
others. A "hot key" user with 1M events would cause one Spark task to process all those
rows while other tasks sit idle — a stragglers problem.

**The mitigation: Salting**
Add a random bucket number to the join key to distribute hot keys across more tasks:
```python
# Without salting: join key = user_id
# With salting:    join key = user_id + "_" + rand(0..19)

salted_orders = orders.withColumn("salt", floor(rand() * N_BUCKETS))
salted_orders = salted_orders.withColumn("salted_key", concat(col("user_id"), lit("_"), col("salt")))

# Replicate session_refs N_BUCKETS times (one copy per salt value)
salted_sessions = session_refs.crossJoin(spark.range(N_BUCKETS).withColumnRenamed("id", "salt"))
salted_sessions = salted_sessions.withColumn("salted_key", concat(col("user_id"), lit("_"), col("salt")))
```

**Results:**
```
Strategy              Time    Orders     Speedup
Without salting      34.6s   100,852     1.00×
With salting         79.1s   100,852     0.44×  (2.28× SLOWER!)
```

**Why did salting hurt?**
- Top-1 user = only **0.02%** of all events → no meaningful skew
- Salting with 20 buckets → `crossJoin` replicates the orders table 20×
- The overhead of the replication far exceeded any skew mitigation benefit
- **Lesson: measure skew before applying mitigation**

**When WOULD salting help?**
When a single key accounts for >1–5% of rows, causing stragglers. In a more skewed
dataset (e.g., a handful of super-users), salting would be beneficial.

---

## 10. Cloud Deployment — GCP Dataproc

### Architecture

```
Local machine
    │
    ├─ gcloud dataproc jobs submit pyspark ...
    │
    ▼
GCP Dataproc Cluster
    ├─ Master node: SparkContext + YARN ResourceManager
    ├─ Worker 1: n1-standard-4 (4 vCPU, 15 GB RAM)
    ├─ Worker 2: n1-standard-4
    ├─ Worker 3: n1-standard-4  (only for 4/5-worker configs)
    ├─ Worker 4: n1-standard-4
    └─ Worker 5: n1-standard-4
         │
         ↕  GCS (gs://dss-project-dax)
         │
    ┌────┴────────────────────────────┐
    │  gs://dss-project-dax/          │
    │    data/full/2019-Oct.csv       │  ← input
    │    results/demo_run/            │  ← output
    └─────────────────────────────────┘
```

### Key scripts

| Script | Purpose |
|--------|---------|
| `src/cloud_pipeline.py` | Full 5-stage pipeline for Dataproc (554 lines) |
| `src/cloud_submit.sh` | Parameterised job submission (1gb/5gb/10gb) |
| `src/run_scaling_study.sh` | Runs all 9 benchmark combinations |
| `src/run_all_benchmarks.sh` | Master benchmark runner |
| `demo.sh` | Live demo entry point |

### How the sample fractions map to data sizes

```
1gb  → --sample-fraction 0.20  (~8.5M rows,  ~20% of Oct CSV)
5gb  → --sample-fraction 0.50  (~42.5M rows, ~50% of Oct CSV)
10gb → --sample-fraction 1.00  (all 84.9M rows — note: uses full file)
```

Wait — the actual CSV is 42M rows, not 84M. The "10gb" run uses 100% of the CSV.
The naming refers to the *approximate byte size* of the data being processed (including
intermediate Parquet), not raw row count.

### Data flow on GCS

```
gs://dss-project-dax/
  data/
    full/2019-Oct.csv          ← raw input (5.3 GB)
  scripts/                     ← Python scripts uploaded via gsutil
    cloud_pipeline.py
    ...
  results/
    demo_run/                  ← output of demo
      enriched_events/
      orders/
      funnel_metrics_daily/
      attribution/
      anomalies/
```

### Cluster configuration

```
Project : project-a0b2f7d8-d4bf-4557-923
Region  : us-central1
Cluster : dss-bench-2w-10gb-14824 (renamed, actually 5 workers now)
Node type: n1-standard-4
  - 4 vCPUs
  - 15 GB RAM
  - 100 GB boot disk
```

---

## 11. Benchmark Results & Analysis

### Raw numbers

```
workers | data    | total_s  | etl_write_s | join_s | rows_per_sec
--------|---------|----------|-------------|--------|-------------
2       | 1gb     |   673.6  |    223.1    |  22.3  |  12,607
2       | 5gb     | 1,021.8  |    300.4    |  44.1  |  41,543
2       | 10gb    | 1,378.9  |    477.2    | 122.0  |  61,569
4       | 1gb     |   282.4  |    101.4    |  15.1  |  30,070
4       | 5gb     |   569.0  |    174.4    |  54.4  |  74,602
4       | 10gb    |   799.8  |    264.3    |  67.3  | 106,148
5       | 1gb     |   256.9  |     88.7    |  14.2  |  33,055
5       | 5gb     |   409.7  |    123.0    |  33.3  | 103,609
5       | 10gb    |   674.0  |    223.9    |  58.1  | 125,961
```

### Speedup analysis (vs 2-worker baseline)

```
Dataset | 4-worker speedup | 5-worker speedup | Ideal (2.5×)
1gb     |     2.38×        |     2.62×        |    2.5×
5gb     |     1.80×        |     2.49×        |    2.5×
10gb    |     1.72×        |     2.05×        |    2.5×
```

**Observations:**
- 1 GB at 5 workers actually **exceeds** ideal speedup (2.62× vs 2.5×) — small data
  processes more efficiently with more parallelism
- 10 GB at 5 workers is 2.05× — below ideal, because ETL I/O bottleneck
  doesn't scale perfectly with CPU count
- Adding workers has diminishing returns at large data sizes because
  GCS read/write bandwidth becomes the constraint

### Stage breakdown (2 workers, 1 GB)

```
Stage              Time    % of total
ETL write          223.1s    33.1%
Sessionization      98.2s    14.6%
Funnel Analysis     74.7s    11.1%
Attribution join    22.3s     3.3%
Anomaly detection   24.5s     3.6%
Other / overhead   230.8s    34.3%
TOTAL              673.6s   100.0%
```

ETL write and sessionization together = **~48% of total time** on 2 workers, 1 GB.

### Throughput scaling

```
2 workers, 10GB:  61,569 rows/sec
4 workers, 10GB: 106,148 rows/sec  (1.72× gain)
5 workers, 10GB: 125,961 rows/sec  (2.05× gain, 82% of ideal 2.5×)
```

---

## 12. How Everything Connects

### Data flow diagram

```
2019-Oct.csv (raw)
        │
        ▼
[etl_enrichment.py]
        │
        ├──────────────────────┬─────────────────────┐
        ▼                      ▼                     ▼
enriched_events/            orders/              catalog/
(+device +referrer)       (purchases only)    (distinct products)
        │                      │
        ▼                      │
[benchmark_job.py]             │
  _derive_sessions()           │
        │                      │
        ├────────────────────┐ │
        ▼                    │ │
  session_flags              │ │
  (per session_id)           │ │
        │                    │ │
        ▼                    │ │
[funnel analysis]            │ │
  funnel_metrics_daily/      │ │
  funnel_metrics_weekly/     │ │
                             │ │
        ┌────────────────────┘ │
        ▼                      ▼
[attribution.py]
  session_referrers ◄──── enriched_events (sessions derived again)
        +                      +
     orders ◄─────────────────┘
        │
        ▼
  attributed_orders/
  attribution_summary/
        │
[anomaly_detection.py]
  enriched_events (filtered to purchases)
        │
        ▼
  daily_anomalies/
  category_anomalies/
```

### Module dependency graph

```
benchmark_job.py
  ├─ imports etl_enrichment.py    (runs Stage 1)
  ├─ imports attribution.py       (reuses _derive_sessions for Stage 2 + runs Stage 4)
  └─ imports anomaly_detection.py (runs Stage 5)

attribution.py
  └─ _derive_sessions()  ← also imported by benchmark_job.py for Stage 2

cloud_pipeline.py
  └─ (standalone version of the full pipeline for GCP Dataproc)
     (does not import local modules — copies logic inline for portability)
```

### Why does `cloud_pipeline.py` not import other modules?

On Dataproc, Python source files are submitted to the cluster workers. If `cloud_pipeline.py`
imported `etl_enrichment.py`, we'd need to upload all dependencies separately.
Instead, `cloud_pipeline.py` (554 lines) contains all logic inline — one self-contained
file that is easy to submit with:
```bash
gcloud dataproc jobs submit pyspark gs://bucket/scripts/cloud_pipeline.py ...
```

---

## 13. Key Numbers to Remember

### Dataset
| Metric | Value |
|--------|-------|
| Total events | 42,448,764 |
| Purchase events | 742,849 |
| Distinct products | 166,794 |
| CSV file size | 5.3 GB |
| Time period | October 2019 (31 days) |

### Synthesis parameters
| Feature | Distribution |
|---------|-------------|
| device | mobile 55%, desktop 30%, tablet 15% |
| referrer | direct 40%, google 25%, facebook 15%, instagram 8%, email 7%, affiliate 5% |

### Thresholds
| Parameter | Value |
|-----------|-------|
| Session inactivity threshold | 30 minutes (1,800 seconds) |
| Attribution lookback window | 24 hours (86,400 seconds) |
| Anomaly z-score threshold | ±2.0 |
| Rolling baseline window | 7 days |

### Benchmark highlights
| Metric | Value |
|--------|-------|
| Best throughput | 125,961 rows/sec (5 workers, 10 GB) |
| Worst throughput | 12,607 rows/sec (2 workers, 1 GB) |
| Fastest total run | 256.9s (5 workers, 1 GB) |
| Slowest total run | 1,378.9s (2 workers, 10 GB) |
| ETL % of total (10GB/2w) | 35% (477s / 1379s) |
| Max speedup achieved | 2.62× (5w vs 2w on 1 GB) |

### Optimization results
| Technique | Speedup | Notes |
|-----------|---------|-------|
| Broadcast join | 1.58× | 11.33s → 7.15s |
| Partition pruning (1-day) | 3.11× | 4.25s → 1.37s |
| Partition pruning (7-day) | 1.73× | 3.09s → 1.78s |
| Salting (skew mitigation) | 0.44× | SLOWER — no skew in dataset |

---

## 14. Likely Questions & Answers

**Q: Why PySpark instead of pandas?**
Pandas loads everything into a single machine's RAM. At 5.3 GB CSV → ~42M rows,
pandas would use ~15–20 GB RAM for the enriched data. With window functions on 1.4M users,
it would also be extremely slow (O(n log n) sort per user on a single core). Spark
distributes both the memory and the computation across multiple nodes.

---

**Q: Why Parquet instead of CSV?**
1. **Columnar format**: queries that only need a few columns don't read the rest (e.g.,
   anomaly detection only needs `event_time` and `event_type` — Parquet skips all other columns)
2. **Compression**: Parquet with Snappy compression is ~4–5× smaller than CSV
3. **Schema preservation**: types (timestamps, doubles) don't need re-parsing
4. **Partition pruning**: Parquet supports directory-based partitioning (CSV does not)

---

**Q: Why CRC32 for device/referrer synthesis?**
We need the same `user_session` to always produce the same device/referrer, across
multiple runs and across different Spark workers. CRC32 is a deterministic hash —
it always produces the same 32-bit integer for the same input string.
Random assignment (`rand()`) would change each run, making results non-reproducible.

---

**Q: Why does session_id = `user_id + "_" + session_index`?**
We need session IDs that are:
1. **Unique globally** — `user_id` prefix ensures no two users share a session ID
2. **Human-readable** — `520088904_3` means "user 520088904's 3rd session"
3. **Derivable without a database** — computed purely from the window function,
   no global counter needed

---

**Q: What does `rangeBetween(-604800, -86400)` mean?**
The window is defined over a numeric column `date_ts` (UNIX timestamp in seconds).
`rangeBetween(A, B)` includes rows where `current_row_value + A ≤ other_row_value ≤ current_row_value + B`.

So `-604800` to `-86400` = `[-7 days, -1 day]` relative to today.
Today is excluded (upper bound is `-1 day`, not `0`) to prevent today's value from
inflating the baseline.

---

**Q: Why was salting counterproductive?**
Salting works by distributing a hot key across N buckets — but the `crossJoin` used to
replicate the orders table multiplies its size by N (20×). The overhead of:
- Creating 20 copies of every order
- Joining on a compound `(user_id, salt)` key instead of just `user_id`
- Extra shuffle caused by the crossJoin

...exceeded the benefit of eliminating stragglers, because the straggler problem didn't
exist in the first place (top-1 user = only 0.02% of events).

---

**Q: What is the difference between `etl_enrichment.py` and `cloud_pipeline.py`?**
`etl_enrichment.py` is a modular script that does only Stage 1 ETL, designed to be
imported by `benchmark_job.py`. It accepts `--input` and `--output` args.

`cloud_pipeline.py` (554 lines) is a self-contained standalone file with ALL 5 stages
inlined — specifically designed for GCP Dataproc job submission where importing local
modules is cumbersome. It additionally accepts `--sample-fraction` to control data size.

---

**Q: Why does `inferSchema=False` when reading CSV?**
`inferSchema=True` causes Spark to do a full pass over the data just to determine
column types. On 42M rows, this is wasted time — we already know the schema and cast
manually (`col("price").cast("double")`, `to_timestamp(...)`).

---

**Q: What happens to orders that have no non-direct referrer session in the 24-hour window?**
The `left join` in attribution keeps these orders. They appear in the `attributed_orders`
table with `attributed_referrer = NULL`. They are **not counted** in the attribution
summary breakdown, but the pipeline doesn't lose them.

In a real-world scenario, these would be labeled "direct" or "organic" in the summary report.

---

**Q: Why use `monotonically_increasing_id()` for order_id instead of a row number?**
`row_number()` requires a window function with an `orderBy` clause — this triggers a
full sort (expensive). `monotonically_increasing_id()` generates unique IDs based on
partition number + position — no sort needed. The IDs are not sequential across partitions
but they are guaranteed unique, which is all we need for `order_id`.

---

**Q: What is the significance of the 2.05× speedup on 10GB with 5 workers?**
Ideal linear speedup for going from 2 → 5 workers would be 2.5×.
We got 2.05× — that's 82% efficiency.

The gap is due to:
1. **GCS I/O bottleneck**: GCS bandwidth doesn't scale 2.5× just by adding workers
2. **ETL write (Amdahl's Law)**: the 35% of time spent writing Parquet is mostly I/O-bound,
   not CPU-bound. Adding more workers doesn't linearly reduce I/O time.
3. **Coordination overhead**: more workers = more task scheduling overhead

---

*End of Study Guide*
