"""
generate_all_charts.py — Generate ALL missing charts for the final presentation.

Categories:
  A. Optimization experiment charts (from log files — no Spark needed)
     1. Broadcast join comparison bar chart
     2. Partition pruning comparison bar chart
     3. Skew/salting comparison bar chart

  B. Analytics output charts (from Parquet — needs local pipeline run or existing data)
     4. Funnel drop-off bar chart (view → cart → purchase)
     5. Attribution revenue breakdown (bar chart by referrer channel)
     6. Anomaly timeline (daily purchases with flagged anomalies)

  C. Additional performance chart
     7. Parallel efficiency chart (speedup / N_workers)

Usage:
    python src/generate_all_charts.py [--run-pipeline]
        --run-pipeline : Run the full pipeline on Kaggle data first to produce analytics outputs
                         (skip if data/processed already has real data)
"""

import argparse
import os
import sys
import re

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHARTS_DIR = os.path.join(BASE_DIR, "results", "charts")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

os.makedirs(CHARTS_DIR, exist_ok=True)

# Common style
plt.rcParams.update({
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "figure.facecolor": "white",
})

COLORS = {
    "blue":   "#4C72B0",
    "orange": "#DD8452",
    "green":  "#55A868",
    "red":    "#C44E52",
    "purple": "#8172B2",
    "brown":  "#937860",
    "teal":   "#64B5CD",
    "grey":   "#999999",
}


# =====================================================================
# A. OPTIMIZATION EXPERIMENT CHARTS (from log files)
# =====================================================================

def chart_broadcast_join():
    """Bar chart comparing shuffle vs broadcast join."""
    log_path = os.path.join(RESULTS_DIR, "broadcast_join_results.log")
    with open(log_path) as f:
        text = f.read()

    shuffle_time = float(re.search(r"Shuffle join \(baseline\)\s+([\d.]+)s", text).group(1))
    broadcast_time = float(re.search(r"Broadcast join\s+([\d.]+)s", text).group(1))
    speedup = shuffle_time / broadcast_time

    fig, ax = plt.subplots(figsize=(6, 4.5))
    bars = ax.bar(
        ["Shuffle Join\n(SortMergeJoin)", "Broadcast Join\n(BroadcastHashJoin)"],
        [shuffle_time, broadcast_time],
        color=[COLORS["blue"], COLORS["green"]],
        width=0.5,
        edgecolor="white",
        linewidth=1.5,
    )

    # Value labels on bars
    for bar, val in zip(bars, [shuffle_time, broadcast_time]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.2,
                f"{val:.2f}s", ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax.set_ylabel("Execution Time (seconds)", fontsize=11)
    ax.set_title("Broadcast Join Optimization\n(Attribution Stage)", fontsize=13, fontweight="bold")
    ax.set_ylim(0, max(shuffle_time, broadcast_time) * 1.2)
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    # Speedup annotation
    ax.annotate(
        f"1.58x speedup",
        xy=(1, broadcast_time), xytext=(1.35, shuffle_time * 0.7),
        fontsize=10, fontweight="bold", color=COLORS["green"],
        arrowprops=dict(arrowstyle="->", color=COLORS["green"], lw=1.5),
    )

    fig.tight_layout()
    path = os.path.join(CHARTS_DIR, "chart_broadcast_join.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart -> {path}")


def chart_partition_pruning():
    """Grouped bar chart comparing flat vs partitioned Parquet queries."""
    log_path = os.path.join(RESULTS_DIR, "partition_analysis_results.log")
    with open(log_path) as f:
        text = f.read()

    # Parse times
    flat_single = float(re.search(r"Flat\s+:\s+([\d.]+)s.*?Single-day", text, re.DOTALL).group(1))
    part_single = float(re.search(r"Partitioned\s+:\s+([\d.]+)s.*?Single-day", text, re.DOTALL).group(1))

    flat_range = float(re.findall(r"Flat\s+:\s+([\d.]+)s", text)[1])
    part_range = float(re.findall(r"Partitioned\s+:\s+([\d.]+)s", text)[1])

    fig, ax = plt.subplots(figsize=(7, 4.5))
    x = np.arange(2)
    width = 0.3

    bars1 = ax.bar(x - width/2, [flat_single, flat_range], width,
                   label="Flat (unpartitioned)", color=COLORS["blue"], edgecolor="white")
    bars2 = ax.bar(x + width/2, [part_single, part_range], width,
                   label="Date-partitioned", color=COLORS["green"], edgecolor="white")

    # Value labels
    for bars in [bars1, bars2]:
        for bar in bars:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                    f"{bar.get_height():.2f}s", ha="center", va="bottom", fontsize=10)

    ax.set_xticks(x)
    ax.set_xticklabels(["Single-Day Filter\n(2019-10-15)", "7-Day Range Filter\n(Oct 10-17)"], fontsize=10)
    ax.set_ylabel("Query Time (seconds)", fontsize=11)
    ax.set_title("Parquet Partition Pruning\n(Date-Partitioned vs Flat)", fontsize=13, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    # Speedup annotations
    ax.text(0, max(flat_single, part_single) * 1.15, "3.11x faster",
            ha="center", fontsize=10, fontweight="bold", color=COLORS["green"])
    ax.text(1, max(flat_range, part_range) * 1.15, "1.73x faster",
            ha="center", fontsize=10, fontweight="bold", color=COLORS["green"])

    ax.set_ylim(0, flat_single * 1.4)
    fig.tight_layout()
    path = os.path.join(CHARTS_DIR, "chart_partition_pruning.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart -> {path}")


def chart_skew_salting():
    """Bar chart showing salting was counterproductive on low-skew data."""
    log_path = os.path.join(RESULTS_DIR, "skew_analysis_results.log")
    with open(log_path) as f:
        text = f.read()

    no_salt = float(re.search(r"no-salt time:\s+([\d.]+)s", text).group(1))
    salted = float(re.search(r"salted time:\s+([\d.]+)s", text).group(1))
    top1_share = re.search(r"Top-1 user share\s+([\d.]+)%", text).group(1)

    fig, ax = plt.subplots(figsize=(6, 4.5))
    bars = ax.bar(
        ["Without Salting\n(baseline)", "With Salting\n(20 buckets)"],
        [no_salt, salted],
        color=[COLORS["green"], COLORS["red"]],
        width=0.5,
        edgecolor="white",
        linewidth=1.5,
    )

    for bar, val in zip(bars, [no_salt, salted]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f"{val:.1f}s", ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax.set_ylabel("Execution Time (seconds)", fontsize=11)
    ax.set_title("Key Salting for Skew Mitigation\n(Attribution Stage — Low Skew Dataset)", fontsize=13, fontweight="bold")
    ax.set_ylim(0, salted * 1.25)
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    # Annotation explaining why salting was worse
    ax.annotate(
        f"0.44x — salting SLOWER\nTop-1 user = {top1_share}% of data\n(no real skew to mitigate)",
        xy=(1, salted), xytext=(0.5, salted * 1.05),
        fontsize=9, ha="center", color=COLORS["red"],
        bbox=dict(boxstyle="round,pad=0.3", facecolor="#FFE0E0", edgecolor=COLORS["red"], alpha=0.8),
    )

    fig.tight_layout()
    path = os.path.join(CHARTS_DIR, "chart_skew_salting.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart -> {path}")


# =====================================================================
# C. ADDITIONAL PERFORMANCE CHART
# =====================================================================

def chart_parallel_efficiency():
    """Parallel efficiency = speedup / (N_workers / baseline_workers)."""
    csv_path = os.path.join(RESULTS_DIR, "benchmark_summary.csv")
    df = pd.read_csv(csv_path)

    workers = [2, 4, 5]
    data_labels = {"1gb": "~1 GB (8.5M rows)", "5gb": "~5 GB (42M rows)", "10gb": "~10 GB (85M rows)"}
    data_colors = {"1gb": COLORS["blue"], "5gb": COLORS["orange"], "10gb": COLORS["green"]}

    fig, ax = plt.subplots(figsize=(7, 4.5))

    for dv, label in data_labels.items():
        subset = df[df["data_variant"] == dv].sort_values("workers")
        base_time = subset[subset["workers"] == 2]["total_s"].values
        if len(base_time) == 0:
            continue
        base_time = base_time[0]

        efficiencies = []
        for w in workers:
            row = subset[subset["workers"] == w]
            if len(row) == 0:
                efficiencies.append(None)
                continue
            t = row["total_s"].values[0]
            speedup = base_time / t
            scale_factor = w / 2  # relative to baseline 2 workers
            efficiency = (speedup / scale_factor) * 100
            efficiencies.append(efficiency)

        ax.plot(workers, efficiencies, marker="o", linewidth=2, label=label,
                color=data_colors[dv])
        for x, y in zip(workers, efficiencies):
            if y:
                ax.annotate(f"{y:.0f}%", (x, y), textcoords="offset points",
                            xytext=(5, 5), fontsize=8)

    # 100% ideal line
    ax.axhline(y=100, color="black", linestyle="--", linewidth=1, alpha=0.4, label="Ideal (100%)")

    ax.set_xlabel("Worker Nodes", fontsize=11)
    ax.set_ylabel("Parallel Efficiency (%)", fontsize=11)
    ax.set_title("Parallel Efficiency vs. Cluster Size", fontsize=13, fontweight="bold")
    ax.set_xticks(workers)
    ax.set_ylim(50, 115)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    fig.tight_layout()
    path = os.path.join(CHARTS_DIR, "chart_parallel_efficiency.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart -> {path}")


# =====================================================================
# B. ANALYTICS OUTPUT CHARTS (from Parquet data)
# =====================================================================

def chart_funnel_dropoff(enriched_df):
    """
    Classic funnel bar chart: total views → carts → purchases.
    Shows the drop-off at each stage.
    """
    total_views = len(enriched_df[enriched_df["event_type"] == "view"])
    total_carts = len(enriched_df[enriched_df["event_type"] == "cart"])
    total_purchases = len(enriched_df[enriched_df["event_type"] == "purchase"])

    stages = ["View", "Add to Cart", "Purchase"]
    counts = [total_views, total_carts, total_purchases]
    pcts = [100, total_carts / total_views * 100, total_purchases / total_views * 100]

    fig, ax = plt.subplots(figsize=(7, 5))

    bar_colors = [COLORS["blue"], COLORS["orange"], COLORS["green"]]
    bars = ax.bar(stages, counts, color=bar_colors, width=0.55, edgecolor="white", linewidth=1.5)

    for bar, c, p in zip(bars, counts, pcts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + total_views * 0.01,
                f"{c:,}\n({p:.1f}%)", ha="center", va="bottom", fontsize=10, fontweight="bold")

    # Drop-off arrows between bars
    v2c_rate = total_carts / total_views * 100
    c2p_rate = total_purchases / total_carts * 100
    ax.annotate(f"{v2c_rate:.1f}%\nconvert", xy=(0.5, total_carts),
                fontsize=9, ha="center", color=COLORS["grey"])
    ax.annotate(f"{c2p_rate:.1f}%\nconvert", xy=(1.5, total_purchases),
                fontsize=9, ha="center", color=COLORS["grey"])

    ax.set_ylabel("Event Count", fontsize=11)
    ax.set_title("E-commerce Conversion Funnel\n(View → Cart → Purchase)", fontsize=13, fontweight="bold")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1e3:.0f}K"))
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    fig.tight_layout()
    path = os.path.join(CHARTS_DIR, "chart_funnel_dropoff.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart -> {path}")


def chart_attribution_revenue(attribution_summary_df):
    """Horizontal bar chart: revenue by attributed referrer channel."""
    df = attribution_summary_df.sort_values("total_revenue", ascending=True)

    fig, ax = plt.subplots(figsize=(7, 4.5))

    channel_colors = {
        "google": "#4285F4",
        "facebook": "#1877F2",
        "instagram": "#E4405F",
        "email": COLORS["orange"],
        "affiliate": COLORS["purple"],
    }

    colors = [channel_colors.get(r, COLORS["grey"]) for r in df["attributed_referrer"]]
    bars = ax.barh(df["attributed_referrer"], df["total_revenue"], color=colors,
                   edgecolor="white", linewidth=1.5, height=0.55)

    for bar, rev, cnt in zip(bars, df["total_revenue"], df["order_count"]):
        ax.text(bar.get_width() + df["total_revenue"].max() * 0.01,
                bar.get_y() + bar.get_height() / 2,
                f"${rev:,.0f}  ({cnt:,} orders)",
                va="center", fontsize=9)

    ax.set_xlabel("Total Attributed Revenue ($)", fontsize=11)
    ax.set_title("Last-Touch Attribution\nRevenue by Referrer Channel", fontsize=13, fontweight="bold")
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M" if x >= 1e6 else f"${x/1e3:.0f}K"))
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.set_xlim(0, df["total_revenue"].max() * 1.3)

    fig.tight_layout()
    path = os.path.join(CHARTS_DIR, "chart_attribution_revenue.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart -> {path}")


def chart_anomaly_timeline(daily_anomalies_df):
    """Line chart of daily purchase counts with anomalies highlighted."""
    df = daily_anomalies_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    fig, ax = plt.subplots(figsize=(10, 4.5))

    # Main line
    ax.plot(df["date"], df["purchase_count"], marker=".", linewidth=1.5,
            color=COLORS["blue"], label="Daily purchases", zorder=2)

    # Rolling average
    if "rolling_avg" in df.columns:
        valid_avg = df[df["rolling_avg"].notna()]
        ax.plot(valid_avg["date"], valid_avg["rolling_avg"], linewidth=2,
                color=COLORS["grey"], linestyle="--", alpha=0.7, label="7-day rolling avg", zorder=1)

    # Highlight anomalies
    anomalies = df[df["is_anomaly"] == True]
    spikes = anomalies[anomalies["z_score"] > 0]
    drops = anomalies[anomalies["z_score"] < 0]

    if len(spikes) > 0:
        ax.scatter(spikes["date"], spikes["purchase_count"], color=COLORS["red"],
                   s=80, zorder=3, label=f"SPIKE ({len(spikes)})", edgecolors="darkred", linewidth=1)
    if len(drops) > 0:
        ax.scatter(drops["date"], drops["purchase_count"], color=COLORS["orange"],
                   s=80, zorder=3, label=f"DROP ({len(drops)})", edgecolors="darkorange",
                   linewidth=1, marker="v")

    # Annotate a few anomalies
    for _, row in anomalies.head(4).iterrows():
        if row["explanation"] and pd.notna(row["explanation"]):
            short_label = "SPIKE" if row["z_score"] > 0 else "DROP"
            pct = abs(row["purchase_count"] - row["rolling_avg"]) / row["rolling_avg"] * 100 if row["rolling_avg"] else 0
            ax.annotate(
                f"{short_label}\n{pct:.0f}%",
                xy=(row["date"], row["purchase_count"]),
                xytext=(0, 15), textcoords="offset points",
                fontsize=7, ha="center", fontweight="bold",
                color=COLORS["red"] if row["z_score"] > 0 else COLORS["orange"],
            )

    ax.set_xlabel("Date", fontsize=11)
    ax.set_ylabel("Purchase Count", fontsize=11)
    ax.set_title("Daily Purchase Volume with Anomaly Detection\n(7-day rolling baseline, z-score > 2)", fontsize=13, fontweight="bold")
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.autofmt_xdate()

    fig.tight_layout()
    path = os.path.join(CHARTS_DIR, "chart_anomaly_timeline.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart -> {path}")


# =====================================================================
# Pipeline runner (generates analytics data from Kaggle dataset)
# =====================================================================

def run_analytics_pipeline():
    """Run the Spark pipeline on real data to produce analytics Parquet outputs."""
    print("\n>>> Running PySpark pipeline on Kaggle dataset to generate analytics data...")
    print("    This may take a few minutes...\n")

    sys.path.insert(0, os.path.join(BASE_DIR, "src"))

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    spark = (
        SparkSession.builder
        .appName("DSS_Chart_Generation")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    import etl_enrichment
    import attribution as attr_module
    import anomaly_detection as anomaly_module
    from benchmark_job import run_sessionization, run_funnel_analysis

    input_path = os.path.join(BASE_DIR, "data", "data_set", "2019-Oct.csv")
    output_dir = PROCESSED_DIR

    print("  [1/5] ETL Enrichment...")
    etl_enrichment.run(input_path, output_dir, spark)

    print("  [2/5] Sessionization...")
    sessionized = run_sessionization(spark, output_dir)

    print("  [3/5] Funnel Analysis...")
    run_funnel_analysis(sessionized, output_dir)

    print("  [4/5] Attribution...")
    attr_out = os.path.join(output_dir, "attribution")
    attr_module.run(output_dir, attr_out, spark)

    print("  [5/5] Anomaly Detection...")
    anom_out = os.path.join(output_dir, "anomalies")
    anomaly_module.run(output_dir, anom_out, spark)

    spark.stop()
    print("\n>>> Pipeline complete. Analytics data ready.\n")


# =====================================================================
# Main
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="Generate all missing charts")
    parser.add_argument("--run-pipeline", action="store_true",
                        help="Run Spark pipeline on Kaggle data first")
    args = parser.parse_args()

    if args.run_pipeline:
        run_analytics_pipeline()

    # ── A. Optimization charts (from log files) ──────────────────────
    print("\n=== A. Optimization Experiment Charts ===")
    chart_broadcast_join()
    chart_partition_pruning()
    chart_skew_salting()

    # ── C. Additional performance chart ──────────────────────────────
    print("\n=== C. Additional Performance Charts ===")
    chart_parallel_efficiency()

    # ── B. Analytics output charts (from Parquet) ────────────────────
    print("\n=== B. Analytics Output Charts ===")

    # Read enriched events
    enriched_path = os.path.join(PROCESSED_DIR, "enriched_events")
    if not os.path.exists(enriched_path):
        print("  WARNING: enriched_events not found. Run with --run-pipeline first.")
        return

    enriched_df = pd.read_parquet(enriched_path)
    n_events = len(enriched_df)
    print(f"  Loaded enriched_events: {n_events:,} rows")

    # Check if data is real (>100K rows) or test data
    if n_events < 100_000:
        print(f"  WARNING: Only {n_events:,} rows — this is test data.")
        print("  Re-running pipeline on Kaggle data for meaningful charts...")
        run_analytics_pipeline()
        enriched_df = pd.read_parquet(enriched_path)
        print(f"  Reloaded enriched_events: {len(enriched_df):,} rows")

    chart_funnel_dropoff(enriched_df)

    # Attribution summary
    attr_summary_path = os.path.join(PROCESSED_DIR, "attribution", "attribution_summary")
    if os.path.exists(attr_summary_path):
        attr_df = pd.read_parquet(attr_summary_path)
        print(f"  Loaded attribution_summary: {len(attr_df)} rows")
        chart_attribution_revenue(attr_df)
    else:
        print("  WARNING: attribution_summary not found — skipping attribution chart.")

    # Daily anomalies
    daily_anom_path = os.path.join(PROCESSED_DIR, "anomalies", "daily_anomalies")
    if os.path.exists(daily_anom_path):
        daily_anom_df = pd.read_parquet(daily_anom_path)
        print(f"  Loaded daily_anomalies: {len(daily_anom_df)} rows")
        if len(daily_anom_df) > 5:
            chart_anomaly_timeline(daily_anom_df)
        else:
            print("  WARNING: Too few days for anomaly timeline — need pipeline run on full data.")
    else:
        print("  WARNING: daily_anomalies not found — skipping anomaly chart.")

    print(f"\n=== Done. All charts saved to {CHARTS_DIR}/ ===\n")


if __name__ == "__main__":
    main()
