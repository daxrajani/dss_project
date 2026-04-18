"""
generate_perf_charts.py — Parse benchmark logs and generate all performance charts.

Reads results/cloud_*w_*.log files, extracts timing data, updates
results/benchmark_summary.csv, then generates 6 performance charts:

  1. scaling_time.png        — Total pipeline time vs workers (by data size)
  2. speedup.png             — Speedup vs workers (with ideal line)
  3. parallel_efficiency.png — Efficiency % vs workers
  4. throughput.png          — Rows/sec vs workers
  5. stage_breakdown.png     — Stacked bar: per-stage time (2-worker baseline)
  6. data_scaling.png        — Time vs data size (by worker count)

Usage:
    python src/generate_perf_charts.py               # use existing benchmark_summary.csv
    python src/generate_perf_charts.py --parse-logs  # re-parse log files first
"""

import argparse
import os
import re
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(BASE_DIR, "results")
CHARTS_DIR  = os.path.join(RESULTS_DIR, "charts")
CSV_PATH    = os.path.join(RESULTS_DIR, "benchmark_summary.csv")

os.makedirs(CHARTS_DIR, exist_ok=True)

# ── colour palette ────────────────────────────────────────────────────────────
COLORS = {
    "blue":   "#4C72B0",
    "orange": "#DD8452",
    "green":  "#55A868",
    "red":    "#C44E52",
    "purple": "#8172B2",
    "brown":  "#937860",
    "grey":   "#999999",
}
DATA_COLORS   = {"1gb": COLORS["blue"],  "5gb": COLORS["orange"], "10gb": COLORS["green"]}
WORKER_COLORS = {2: COLORS["blue"], 4: COLORS["orange"], 5: COLORS["green"]}
DATA_LABELS   = {"1gb": "~1 GB (8.5M rows)", "5gb": "~5 GB (42M rows)", "10gb": "~10 GB (85M rows)"}

plt.rcParams.update({
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "figure.facecolor": "white",
})


# ─────────────────────────────────────────────────────────────────────────────
# Log parser
# ─────────────────────────────────────────────────────────────────────────────

def _extract_float(pattern, text, default=None):
    m = re.search(pattern, text)
    return float(m.group(1)) if m else default

def _extract_int(pattern, text, default=None):
    m = re.search(pattern, text)
    return int(m.group(1).replace(",", "")) if m else default

def parse_log(log_path):
    """
    Parse a cloud_Nw_Xgb.log file and return a dict of extracted metrics.
    Uses the LAST occurrence of each field (the run may contain warm-up/retry output).
    """
    fname = os.path.basename(log_path)
    m = re.match(r"cloud_(\d+)w_([\d]+gb)\.log", fname)
    if not m:
        return None
    workers      = int(m.group(1))
    data_variant = m.group(2)

    with open(log_path) as f:
        text = f.read()

    # Use the PIPELINE SUMMARY block (last occurrence)
    summaries = list(re.finditer(r"PIPELINE SUMMARY.*?={60}", text, re.DOTALL))
    block = summaries[-1].group(0) if summaries else text

    def _f(pat):  return _extract_float(pat, block)
    def _i(pat):  return _extract_int(pat, block)

    total_s      = _f(r"TOTAL PIPELINE TIME\s+([\d.]+)s")
    input_rows   = _i(r"Input rows\s+([\d,]+)")
    orders_attr  = _i(r"Orders attributed\s+([\d,]+)")
    sys_anom     = _i(r"System anomalies flagged\s+([\d,]+)")
    cat_anom     = _i(r"Category anomalies flagged\s+([\d,]+)")
    funnel_daily = _i(r"Funnel daily rows\s+([\d,]+)")

    etl_s     = _f(r"Stage 1\s+ETL compute\s+([\d.]+)s")
    session_s = _f(r"Stage 2\s+Sessionization\s+([\d.]+)s")
    funnel_s  = _f(r"Stage 3\s+Funnel Analysis\s+([\d.]+)s")
    attr_s    = _f(r"Stage 4\s+Attribution\s+([\d.]+)s")
    anom_s    = _f(r"Stage 5\s+Anomaly Detection\s+([\d.]+)s")

    etl_write_s  = _f(r"ETL Parquet write\s+([\d.]+)s")
    sess_refs_s  = _f(r"session_referrers build\s+([\d.]+)s")
    join_s       = _f(r"last-touch join\s+([\d.]+)s")
    attr_write_s = _f(r"attribution Parquet write\s+([\d.]+)s")
    daily_anom_s = _f(r"daily anomalies\s+([\d.]+)s")
    cat_anom_s   = _f(r"category anomalies\s+([\d.]+)s")

    rows_per_sec = round(input_rows / total_s) if (input_rows and total_s) else None

    return {
        "workers":          workers,
        "data_variant":     data_variant,
        "input_rows":       input_rows,
        "total_s":          total_s,
        "etl_s":            etl_s,
        "session_s":        session_s,
        "funnel_s":         funnel_s,
        "attr_s":           attr_s,
        "anom_s":           anom_s,
        "etl_write_s":      etl_write_s,
        "session_refs_s":   sess_refs_s,
        "join_s":           join_s,
        "attr_write_s":     attr_write_s,
        "daily_anom_s":     daily_anom_s,
        "cat_anom_s":       cat_anom_s,
        "orders_attr":      orders_attr,
        "sys_anomalies":    sys_anom,
        "cat_anomalies":    cat_anom,
        "rows_per_sec":     rows_per_sec,
        "funnel_daily_rows": funnel_daily,
    }


def rebuild_csv():
    """Parse all cloud_*w_*gb.log files in results/ and write benchmark_summary.csv."""
    import glob
    log_files = sorted(glob.glob(os.path.join(RESULTS_DIR, "cloud_*w_*gb.log")))
    if not log_files:
        print("  [WARN] No cloud_*w_*gb.log files found in results/")
        return None

    rows = []
    for lf in log_files:
        rec = parse_log(lf)
        if rec:
            rows.append(rec)
            print(f"  Parsed: {os.path.basename(lf)}  →  total={rec['total_s']}s  rows={rec['input_rows']:,}")
        else:
            print(f"  [SKIP] Could not parse {os.path.basename(lf)}")

    if not rows:
        return None

    df = pd.DataFrame(rows).sort_values(["workers", "data_variant"])
    df.to_csv(CSV_PATH, index=False)
    print(f"\n  Updated: {CSV_PATH}  ({len(df)} rows)\n")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Chart helpers
# ─────────────────────────────────────────────────────────────────────────────

def _save(fig, name):
    path = os.path.join(CHARTS_DIR, name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved → {path}")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Scaling time — total pipeline time vs workers
# ─────────────────────────────────────────────────────────────────────────────

def chart_scaling_time(df):
    fig, ax = plt.subplots(figsize=(7, 4.5))
    workers = sorted(df["workers"].unique())

    for dv in ["1gb", "5gb", "10gb"]:
        sub = df[df["data_variant"] == dv].sort_values("workers")
        if sub.empty:
            continue
        ax.plot(sub["workers"], sub["total_s"], marker="o", linewidth=2,
                label=DATA_LABELS.get(dv, dv), color=DATA_COLORS[dv])
        for _, row in sub.iterrows():
            ax.annotate(f"{row['total_s']:.0f}s",
                        (row["workers"], row["total_s"]),
                        textcoords="offset points", xytext=(6, 4), fontsize=8)

    ax.set_xlabel("Worker Nodes")
    ax.set_ylabel("Total Pipeline Time (seconds)")
    ax.set_title("Scaling Study — Pipeline Time vs Cluster Size")
    ax.set_xticks(workers)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    _save(fig, "chart_scaling_time.png")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Speedup vs workers
# ─────────────────────────────────────────────────────────────────────────────

def chart_speedup(df):
    fig, ax = plt.subplots(figsize=(7, 4.5))
    workers = sorted(df["workers"].unique())
    base_w  = min(workers)

    # Ideal speedup line
    ideal_w = np.linspace(base_w, max(workers), 50)
    ax.plot(ideal_w, ideal_w / base_w, linestyle="--", color="black",
            linewidth=1, alpha=0.5, label="Ideal linear speedup")

    for dv in ["1gb", "5gb", "10gb"]:
        sub = df[df["data_variant"] == dv].sort_values("workers")
        if sub.empty:
            continue
        base_time = sub[sub["workers"] == base_w]["total_s"].values
        if not len(base_time):
            continue
        base_time = base_time[0]
        speedups  = base_time / sub["total_s"].values

        ax.plot(sub["workers"].values, speedups, marker="o", linewidth=2,
                label=DATA_LABELS.get(dv, dv), color=DATA_COLORS[dv])
        for w, s in zip(sub["workers"].values, speedups):
            ax.annotate(f"{s:.2f}×", (w, s),
                        textcoords="offset points", xytext=(6, 4), fontsize=8)

    ax.set_xlabel("Worker Nodes")
    ax.set_ylabel(f"Speedup (relative to {base_w} workers)")
    ax.set_title("Speedup vs Cluster Size")
    ax.set_xticks(workers)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    _save(fig, "chart_speedup.png")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Parallel efficiency
# ─────────────────────────────────────────────────────────────────────────────

def chart_parallel_efficiency(df):
    fig, ax = plt.subplots(figsize=(7, 4.5))
    workers = sorted(df["workers"].unique())
    base_w  = min(workers)

    ax.axhline(y=100, color="black", linestyle="--", linewidth=1,
               alpha=0.4, label="Ideal (100%)")

    for dv in ["1gb", "5gb", "10gb"]:
        sub = df[df["data_variant"] == dv].sort_values("workers")
        if sub.empty:
            continue
        base_time = sub[sub["workers"] == base_w]["total_s"].values
        if not len(base_time):
            continue
        base_time = base_time[0]
        effs = []
        ws   = []
        for _, row in sub.iterrows():
            speedup  = base_time / row["total_s"]
            eff      = speedup / (row["workers"] / base_w) * 100
            effs.append(eff)
            ws.append(row["workers"])

        ax.plot(ws, effs, marker="o", linewidth=2,
                label=DATA_LABELS.get(dv, dv), color=DATA_COLORS[dv])
        for w, e in zip(ws, effs):
            ax.annotate(f"{e:.0f}%", (w, e),
                        textcoords="offset points", xytext=(5, 5), fontsize=8)

    ax.set_xlabel("Worker Nodes")
    ax.set_ylabel("Parallel Efficiency (%)")
    ax.set_title("Parallel Efficiency vs Cluster Size")
    ax.set_xticks(workers)
    ax.set_ylim(40, 115)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    _save(fig, "chart_parallel_efficiency.png")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Throughput (rows/sec)
# ─────────────────────────────────────────────────────────────────────────────

def chart_throughput(df):
    if "rows_per_sec" not in df.columns or df["rows_per_sec"].isna().all():
        print("  [SKIP] No rows_per_sec data available.")
        return

    fig, ax = plt.subplots(figsize=(7, 4.5))
    workers = sorted(df["workers"].unique())

    for dv in ["1gb", "5gb", "10gb"]:
        sub = df[df["data_variant"] == dv].sort_values("workers")
        if sub.empty or sub["rows_per_sec"].isna().all():
            continue
        ax.plot(sub["workers"], sub["rows_per_sec"] / 1000, marker="o", linewidth=2,
                label=DATA_LABELS.get(dv, dv), color=DATA_COLORS[dv])
        for _, row in sub.iterrows():
            if pd.notna(row["rows_per_sec"]):
                ax.annotate(f"{row['rows_per_sec']/1000:.0f}K",
                            (row["workers"], row["rows_per_sec"] / 1000),
                            textcoords="offset points", xytext=(6, 4), fontsize=8)

    ax.set_xlabel("Worker Nodes")
    ax.set_ylabel("Throughput (K rows/second)")
    ax.set_title("Processing Throughput vs Cluster Size")
    ax.set_xticks(workers)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    _save(fig, "chart_throughput.png")


# ─────────────────────────────────────────────────────────────────────────────
# 5. Stage breakdown — stacked bar (use row with most stage detail)
# ─────────────────────────────────────────────────────────────────────────────

def chart_stage_breakdown(df):
    stage_cols = ["etl_s", "session_s", "funnel_s", "attr_s", "anom_s"]
    stage_labels = {
        "etl_s":     "Stage 1: ETL",
        "session_s": "Stage 2: Sessionization",
        "funnel_s":  "Stage 3: Funnel",
        "attr_s":    "Stage 4: Attribution",
        "anom_s":    "Stage 5: Anomaly Detection",
    }
    stage_colors = [COLORS["blue"], COLORS["orange"], COLORS["green"],
                    COLORS["purple"], COLORS["red"]]

    # Find rows that have stage-level detail
    has_stages = df.dropna(subset=stage_cols, how="all")
    if has_stages.empty:
        print("  [SKIP] No per-stage timing data — skipping stage breakdown chart.")
        return

    # Group by workers + data_variant, pick representative rows
    rows_to_plot = []
    for dv in ["1gb", "5gb", "10gb"]:
        sub = has_stages[has_stages["data_variant"] == dv].sort_values("workers")
        for _, row in sub.iterrows():
            if not all(pd.isna(row[c]) for c in stage_cols):
                rows_to_plot.append(row)

    if not rows_to_plot:
        print("  [SKIP] No stage breakdown data found.")
        return

    plot_df   = pd.DataFrame(rows_to_plot)
    x_labels  = [f"{int(r['workers'])}w\n{r['data_variant']}" for _, r in plot_df.iterrows()]
    x_pos     = np.arange(len(x_labels))

    fig, ax = plt.subplots(figsize=(max(8, len(x_labels) * 1.2), 5))

    bottoms = np.zeros(len(x_labels))
    for col, color in zip(stage_cols, stage_colors):
        vals = plot_df[col].fillna(0).values
        ax.bar(x_pos, vals, bottom=bottoms, color=color,
               label=stage_labels[col], edgecolor="white", linewidth=0.8)
        # label segments that are > 5% of total
        for i, (v, b, total) in enumerate(zip(vals, bottoms, plot_df["total_s"].values)):
            if total and v / total > 0.06:
                ax.text(x_pos[i], b + v / 2, f"{v:.0f}s",
                        ha="center", va="center", fontsize=7, color="white", fontweight="bold")
        bottoms += vals

    # Total time on top of bar
    for i, (b, total) in enumerate(zip(bottoms, plot_df["total_s"].values)):
        ax.text(x_pos[i], b + 2, f"{total:.0f}s total",
                ha="center", va="bottom", fontsize=8, fontweight="bold")

    ax.set_xticks(x_pos)
    ax.set_xticklabels(x_labels, fontsize=9)
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Pipeline Stage Time Breakdown\n(workers × data size)")
    ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1, 1))
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    _save(fig, "chart_stage_breakdown.png")


# ─────────────────────────────────────────────────────────────────────────────
# 6. Data size scaling — time vs data size (one line per worker count)
# ─────────────────────────────────────────────────────────────────────────────

def chart_data_scaling(df):
    order_map = {"1gb": 1, "5gb": 5, "10gb": 10}
    size_gb   = {"1gb": 1, "5gb": 5, "10gb": 10}

    workers_list = sorted(df["workers"].unique())
    fig, ax = plt.subplots(figsize=(7, 4.5))

    for w in workers_list:
        sub = df[df["workers"] == w].copy()
        sub = sub[sub["data_variant"].isin(order_map)]
        sub = sub.sort_values("data_variant", key=lambda s: s.map(order_map))
        if sub.empty:
            continue
        x = [size_gb[dv] for dv in sub["data_variant"]]
        ax.plot(x, sub["total_s"].values, marker="o", linewidth=2,
                label=f"{w} workers", color=WORKER_COLORS.get(w, COLORS["grey"]))
        for xi, yi in zip(x, sub["total_s"].values):
            ax.annotate(f"{yi:.0f}s", (xi, yi),
                        textcoords="offset points", xytext=(6, 4), fontsize=8)

    ax.set_xlabel("Dataset Size (GB)")
    ax.set_ylabel("Total Pipeline Time (seconds)")
    ax.set_title("Pipeline Time vs Dataset Size\n(by cluster size)")
    ax.set_xticks([1, 5, 10])
    ax.set_xticklabels(["~1 GB\n(8.5M rows)", "~5 GB\n(42M rows)", "~10 GB\n(85M rows)"])
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    _save(fig, "chart_data_scaling.png")


# ─────────────────────────────────────────────────────────────────────────────
# Print summary table
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(df):
    print("\n" + "═"*72)
    print("  BENCHMARK SUMMARY — 9 Combinations (Workers × Data Size)")
    print("═"*72)
    print(f"  {'Workers':>8}  {'Data':>6}  {'Rows':>12}  {'Time(s)':>8}  "
          f"{'Rows/sec':>10}  {'Speedup':>8}")
    print(f"  {'-'*8}  {'-'*6}  {'-'*12}  {'-'*8}  {'-'*10}  {'-'*8}")

    base_times = {}
    for dv in ["1gb", "5gb", "10gb"]:
        sub = df[df["data_variant"] == dv].sort_values("workers")
        base = sub.iloc[0]["total_s"] if not sub.empty else None
        base_times[dv] = base

    for dv in ["1gb", "5gb", "10gb"]:
        sub = df[df["data_variant"] == dv].sort_values("workers")
        for _, row in sub.iterrows():
            speedup = (base_times[dv] / row["total_s"]
                       if base_times[dv] and pd.notna(row["total_s"]) else None)
            speedup_str = f"{speedup:.2f}×" if speedup else "  —"
            rps = f"{int(row['rows_per_sec']):,}" if pd.notna(row.get("rows_per_sec")) else "—"
            rows = f"{int(row['input_rows']):,}" if pd.notna(row.get("input_rows")) else "—"
            print(f"  {int(row['workers']):>8}  {row['data_variant']:>6}  {rows:>12}  "
                  f"{row['total_s']:>8.1f}  {rps:>10}  {speedup_str:>8}")
        print()

    print("═"*72 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate all performance charts from benchmark results")
    parser.add_argument("--parse-logs", action="store_true",
                        help="Re-parse cloud_*w_*gb.log files and rebuild benchmark_summary.csv")
    parser.add_argument("--csv", default=CSV_PATH,
                        help=f"Path to benchmark_summary.csv (default: {CSV_PATH})")
    args = parser.parse_args()

    if args.parse_logs:
        print("\nParsing log files ...")
        df = rebuild_csv()
        if df is None:
            print("ERROR: No log files parsed. Cannot generate charts.")
            sys.exit(1)
    else:
        if not os.path.exists(args.csv):
            print(f"ERROR: {args.csv} not found. Run with --parse-logs to generate it from log files.")
            sys.exit(1)
        df = pd.read_csv(args.csv)
        print(f"Loaded {args.csv}  ({len(df)} rows)")

    if df.empty:
        print("ERROR: No benchmark data available.")
        sys.exit(1)

    print_summary(df)

    print("Generating performance charts ...")
    chart_scaling_time(df)
    chart_speedup(df)
    chart_parallel_efficiency(df)
    chart_throughput(df)
    chart_stage_breakdown(df)
    chart_data_scaling(df)

    print(f"\nAll charts saved to: {CHARTS_DIR}/")
    print("Charts generated:")
    for name in [
        "chart_scaling_time.png",
        "chart_speedup.png",
        "chart_parallel_efficiency.png",
        "chart_throughput.png",
        "chart_stage_breakdown.png",
        "chart_data_scaling.png",
    ]:
        path = os.path.join(CHARTS_DIR, name)
        status = "✓" if os.path.exists(path) else "✗"
        print(f"  {status} {name}")
    print()


if __name__ == "__main__":
    main()
