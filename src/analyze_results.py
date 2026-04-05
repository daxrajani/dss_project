"""
analyze_results.py — Parse cloud benchmark logs and produce:
  1. results/benchmark_summary.csv   — full timing table
  2. results/charts/                 — PNG charts for the report

Usage:
    python src/analyze_results.py
    python src/analyze_results.py --logs-dir results --out-dir results
"""

import argparse
import os
import re
import csv

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _extract(pattern, text, cast=float, default=None):
    m = re.search(pattern, text)
    if m:
        return cast(m.group(1).replace(",", ""))
    return default


def parse_log(path):
    with open(path) as f:
        text = f.read()

    # Take the LAST occurrence of each metric (most recent job run in the file)
    def last(pattern, cast=float):
        matches = re.findall(pattern, text, re.MULTILINE)
        if matches:
            return cast(matches[-1].replace(",", ""))
        return None

    return {
        "total_s":         last(r"TOTAL PIPELINE TIME\s+([\d.]+)s"),
        "etl_s":           last(r"Stage 1\s+ETL compute\s+([\d.]+)s"),
        "session_s":       last(r"Stage 2\s+Sessionization\s+([\d.]+)s"),
        "funnel_s":        last(r"Stage 3\s+Funnel Analysis\s+([\d.]+)s"),
        "attr_s":          last(r"Stage 4\s+Attribution\s+([\d.]+)s"),
        "anom_s":          last(r"Stage 5\s+Anomaly Detection\s+([\d.]+)s"),
        "etl_write_s":     last(r"ETL Parquet write\s+([\d.]+)s"),
        "session_refs_s":  last(r"session_referrers build\s+([\d.]+)s"),
        "join_s":          last(r"last-touch join\s+([\d.]+)s"),
        "attr_write_s":    last(r"attribution Parquet write\s+([\d.]+)s"),
        "daily_anom_s":    last(r"daily anomalies\s+([\d.]+)s"),
        "cat_anom_s":      last(r"category anomalies\s+([\d.]+)s"),
        "input_rows":      last(r"Input rows\s+([\d,]+)", cast=str),
        "orders_attr":     last(r"Orders attributed\s+([\d,]+)", cast=str),
        "sys_anomalies":   last(r"System anomalies flagged\s+([\d]+)", cast=str),
        "cat_anomalies":   last(r"Category anomalies flagged\s+([\d]+)", cast=str),
        "funnel_daily_rows": last(r"Funnel daily rows\s+([\d,]+)", cast=str),
    }


def load_all(logs_dir):
    rows = []
    pattern = re.compile(r"cloud_(\d+)w_(1gb|5gb|10gb)\.log")
    data_order = {"1gb": 1, "5gb": 5, "10gb": 10}

    for fname in sorted(os.listdir(logs_dir)):
        m = pattern.match(fname)
        if not m:
            continue
        workers = int(m.group(1))
        data_var = m.group(2)
        metrics = parse_log(os.path.join(logs_dir, fname))
        rows.append({
            "workers": workers,
            "data_variant": data_var,
            "data_gb": data_order[data_var],
            **metrics,
        })

    rows.sort(key=lambda r: (r["workers"], r["data_gb"]))

    # Derived metric: throughput (rows / second)
    for r in rows:
        try:
            n = int(str(r["input_rows"]).replace(",", ""))
            r["rows_per_sec"] = round(n / r["total_s"]) if r["total_s"] else None
        except Exception:
            r["rows_per_sec"] = None

    return rows


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "workers", "data_variant", "input_rows",
    "total_s", "etl_s", "session_s", "funnel_s", "attr_s", "anom_s",
    "etl_write_s", "session_refs_s", "join_s", "attr_write_s",
    "daily_anom_s", "cat_anom_s",
    "orders_attr", "sys_anomalies", "cat_anomalies",
    "rows_per_sec", "funnel_daily_rows",
]

def write_csv(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  CSV → {path}")


# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------

WORKERS = [2, 4, 5]
DATA_LABELS = {"1gb": "~1 GB (8.5M rows)", "5gb": "~5 GB (42M rows)", "10gb": "~10 GB (85M rows)"}
DATA_COLORS = {"1gb": "#4C72B0", "5gb": "#DD8452", "10gb": "#55A868"}
WORKER_COLORS = {"2": "#4C72B0", "4": "#DD8452", "5": "#55A868"}
STAGES = ["etl_s", "session_s", "funnel_s", "attr_s", "anom_s", "etl_write_s", "attr_write_s"]
STAGE_LABELS = ["ETL compute", "Sessionization", "Funnel analysis",
                "Attribution", "Anomaly detect", "ETL write", "Attr write"]
STAGE_COLORS = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2", "#937860", "#64B5CD"]

def _lookup(rows, workers, data_var, key):
    for r in rows:
        if r["workers"] == workers and r["data_variant"] == data_var:
            return r.get(key)
    return None


# ---------------------------------------------------------------------------
# Chart 1 — Total pipeline time vs workers (one line per data size)
# ---------------------------------------------------------------------------

def chart_total_time(rows, out_dir):
    fig, ax = plt.subplots(figsize=(7, 4.5))

    for dv, label in DATA_LABELS.items():
        ys = [_lookup(rows, w, dv, "total_s") for w in WORKERS]
        ax.plot(WORKERS, ys, marker="o", linewidth=2, label=label,
                color=DATA_COLORS[dv])
        for x, y in zip(WORKERS, ys):
            if y:
                ax.annotate(f"{y:.0f}s", (x, y), textcoords="offset points",
                            xytext=(5, 4), fontsize=8)

    ax.set_xlabel("Worker nodes", fontsize=11)
    ax.set_ylabel("Total pipeline time (s)", fontsize=11)
    ax.set_title("Total Pipeline Time vs. Cluster Size", fontsize=13, fontweight="bold")
    ax.set_xticks(WORKERS)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    fig.tight_layout()
    path = os.path.join(out_dir, "chart_total_time.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart → {path}")


# ---------------------------------------------------------------------------
# Chart 2 — Speedup relative to 2-worker baseline
# ---------------------------------------------------------------------------

def chart_speedup(rows, out_dir):
    fig, ax = plt.subplots(figsize=(7, 4.5))

    for dv, label in DATA_LABELS.items():
        base = _lookup(rows, 2, dv, "total_s")
        if not base:
            continue
        ys = [base / _lookup(rows, w, dv, "total_s") for w in WORKERS]
        ax.plot(WORKERS, ys, marker="o", linewidth=2, label=label,
                color=DATA_COLORS[dv])
        for x, y in zip(WORKERS, ys):
            ax.annotate(f"{y:.2f}×", (x, y), textcoords="offset points",
                        xytext=(5, 4), fontsize=8)

    # Ideal linear speedup reference
    ideal = [w / 2 for w in WORKERS]
    ax.plot(WORKERS, ideal, "k--", linewidth=1, alpha=0.4, label="Ideal (linear)")

    ax.set_xlabel("Worker nodes", fontsize=11)
    ax.set_ylabel("Speedup (relative to 2-worker)", fontsize=11)
    ax.set_title("Speedup vs. Cluster Size (2-worker baseline)", fontsize=13, fontweight="bold")
    ax.set_xticks(WORKERS)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    path = os.path.join(out_dir, "chart_speedup.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart → {path}")


# ---------------------------------------------------------------------------
# Chart 3 — Stacked bar: stage breakdown per (workers × data size)
# ---------------------------------------------------------------------------

def chart_stage_breakdown(rows, out_dir):
    configs = [(w, dv) for w in WORKERS for dv in ["1gb", "5gb", "10gb"]]
    xlabels = [f"{w}w\n{dv}" for w, dv in configs]
    x = np.arange(len(configs))
    width = 0.6

    fig, ax = plt.subplots(figsize=(12, 5.5))
    bottoms = np.zeros(len(configs))

    for stage, label, color in zip(STAGES, STAGE_LABELS, STAGE_COLORS):
        vals = np.array([_lookup(rows, w, dv, stage) or 0 for w, dv in configs])
        ax.bar(x, vals, width, bottom=bottoms, label=label, color=color)
        bottoms += vals

    ax.set_xticks(x)
    ax.set_xticklabels(xlabels, fontsize=9)
    ax.set_ylabel("Time (s)", fontsize=11)
    ax.set_title("Pipeline Stage Breakdown by Configuration", fontsize=13, fontweight="bold")
    ax.legend(loc="upper left", fontsize=8, ncol=2)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    path = os.path.join(out_dir, "chart_stage_breakdown.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart → {path}")


# ---------------------------------------------------------------------------
# Chart 4 — Attribution time vs data size (log-log scaling)
# ---------------------------------------------------------------------------

def chart_attribution_scaling(rows, out_dir):
    fig, ax = plt.subplots(figsize=(7, 4.5))

    row_counts = {"1gb": 8_491_904, "5gb": 42_448_764, "10gb": 84_897_528}

    for w in WORKERS:
        xs = [row_counts[dv] / 1e6 for dv in ["1gb", "5gb", "10gb"]]
        ys = [_lookup(rows, w, dv, "attr_s") for dv in ["1gb", "5gb", "10gb"]]
        color = WORKER_COLORS[str(w)]
        ax.plot(xs, ys, marker="o", linewidth=2, label=f"{w} workers", color=color)
        for x, y in zip(xs, ys):
            if y:
                ax.annotate(f"{y:.0f}s", (x, y), textcoords="offset points",
                            xytext=(5, 4), fontsize=8)

    ax.set_xlabel("Input rows (millions)", fontsize=11)
    ax.set_ylabel("Attribution stage time (s)", fontsize=11)
    ax.set_title("Attribution Time vs. Data Volume", fontsize=13, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    path = os.path.join(out_dir, "chart_attribution_scaling.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart → {path}")


# ---------------------------------------------------------------------------
# Chart 5 — Heatmap: total time (workers × data size)
# ---------------------------------------------------------------------------

def chart_heatmap(rows, out_dir):
    data_variants = ["1gb", "5gb", "10gb"]
    matrix = np.array([
        [_lookup(rows, w, dv, "total_s") or 0 for dv in data_variants]
        for w in WORKERS
    ])

    fig, ax = plt.subplots(figsize=(6, 4))
    im = ax.imshow(matrix, cmap="RdYlGn_r", aspect="auto")

    ax.set_xticks(range(len(data_variants)))
    ax.set_xticklabels(["~1 GB", "~5 GB", "~10 GB"], fontsize=10)
    ax.set_yticks(range(len(WORKERS)))
    ax.set_yticklabels([f"{w} workers" for w in WORKERS], fontsize=10)
    ax.set_title("Total Pipeline Time Heatmap (seconds)", fontsize=12, fontweight="bold")

    for i in range(len(WORKERS)):
        for j in range(len(data_variants)):
            ax.text(j, i, f"{matrix[i, j]:.0f}s",
                    ha="center", va="center", fontsize=11,
                    color="white" if matrix[i, j] > matrix.mean() else "black")

    fig.colorbar(im, ax=ax, label="Seconds")
    fig.tight_layout()
    path = os.path.join(out_dir, "chart_heatmap.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart → {path}")


# ---------------------------------------------------------------------------
# Chart 6 — Throughput (rows/sec) vs workers
# ---------------------------------------------------------------------------

def chart_throughput(rows, out_dir):
    fig, ax = plt.subplots(figsize=(7, 4.5))

    for dv, label in DATA_LABELS.items():
        ys = [_lookup(rows, w, dv, "rows_per_sec") for w in WORKERS]
        if any(y is not None for y in ys):
            ax.plot(WORKERS, ys, marker="o", linewidth=2, label=label,
                    color=DATA_COLORS[dv])
            for x, y in zip(WORKERS, ys):
                if y:
                    ax.annotate(f"{y/1000:.0f}K", (x, y), textcoords="offset points",
                                xytext=(5, 4), fontsize=8)

    ax.set_xlabel("Worker nodes", fontsize=11)
    ax.set_ylabel("Throughput (rows / second)", fontsize=11)
    ax.set_title("Pipeline Throughput vs. Cluster Size", fontsize=13, fontweight="bold")
    ax.set_xticks(WORKERS)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    path = os.path.join(out_dir, "chart_throughput.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Chart → {path}")


# ---------------------------------------------------------------------------
# Print summary table to stdout
# ---------------------------------------------------------------------------

def print_summary(rows):
    print("\n" + "=" * 90)
    print(f" {'Config':<10} {'Rows':>10} {'Total':>8} {'ETL':>7} {'Session':>8} "
          f"{'Funnel':>7} {'Attr':>7} {'Anom':>7} {'Rows/s':>8} {'Speedup':>8}")
    print(" " + "-" * 88)
    baselines = {}
    for r in rows:
        dv = r["data_variant"]
        if r["workers"] == 2:
            baselines[dv] = r["total_s"]
        speedup = baselines.get(dv, r["total_s"]) / r["total_s"] if r["total_s"] else 0
        def fmt(v, w=6): return f"{v:>{w}.1f}s" if v is not None else "  N/A"
        rps = f"{r['rows_per_sec']/1000:.0f}K" if r.get("rows_per_sec") else "N/A"
        print(f" {r['workers']}w-{r['data_variant']:<6} {r['input_rows']:>10} "
              f"{fmt(r['total_s'],7)} {fmt(r['etl_s'])} "
              f"{fmt(r['session_s'],7)} {fmt(r['funnel_s'])} "
              f"{fmt(r['attr_s'])} {fmt(r['anom_s'])} {rps:>8} {speedup:>7.2f}x")
    print("=" * 90 + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Analyze DSS benchmark results")
    parser.add_argument("--logs-dir", default="results", help="Directory with cloud_*w_*.log files")
    parser.add_argument("--out-dir",  default="results", help="Output directory for CSV and charts")
    args = parser.parse_args()

    charts_dir = os.path.join(args.out_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)

    print("\nParsing logs ...")
    rows = load_all(args.logs_dir)
    print(f"  Found {len(rows)} benchmark runs.")

    print_summary(rows)

    print("Writing CSV ...")
    write_csv(rows, os.path.join(args.out_dir, "benchmark_summary.csv"))

    print("Generating charts ...")
    chart_total_time(rows, charts_dir)
    chart_speedup(rows, charts_dir)
    chart_stage_breakdown(rows, charts_dir)
    chart_attribution_scaling(rows, charts_dir)
    chart_heatmap(rows, charts_dir)
    chart_throughput(rows, charts_dir)

    print(f"\nDone. Charts in {charts_dir}/\n")


if __name__ == "__main__":
    main()
