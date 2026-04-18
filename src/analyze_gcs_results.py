"""
analyze_gcs_results.py — Download pipeline outputs from GCS and show analysis results.

Downloads the Parquet files written by cloud_pipeline.py, prints summary tables,
and generates charts to results/demo_analysis/.

Usage:
    python src/analyze_gcs_results.py --gcs-output gs://YOUR_BUCKET/results/demo_run
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR  = os.path.join(BASE_DIR, "results", "demo_analysis")

# ── colour palette ──────────────────────────────────────────────────────────
COLORS = {
    "blue":   "#4C72B0",
    "orange": "#DD8452",
    "green":  "#55A868",
    "red":    "#C44E52",
    "purple": "#8172B2",
    "grey":   "#999999",
}
plt.rcParams.update({
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "figure.facecolor": "white",
})

# ─────────────────────────────────────────────────────────────────────────────
# GCS helpers
# ─────────────────────────────────────────────────────────────────────────────

def _find_gsutil():
    found = shutil.which("gsutil")
    if found:
        return found
    for p in [
        os.path.expanduser("~/google-cloud-sdk/bin/gsutil"),
        "/usr/lib/google-cloud-sdk/bin/gsutil",
        "/snap/bin/gsutil",
        "/usr/local/bin/gsutil",
    ]:
        if os.path.isfile(p):
            return p
    return "gsutil"


def download_parquet(gsutil, gcs_path, local_dir):
    """Download a GCS Parquet directory to a local directory."""
    os.makedirs(local_dir, exist_ok=True)
    result = subprocess.run(
        [gsutil, "-m", "cp", "-r", gcs_path + "/*.parquet", local_dir],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        # Try without glob (some GCS paths use part-*.snappy.parquet naming)
        result2 = subprocess.run(
            [gsutil, "-m", "cp", "-r", gcs_path + "/", local_dir],
            capture_output=True, text=True,
        )
        if result2.returncode != 0:
            print(f"  [WARN] Could not download {gcs_path}: {result2.stderr.strip()[:200]}")
            return False
    return True


def read_parquet_dir(local_dir):
    """Read all .parquet files under local_dir into a DataFrame."""
    files = []
    for root, _, fnames in os.walk(local_dir):
        for fn in fnames:
            if fn.endswith(".parquet"):
                files.append(os.path.join(root, fn))
    if not files:
        return None
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# Chart functions
# ─────────────────────────────────────────────────────────────────────────────

def chart_funnel(df):
    """Bar chart: view → cart → purchase event counts and conversion rates."""
    vc = df["event_type"].value_counts()
    views     = vc.get("view", 0)
    carts     = vc.get("cart", 0)
    purchases = vc.get("purchase", 0)

    if views == 0:
        print("  [WARN] No view events found — skipping funnel chart.")
        return

    stages = ["View", "Add to Cart", "Purchase"]
    counts = [views, carts, purchases]
    pcts   = [100, carts/views*100 if views else 0, purchases/views*100 if views else 0]

    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.bar(stages, counts,
                  color=[COLORS["blue"], COLORS["orange"], COLORS["green"]],
                  width=0.55, edgecolor="white", linewidth=1.5)
    for bar, c, p in zip(bars, counts, pcts):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + views*0.01,
                f"{c:,}\n({p:.1f}%)", ha="center", va="bottom",
                fontsize=10, fontweight="bold")

    v2c = carts/views*100 if views else 0
    c2p = purchases/carts*100 if carts else 0
    ax.annotate(f"{v2c:.1f}% convert", xy=(0.5, carts), fontsize=9,
                ha="center", color=COLORS["grey"])
    ax.annotate(f"{c2p:.1f}% convert", xy=(1.5, purchases), fontsize=9,
                ha="center", color=COLORS["grey"])

    ax.set_ylabel("Event Count")
    ax.set_title("E-commerce Conversion Funnel\n(View → Cart → Purchase)")
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1e3:.0f}K"))
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    path = os.path.join(OUT_DIR, "funnel_dropoff.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved → {path}")


def chart_attribution(df):
    """Horizontal bar: revenue by referrer channel."""
    if "total_revenue" not in df.columns or "attributed_referrer" not in df.columns:
        print("  [WARN] Unexpected attribution_summary schema — skipping chart.")
        return

    df = df.sort_values("total_revenue", ascending=True)
    channel_colors = {
        "google": "#4285F4", "facebook": "#1877F2",
        "instagram": "#E4405F", "email": COLORS["orange"],
        "affiliate": COLORS["purple"],
    }
    colors = [channel_colors.get(r, COLORS["grey"]) for r in df["attributed_referrer"]]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    bars = ax.barh(df["attributed_referrer"], df["total_revenue"],
                   color=colors, edgecolor="white", linewidth=1.5, height=0.55)
    for bar, rev, cnt in zip(bars, df["total_revenue"], df["order_count"]):
        ax.text(bar.get_width() + df["total_revenue"].max()*0.01,
                bar.get_y() + bar.get_height()/2,
                f"${rev:,.0f}  ({cnt:,} orders)", va="center", fontsize=9)

    ax.set_xlabel("Total Attributed Revenue ($)")
    ax.set_title("Last-Touch Attribution\nRevenue by Referrer Channel")
    ax.xaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M" if x >= 1e6 else f"${x/1e3:.0f}K"))
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.set_xlim(0, df["total_revenue"].max() * 1.35)
    fig.tight_layout()
    path = os.path.join(OUT_DIR, "attribution_revenue.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved → {path}")


def chart_anomalies(df):
    """Line chart: daily purchase counts with anomaly markers."""
    if "date" not in df.columns or "purchase_count" not in df.columns:
        print("  [WARN] Unexpected daily_anomalies schema — skipping chart.")
        return

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.plot(df["date"], df["purchase_count"], marker=".", linewidth=1.5,
            color=COLORS["blue"], label="Daily purchases", zorder=2)

    if "rolling_avg" in df.columns:
        valid = df[df["rolling_avg"].notna()]
        ax.plot(valid["date"], valid["rolling_avg"], linewidth=2,
                color=COLORS["grey"], linestyle="--", alpha=0.7,
                label="7-day rolling avg", zorder=1)

    anom = df[df["is_anomaly"] == True]
    spikes = anom[anom["z_score"] > 0]
    drops  = anom[anom["z_score"] < 0]

    if len(spikes):
        ax.scatter(spikes["date"], spikes["purchase_count"], color=COLORS["red"],
                   s=80, zorder=3, label=f"SPIKE ({len(spikes)})",
                   edgecolors="darkred", linewidth=1)
    if len(drops):
        ax.scatter(drops["date"], drops["purchase_count"], color=COLORS["orange"],
                   s=80, zorder=3, label=f"DROP ({len(drops)})",
                   edgecolors="darkorange", linewidth=1, marker="v")

    for _, row in anom.head(4).iterrows():
        short = "SPIKE" if row["z_score"] > 0 else "DROP"
        pct   = (abs(row["purchase_count"] - row["rolling_avg"]) / row["rolling_avg"] * 100
                 if row.get("rolling_avg") else 0)
        ax.annotate(f"{short}\n{pct:.0f}%",
                    xy=(row["date"], row["purchase_count"]),
                    xytext=(0, 15), textcoords="offset points",
                    fontsize=7, ha="center", fontweight="bold",
                    color=COLORS["red"] if row["z_score"] > 0 else COLORS["orange"])

    ax.set_xlabel("Date")
    ax.set_ylabel("Purchase Count")
    ax.set_title("Daily Purchase Volume with Anomaly Detection\n"
                 "(7-day rolling baseline, z-score > 2)")
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.autofmt_xdate()
    fig.tight_layout()
    path = os.path.join(OUT_DIR, "anomaly_timeline.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved → {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Summary table printers
# ─────────────────────────────────────────────────────────────────────────────

def print_header(title):
    w = 60
    print("\n" + "═"*w)
    print(f"  {title}")
    print("═"*w)


def print_funnel_summary(df):
    print_header("FUNNEL ANALYSIS RESULTS")
    vc = df["event_type"].value_counts()
    views     = vc.get("view", 0)
    carts     = vc.get("cart", 0)
    purchases = vc.get("purchase", 0)
    print(f"  Total events       : {len(df):>12,}")
    print(f"  Views              : {views:>12,}")
    print(f"  Add-to-cart        : {carts:>12,}  ({carts/views*100:.2f}% of views)")
    print(f"  Purchases          : {purchases:>12,}  ({purchases/views*100:.2f}% of views)")
    print(f"  Cart → Purchase    : {purchases/carts*100:>11.2f}%")


def print_attribution_summary(df):
    print_header("LAST-TOUCH ATTRIBUTION RESULTS")
    if df is None or df.empty:
        print("  No data.")
        return
    total_rev    = df["total_revenue"].sum()
    total_orders = df["order_count"].sum()
    print(f"  {'Channel':<14} {'Orders':>10} {'Revenue':>14} {'Share':>8}")
    print(f"  {'-'*14} {'-'*10} {'-'*14} {'-'*8}")
    for _, row in df.sort_values("total_revenue", ascending=False).iterrows():
        share = row["total_revenue"] / total_rev * 100 if total_rev else 0
        print(f"  {row['attributed_referrer']:<14} {row['order_count']:>10,} "
              f"  ${row['total_revenue']:>11,.2f}  {share:>6.1f}%")
    print(f"  {'TOTAL':<14} {total_orders:>10,}  ${total_rev:>11,.2f}")


def print_anomaly_summary(df):
    print_header("ANOMALY DETECTION RESULTS")
    if df is None or df.empty:
        print("  No data.")
        return
    n_days   = len(df)
    n_anom   = df["is_anomaly"].sum()
    n_spikes = (df["z_score"] > 0) & df["is_anomaly"]
    n_drops  = (df["z_score"] < 0) & df["is_anomaly"]
    print(f"  Days analysed      : {n_days}")
    print(f"  Anomalies flagged  : {int(n_anom)}  "
          f"(SPIKE: {int(n_spikes.sum())}  DROP: {int(n_drops.sum())})")
    if n_anom:
        print(f"\n  {'Date':<12} {'Count':>8} {'Z-score':>9} {'Type':<7} Explanation")
        print(f"  {'-'*12} {'-'*8} {'-'*9} {'-'*7} {'-'*30}")
        for _, row in df[df["is_anomaly"]].sort_values("z_score", key=abs, ascending=False).head(10).iterrows():
            kind = "SPIKE" if row["z_score"] > 0 else "DROP"
            expl = str(row.get("explanation", ""))[:50]
            print(f"  {str(row['date']):<12} {int(row['purchase_count']):>8,} "
                  f"  {row['z_score']:>+7.2f}  {kind:<7} {expl}")


def print_funnel_rates_summary(df):
    """Print top-10 funnel conversion rows by category."""
    print_header("FUNNEL CONVERSION RATES (top categories)")
    if df is None or df.empty:
        print("  No data.")
        return
    cols = [c for c in ["category_code", "date", "view_count", "cart_count",
                         "purchase_count", "view_to_cart_rate", "cart_to_purchase_rate"]
            if c in df.columns]
    print(df[cols].sort_values("purchase_count", ascending=False).head(15).to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Download GCS pipeline output and display analytics results")
    parser.add_argument("--gcs-output", required=True,
                        help="GCS path written by cloud_pipeline.py "
                             "(e.g. gs://my-bucket/results/demo_run)")
    parser.add_argument("--no-charts", action="store_true",
                        help="Skip chart generation (tables only)")
    args = parser.parse_args()

    gcs_base = args.gcs_output.rstrip("/")
    gsutil   = _find_gsutil()
    os.makedirs(OUT_DIR, exist_ok=True)

    print(f"\nDownloading pipeline results from: {gcs_base}")
    print(f"Charts will be saved to          : {OUT_DIR}/\n")

    tmpdir = tempfile.mkdtemp(prefix="dss_results_")

    try:
        # ── 1. Enriched events (for funnel raw counts) ────────────────────
        enriched_local = os.path.join(tmpdir, "enriched_events")
        print("Downloading enriched_events ...")
        ok = download_parquet(gsutil, f"{gcs_base}/enriched_events", enriched_local)
        enriched_df = read_parquet_dir(enriched_local) if ok else None

        # ── 2. Funnel daily metrics ───────────────────────────────────────
        funnel_local = os.path.join(tmpdir, "funnel_daily")
        print("Downloading funnel_metrics_daily ...")
        ok2 = download_parquet(gsutil, f"{gcs_base}/funnel_metrics_daily", funnel_local)
        funnel_df = read_parquet_dir(funnel_local) if ok2 else None

        # ── 3. Attribution summary ────────────────────────────────────────
        attr_local = os.path.join(tmpdir, "attribution_summary")
        print("Downloading attribution_summary ...")
        ok3 = download_parquet(gsutil, f"{gcs_base}/attribution/attribution_summary", attr_local)
        attr_df = read_parquet_dir(attr_local) if ok3 else None

        # ── 4. Daily anomalies ────────────────────────────────────────────
        anom_local = os.path.join(tmpdir, "daily_anomalies")
        print("Downloading daily_anomalies ...")
        ok4 = download_parquet(gsutil, f"{gcs_base}/anomalies/daily_anomalies", anom_local)
        anom_df = read_parquet_dir(anom_local) if ok4 else None

        # ── Print summary tables ──────────────────────────────────────────
        if enriched_df is not None:
            print_funnel_summary(enriched_df)
        if funnel_df is not None:
            print_funnel_rates_summary(funnel_df)
        if attr_df is not None:
            print_attribution_summary(attr_df)
        if anom_df is not None:
            print_anomaly_summary(anom_df)

        # ── Generate charts ───────────────────────────────────────────────
        if not args.no_charts:
            print_header("GENERATING CHARTS")
            if enriched_df is not None:
                print("  Funnel drop-off chart ...")
                chart_funnel(enriched_df)
            if attr_df is not None:
                print("  Attribution revenue chart ...")
                chart_attribution(attr_df)
            if anom_df is not None:
                print("  Anomaly timeline chart ...")
                chart_anomalies(anom_df)

            print(f"\n  All charts saved to: {OUT_DIR}/")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    print("\n" + "═"*60)
    print("  Analysis complete.")
    print("═"*60 + "\n")


if __name__ == "__main__":
    main()
