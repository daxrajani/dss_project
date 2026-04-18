"""
dashboard_charts.py — Generate static PNG analytics charts from GCS pipeline output.

Reads Parquet files from GCS (or local /tmp/dss_results), produces 3 dark-themed
PNG charts, saves them to --local-dir, and optionally uploads to GCS.

Usage (called by the dashboard after pipeline completes):
    python src/dashboard_charts.py \
        --gcs-output gs://dss-project-dax/results/demo_run \
        --local-dir /tmp/dss_charts
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
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

# ── Dark theme to match dashboard ────────────────────────────────────────────
plt.rcParams.update({
    "figure.facecolor":  "#0d1117",
    "axes.facecolor":    "#161b22",
    "axes.edgecolor":    "#30363d",
    "axes.labelcolor":   "#8b949e",
    "text.color":        "#c9d1d9",
    "xtick.color":       "#8b949e",
    "ytick.color":       "#8b949e",
    "grid.color":        "#21262d",
    "legend.facecolor":  "#161b22",
    "legend.edgecolor":  "#30363d",
    "font.size":         10,
    "axes.titlesize":    11,
    "axes.titleweight":  "bold",
})

C_BLUE   = "#58a6ff"
C_GREEN  = "#3fb950"
C_PURPLE = "#bc8cff"
C_YELLOW = "#e3b341"
C_RED    = "#f85149"
C_MUTED  = "#8b949e"

REF_COLORS = {
    "google":    "#4285F4",
    "facebook":  "#1877F2",
    "instagram": "#E1306C",
    "email":     "#e3b341",
    "affiliate": "#3fb950",
    "direct":    "#8b949e",
}


def _fmt(x):
    if x >= 1_000_000: return f"{x/1e6:.1f}M"
    if x >= 1_000:     return f"{x/1e3:.0f}K"
    return str(int(x))


# ── GCS helpers ───────────────────────────────────────────────────────────────

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


def _download(gsutil, gcs_path, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    # rsync requires destination to exist (created above)
    r = subprocess.run(
        [gsutil, "-m", "rsync", "-r",
         gcs_path.rstrip("/") + "/",
         local_dir.rstrip("/") + "/"],
        capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  [WARN] download failed for {gcs_path}: {r.stderr.strip()[:150]}")
    return r.returncode == 0


def _read(local_dir):
    files = [os.path.join(r, f)
             for r, _, fs in os.walk(local_dir)
             for f in fs if f.endswith(".parquet")]
    if not files:
        return None
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _upload(gsutil, local_path, gcs_dir):
    subprocess.run([gsutil, "cp", local_path, gcs_dir + "/"],
                   capture_output=True)


# ── Chart 1 — Funnel ──────────────────────────────────────────────────────────

def make_funnel_chart(funnel_df, out_path):
    daily = (funnel_df
             .groupby("date", as_index=False)
             .agg(views=("total_sessions_with_view", "sum"),
                  carts=("total_sessions_with_cart", "sum"),
                  purchases=("total_sessions_with_purchase", "sum"))
             .sort_values("date"))

    by_device = (funnel_df
                 .groupby("device", as_index=False)
                 .agg(views=("total_sessions_with_view", "sum"),
                      carts=("total_sessions_with_cart", "sum"),
                      purchases=("total_sessions_with_purchase", "sum")))

    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle("Funnel Analysis — Oct 2019 (8.5 M rows)", fontsize=13, color="#c9d1d9")

    # ── Left: overall funnel bar ──────────────────────────────────────────────
    ax = axes[0]
    totV = int(daily["views"].sum())
    totC = int(daily["carts"].sum())
    totP = int(daily["purchases"].sum())
    v2c  = totC / totV * 100 if totV else 0
    c2p  = totP / totC * 100 if totC else 0

    bars = ax.bar(["Views", "Carts", "Purchases"], [totV, totC, totP],
                  color=["#1f3a5c", "#1a4731", "#3d2a6e"],
                  edgecolor=[C_BLUE, C_GREEN, C_PURPLE],
                  linewidth=1.5, width=0.55)
    for bar, v in zip(bars, [totV, totC, totP]):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + totV * 0.015,
                _fmt(v), ha="center", va="bottom",
                fontsize=10, fontweight="bold", color="#c9d1d9")
    ax.set_title(f"Overall Funnel\nV→C: {v2c:.1f}%  ·  C→P: {c2p:.1f}%")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _fmt(x)))
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    # ── Middle: daily conversion rates ───────────────────────────────────────
    ax2 = axes[1]
    daily["v2c"] = (daily["carts"] / daily["views"].replace(0, np.nan) * 100).fillna(0)
    daily["c2p"] = (daily["purchases"] / daily["carts"].replace(0, np.nan) * 100).fillna(0)
    xs = range(len(daily))
    ax2.plot(xs, daily["v2c"], color=C_BLUE,  linewidth=1.8,
             label="View→Cart %", marker=".", markersize=4)
    ax2.plot(xs, daily["c2p"], color=C_GREEN, linewidth=1.8,
             label="Cart→Buy %",  marker=".", markersize=4)
    step = max(1, len(daily) // 8)
    ax2.set_xticks(range(0, len(daily), step))
    ax2.set_xticklabels(
        [str(daily["date"].iloc[i])[-5:] for i in range(0, len(daily), step)],
        rotation=45, fontsize=8)
    ax2.set_ylabel("Conversion Rate (%)")
    ax2.set_title("Daily Conversion Rates")
    ax2.legend(fontsize=9)
    ax2.grid(linestyle="--", alpha=0.3)

    # ── Right: by device ─────────────────────────────────────────────────────
    ax3 = axes[2]
    devs   = by_device["device"].tolist()
    x_pos  = np.arange(len(devs))
    w      = 0.25
    dcols  = [C_BLUE, C_GREEN, C_PURPLE]
    for i, (col_name, color) in enumerate(zip(["views", "carts", "purchases"], dcols)):
        bars3 = ax3.bar(x_pos + i * w, by_device[col_name], w,
                        label=col_name.capitalize(),
                        color=color, alpha=0.85, edgecolor="#0d1117", linewidth=0.8)
    ax3.set_xticks(x_pos + w)
    ax3.set_xticklabels(devs, fontsize=9)
    ax3.set_title("Funnel by Device")
    ax3.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _fmt(x)))
    ax3.legend(fontsize=8)
    ax3.grid(axis="y", linestyle="--", alpha=0.3)

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(out_path, dpi=130, bbox_inches="tight", facecolor="#0d1117")
    plt.close(fig)
    print(f"  Saved: {out_path}")


# ── Chart 2 — Attribution ─────────────────────────────────────────────────────

def make_attribution_chart(attr_df, out_path):
    by_ref = (attr_df
              .groupby("attributed_referrer", as_index=False)
              .agg(orders=("order_id", "count"),
                   revenue=("order_amount", "sum"))
              .sort_values("revenue", ascending=True))

    by_dev = (attr_df
              .groupby("attribution_device", as_index=False)
              .agg(orders=("order_id", "count"),
                   revenue=("order_amount", "sum"))
              .sort_values("revenue", ascending=False))

    attr2 = attr_df.copy()
    attr2["day"] = pd.to_datetime(attr2["order_time"]).dt.date.astype(str)
    rev_daily = (attr2.groupby("day", as_index=False)
                 .agg(revenue=("order_amount", "sum"))
                 .sort_values("day"))

    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle("Last-Touch Attribution Analysis", fontsize=13, color="#c9d1d9")

    # ── Left: revenue by referrer ─────────────────────────────────────────────
    ax = axes[0]
    colors = [REF_COLORS.get(r, C_BLUE) for r in by_ref["attributed_referrer"]]
    ax.barh(by_ref["attributed_referrer"], by_ref["revenue"],
            color=colors, edgecolor="#0d1117", linewidth=1, height=0.6)
    max_rev = by_ref["revenue"].max()
    for _, row in by_ref.iterrows():
        ax.text(row["revenue"] + max_rev * 0.01,
                by_ref[by_ref["attributed_referrer"] == row["attributed_referrer"]].index[0],
                f"${row['revenue']:,.0f}  ({row['orders']:,})",
                va="center", fontsize=8.5, color="#c9d1d9")
    ax.set_xlabel("Revenue ($)")
    ax.set_title("Revenue by Referrer Channel")
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K" if x < 1e6 else f"${x/1e6:.1f}M"))
    ax.set_xlim(0, max_rev * 1.5)
    ax.grid(axis="x", linestyle="--", alpha=0.3)
    ax.set_yticks(range(len(by_ref)))
    ax.set_yticklabels(by_ref["attributed_referrer"])

    # ── Middle: by device bar ─────────────────────────────────────────────────
    ax2 = axes[1]
    dev_colors = [C_BLUE, C_GREEN, C_PURPLE]
    bars2 = ax2.bar(by_dev["attribution_device"], by_dev["revenue"],
                    color=dev_colors[:len(by_dev)],
                    edgecolor="#0d1117", linewidth=1, width=0.55)
    for bar, rev in zip(bars2, by_dev["revenue"]):
        ax2.text(bar.get_x() + bar.get_width() / 2,
                 bar.get_height() + by_dev["revenue"].max() * 0.015,
                 f"${rev:,.0f}", ha="center", va="bottom", fontsize=9, color="#c9d1d9")
    ax2.set_title("Revenue by Device")
    ax2.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K" if x < 1e6 else f"${x/1e6:.1f}M"))
    ax2.grid(axis="y", linestyle="--", alpha=0.3)

    # ── Right: daily revenue line ─────────────────────────────────────────────
    ax3 = axes[2]
    xs = range(len(rev_daily))
    ax3.plot(xs, rev_daily["revenue"], color=C_YELLOW, linewidth=1.8, marker=".", markersize=4)
    ax3.fill_between(xs, rev_daily["revenue"], alpha=0.12, color=C_YELLOW)
    step = max(1, len(rev_daily) // 8)
    ax3.set_xticks(range(0, len(rev_daily), step))
    ax3.set_xticklabels(
        [str(rev_daily["day"].iloc[i])[-5:] for i in range(0, len(rev_daily), step)],
        rotation=45, fontsize=8)
    ax3.set_ylabel("Revenue ($)")
    ax3.set_title("Daily Attributed Revenue")
    ax3.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K" if x < 1e6 else f"${x/1e6:.1f}M"))
    ax3.grid(linestyle="--", alpha=0.3)

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(out_path, dpi=130, bbox_inches="tight", facecolor="#0d1117")
    plt.close(fig)
    print(f"  Saved: {out_path}")


# ── Chart 3 — Anomalies ───────────────────────────────────────────────────────

def make_anomaly_chart(anom_df, cat_anom_df, out_path):
    anom_df = anom_df.copy().sort_values("date").reset_index(drop=True)

    fig, axes = plt.subplots(1, 2, figsize=(15, 5))
    fig.suptitle("Anomaly Detection — Daily Purchase Volume (7-day rolling baseline)",
                 fontsize=13, color="#c9d1d9")

    # ── Left: daily line + anomaly markers ───────────────────────────────────
    ax = axes[0]
    xs = range(len(anom_df))
    ax.plot(xs, anom_df["purchase_count"], color=C_BLUE, linewidth=1.8,
            marker=".", markersize=3, label="Daily purchases", zorder=2)
    ax.fill_between(xs, anom_df["purchase_count"], alpha=0.08, color=C_BLUE)

    if "rolling_avg" in anom_df.columns:
        valid = anom_df["rolling_avg"].notna()
        vx = [i for i, v in enumerate(valid) if v]
        vy = anom_df.loc[valid, "rolling_avg"].values
        ax.plot(vx, vy, color=C_MUTED, linewidth=1.5,
                linestyle="--", alpha=0.8, label="7-day rolling avg", zorder=1)

    anom_mask = anom_df["is_anomaly"] == True
    spikes = anom_df[anom_mask & (anom_df["z_score"] > 0)]
    drops  = anom_df[anom_mask & (anom_df["z_score"] < 0)]
    if len(spikes):
        ax.scatter(spikes.index, spikes["purchase_count"],
                   color=C_RED, s=80, zorder=5,
                   edgecolors="#0d1117", linewidth=1,
                   label=f"Spike ({len(spikes)})")
    if len(drops):
        ax.scatter(drops.index, drops["purchase_count"],
                   color=C_YELLOW, s=80, zorder=5, marker="v",
                   edgecolors="#0d1117", linewidth=1,
                   label=f"Drop ({len(drops)})")

    # Annotate up to 5 anomalies
    for i, (idx, row) in enumerate(anom_df[anom_mask].head(5).iterrows()):
        kind  = "SPIKE" if row["z_score"] > 0 else "DROP"
        color = C_RED if row["z_score"] > 0 else C_YELLOW
        pct   = (abs(row["purchase_count"] - row["rolling_avg"]) / row["rolling_avg"] * 100
                 if row.get("rolling_avg") else 0)
        ax.annotate(f"{kind}\n{pct:.0f}%",
                    xy=(idx, row["purchase_count"]),
                    xytext=(0, 14), textcoords="offset points",
                    fontsize=7, ha="center", fontweight="bold", color=color)

    step = max(1, len(anom_df) // 8)
    ax.set_xticks(range(0, len(anom_df), step))
    ax.set_xticklabels(
        [str(anom_df["date"].iloc[i])[-5:] for i in range(0, len(anom_df), step)],
        rotation=45, fontsize=8)
    ax.set_ylabel("Purchases / Day")
    ax.set_title(f"Daily Purchases + {int(anom_mask.sum())} Anomalies Detected")
    ax.legend(fontsize=9, loc="upper left")
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    # ── Right: top categories by anomaly count ────────────────────────────────
    ax2 = axes[1]
    cat_summary = (cat_anom_df[cat_anom_df["is_anomaly"] == True]
                   .groupby("category_code", as_index=False)
                   .size()
                   .rename(columns={"size": "count"})
                   .sort_values("count", ascending=True)
                   .tail(10))
    labels = [c.split(".")[-1] if "." in str(c) else str(c)
              for c in cat_summary["category_code"]]
    ax2.barh(labels, cat_summary["count"],
             color=C_RED, edgecolor="#0d1117", linewidth=1, height=0.6, alpha=0.8)
    for i, (_, row) in enumerate(cat_summary.iterrows()):
        ax2.text(row["count"] + 0.05, i,
                 str(int(row["count"])), va="center", fontsize=9, color="#c9d1d9")
    ax2.set_xlabel("Anomaly Days")
    ax2.set_title("Top Categories by Anomaly Count")
    ax2.grid(axis="x", linestyle="--", alpha=0.3)

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(out_path, dpi=130, bbox_inches="tight", facecolor="#0d1117")
    plt.close(fig)
    print(f"  Saved: {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Generate PNG analytics charts from GCS pipeline output")
    ap.add_argument("--gcs-output", required=True,
                    help="GCS base path e.g. gs://bucket/results/demo_run")
    ap.add_argument("--local-dir",  default="/tmp/dss_charts",
                    help="Local directory to save PNG files (default: /tmp/dss_charts)")
    ap.add_argument("--upload", action="store_true",
                    help="Upload PNGs back to GCS after generating")
    args = ap.parse_args()

    gcs_base = args.gcs_output.rstrip("/")
    out_dir  = args.local_dir
    os.makedirs(out_dir, exist_ok=True)

    gsutil = _find_gsutil()
    tmpdir = tempfile.mkdtemp(prefix="dss_chart_")

    print(f"\n[charts] Source : {gcs_base}")
    print(f"[charts] Output : {out_dir}\n")

    try:
        # Download parquet directories
        funnel_local = os.path.join(tmpdir, "funnel")
        attr_local   = os.path.join(tmpdir, "attr")
        anom_local   = os.path.join(tmpdir, "anom")
        cat_local    = os.path.join(tmpdir, "cat")

        print("Downloading funnel_metrics_daily ...")
        _download(gsutil, f"{gcs_base}/funnel_metrics_daily", funnel_local)
        print("Downloading attributed_orders ...")
        _download(gsutil, f"{gcs_base}/attributed_orders",    attr_local)
        print("Downloading daily_anomalies ...")
        _download(gsutil, f"{gcs_base}/daily_anomalies",      anom_local)
        print("Downloading category_anomalies ...")
        _download(gsutil, f"{gcs_base}/category_anomalies",   cat_local)

        funnel_df = _read(funnel_local)
        attr_df   = _read(attr_local)
        anom_df   = _read(anom_local)
        cat_df    = _read(cat_local)

        print("\nGenerating charts ...")
        generated = []

        if funnel_df is not None:
            p = os.path.join(out_dir, "funnel.png")
            make_funnel_chart(funnel_df, p)
            generated.append(p)

        if attr_df is not None:
            p = os.path.join(out_dir, "attribution.png")
            make_attribution_chart(attr_df, p)
            generated.append(p)

        if anom_df is not None and cat_df is not None:
            p = os.path.join(out_dir, "anomalies.png")
            make_anomaly_chart(anom_df, cat_df, p)
            generated.append(p)

        if args.upload and generated:
            print("\nUploading PNGs to GCS ...")
            for p in generated:
                _upload(gsutil, p, f"{gcs_base}/charts")
            print(f"  Done → {gcs_base}/charts/")

        print(f"\n[charts] Done — {len(generated)} charts saved to {out_dir}/")
        return 0

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
