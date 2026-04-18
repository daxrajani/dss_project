"""
DSS Project — Live Demo Dashboard
COEN 6731 Distributed Systems · Concordia University
Dax Rajani (40294118) & Harsh Ahuja (40301748)
"""

from flask import Flask, render_template, Response, jsonify, stream_with_context, request
import subprocess, json, os, re, math, requests, time, threading, shutil
from datetime import datetime, timedelta, timezone

app = Flask(__name__)


# ── Resolve gcloud / gsutil ───────────────────────────────────────────
def _find_tool(name):
    """Find gcloud/gsutil: PATH first, then common install locations."""
    found = shutil.which(name)
    if found:
        return found
    candidates = [
        os.path.expanduser(f"~/google-cloud-sdk/bin/{name}"),
        f"/usr/lib/google-cloud-sdk/bin/{name}",
        f"/snap/bin/{name}",
        f"/usr/local/bin/{name}",
        f"/usr/bin/{name}",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    return name   # fall back to bare name — will fail with a clear error


GCLOUD  = _find_tool("gcloud")
GSUTIL  = _find_tool("gsutil")

# ── GCP config — override via environment variables ───────────────────
# Set these before running:
#   export GCP_PROJECT=your-project-id
#   export GCS_BUCKET=gs://your-bucket
#   export DATAPROC_CLUSTER=your-cluster-name
#   export GCP_REGION=us-central1
#   export GCP_ZONE=us-central1-f
PROJECT = os.environ.get("GCP_PROJECT", "")
REGION  = os.environ.get("GCP_REGION",  "us-central1")
ZONE    = os.environ.get("GCP_ZONE",    "us-central1-f")
CLUSTER = os.environ.get("DATAPROC_CLUSTER", "")
BUCKET  = os.environ.get("GCS_BUCKET",  "")
INPUT   = os.environ.get("GCS_INPUT",   f"{BUCKET}/data/2019-Oct.csv")
SCRIPTS = os.environ.get("GCS_SCRIPTS", f"{BUCKET}/scripts")
RESULTS_LOCAL = os.environ.get("DSS_RESULTS_DIR", "/tmp/dss_results")

MONITORING_BASE = "https://monitoring.googleapis.com/v3"

# Shared resize state (updated by background thread)
_resize_state = {"status": "idle", "workers": None, "error": None, "error_type": None}


# ── Helpers ───────────────────────────────────────────────────────────

def _gcloud_token():
    r = subprocess.run([GCLOUD, "auth", "print-access-token"],
                       capture_output=True, text=True)
    return r.stdout.strip()


def _monitoring_get(params: dict) -> dict:
    url = f"{MONITORING_BASE}/projects/{PROJECT}/timeSeries"
    headers = {"Authorization": f"Bearer {_gcloud_token()}"}
    resp = requests.get(url, params=params, headers=headers, timeout=15)
    return resp.json()


def _nan_safe(val):
    """Convert NaN/Inf floats to None for JSON serialisation."""
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _df_to_records(df):
    """Convert DataFrame to list-of-dicts with NaN → None."""
    import pandas as pd
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


# ── Pages ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", cluster=CLUSTER, zone=ZONE)


# ── Cluster nodes ─────────────────────────────────────────────────────

@app.route("/api/nodes")
def nodes():
    try:
        r = subprocess.run(
            [GCLOUD, "compute", "instances", "list",
             f"--filter=name~{CLUSTER}",
             "--format=json",
             f"--project={PROJECT}"],
            capture_output=True, text=True)
    except FileNotFoundError:
        return jsonify({"error": f"gcloud not found at: {GCLOUD}"}), 500
    try:
        instances = json.loads(r.stdout or "[]")
    except json.JSONDecodeError:
        return jsonify([])

    return jsonify([{
        "name":    inst["name"],
        "status":  inst["status"],
        "type":    "master" if inst["name"].endswith("-m") else "worker",
        "machine": inst["machineType"].split("/")[-1],
        "zone":    inst.get("zone", "").split("/")[-1],
    } for inst in instances])


# ── VM metrics (Cloud Monitoring) ─────────────────────────────────────

@app.route("/api/vm-metrics")
def vm_metrics():
    try:
        now   = datetime.now(timezone.utc)
        start = (now - timedelta(minutes=25)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end   = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        data = _monitoring_get({
            "filter": (
                'metric.type="compute.googleapis.com/instance/cpu/utilization"'
                f' AND resource.labels.zone="{ZONE}"'
            ),
            "interval.startTime": start,
            "interval.endTime":   end,
            "aggregation.alignmentPeriod":  "60s",
            "aggregation.perSeriesAligner": "ALIGN_MEAN",
        })

        series = {}
        for ts in data.get("timeSeries", []):
            name = ts["metric"]["labels"].get("instance_name", "")
            if not name.startswith(CLUSTER):
                continue
            short = name.replace(f"{CLUSTER}-", "")
            points = sorted(
                [{"t": p["interval"]["endTime"],
                  "v": round(p["value"].get("doubleValue", 0) * 100, 2)}
                 for p in ts["points"]],
                key=lambda x: x["t"])
            series[short] = points

        # Also fetch disk write throughput (bytes/s)
        disk_data = _monitoring_get({
            "filter": (
                'metric.type="compute.googleapis.com/instance/disk/write_bytes_count"'
                f' AND resource.labels.zone="{ZONE}"'
            ),
            "interval.startTime": start,
            "interval.endTime":   end,
            "aggregation.alignmentPeriod":  "60s",
            "aggregation.perSeriesAligner": "ALIGN_RATE",
        })

        disk = {}
        for ts in disk_data.get("timeSeries", []):
            name = ts["metric"]["labels"].get("instance_name", "")
            if not name.startswith(CLUSTER):
                continue
            short = name.replace(f"{CLUSTER}-", "")
            pts = sorted(
                [{"t": p["interval"]["endTime"],
                  "v": round(p["value"].get("doubleValue", 0) / 1024, 1)}  # KB/s
                 for p in ts["points"]],
                key=lambda x: x["t"])
            disk[short] = pts

        return jsonify({"cpu": series, "disk": disk})
    except Exception as e:
        return jsonify({"error": str(e), "cpu": {}, "disk": {}}), 200


# ── Cluster resize ────────────────────────────────────────────────────

@app.route("/api/resize", methods=["POST"])
def resize_cluster():
    """Start a background resize and return immediately. Poll /api/resize-status for result."""
    global _resize_state
    data = request.get_json(silent=True) or {}
    n = max(1, min(5, int(data.get("workers", 5))))

    if _resize_state["status"] == "running":
        return jsonify({"error": "Resize already in progress"}), 409

    _resize_state = {"status": "running", "workers": n, "error": None, "error_type": None}

    def _do_resize():
        global _resize_state
        r = subprocess.run(
            [GCLOUD, "dataproc", "clusters", "update", CLUSTER,
             f"--num-workers={n}",
             f"--region={REGION}",
             f"--project={PROJECT}"],          # synchronous — waits for completion
            capture_output=True, text=True, timeout=360)
        if r.returncode != 0:
            err = r.stderr.strip()
            etype = ("capacity"
                     if "does not have enough resources" in err or "UNAVAILABLE" in err
                     else "unknown")
            _resize_state = {"status": "failed", "workers": n,
                             "error": err, "error_type": etype}
        else:
            _resize_state = {"status": "done", "workers": n,
                             "error": None, "error_type": None}

    threading.Thread(target=_do_resize, daemon=True).start()
    return jsonify({"ok": True, "workers": n})


@app.route("/api/resize-status")
def resize_status():
    return jsonify(_resize_state)


@app.route("/api/cluster-workers")
def cluster_workers():
    """Return actual current running worker count from Dataproc."""
    r = subprocess.run(
        [GCLOUD, "dataproc", "clusters", "describe", CLUSTER,
         f"--region={REGION}", f"--project={PROJECT}",
         "--format=value(config.workerConfig.numInstances,status.state)"],
        capture_output=True, text=True)
    parts = r.stdout.strip().split()
    workers = int(parts[0]) if parts else 0
    state   = parts[1] if len(parts) > 1 else "UNKNOWN"
    return jsonify({"workers": workers, "state": state})


# ── Last-run metadata ─────────────────────────────────────────────────

@app.route("/api/last-run-info")
def last_run_info():
    """Return timestamp of last downloaded results, if any."""
    path = os.path.join(RESULTS_LOCAL, "daily_anomalies")
    if os.path.isdir(path) and os.listdir(path):
        mtime = max(os.path.getmtime(os.path.join(dp, f))
                    for dp, _, files in os.walk(path) for f in files)
        return jsonify({"exists": True, "ts": mtime,
                        "human": datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")})
    return jsonify({"exists": False})


# ── Job submission ────────────────────────────────────────────────────

@app.route("/api/submit", methods=["POST"])
def submit():
    data     = request.get_json(silent=True) or {}
    fraction = float(data.get("fraction", 0.2))
    fraction = max(0.05, min(1.0, fraction))   # clamp to [5%, 100%]

    r = subprocess.run(
        [GCLOUD, "dataproc", "jobs", "submit", "pyspark",
         f"{SCRIPTS}/cloud_pipeline.py",
         f"--cluster={CLUSTER}",
         f"--region={REGION}",
         f"--project={PROJECT}",
         "--async",
         "--format=json",
         "--",
         "--input",  INPUT,
         "--output", f"{BUCKET}/results/demo_run",
         "--sample-fraction", str(fraction)],
        capture_output=True, text=True)

    if r.returncode != 0:
        return jsonify({"error": r.stderr.strip()}), 500

    try:
        job_data = json.loads(r.stdout)
        job_id = job_data["reference"]["jobId"]
    except (json.JSONDecodeError, KeyError):
        m = re.search(r"Job \[(\w+)\]", r.stderr)
        if not m:
            return jsonify({"error": "Could not parse job ID"}), 500
        job_id = m.group(1)

    return jsonify({"job_id": job_id, "fraction": fraction})


# ── Log streaming (SSE) ───────────────────────────────────────────────

@app.route("/api/logs/<job_id>")
def stream_logs(job_id):
    @stream_with_context
    def generate():
        proc = subprocess.Popen(
            [GCLOUD, "dataproc", "jobs", "wait", job_id,
             f"--region={REGION}",
             f"--project={PROJECT}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True, bufsize=1,
            env={**os.environ, "PYTHONUNBUFFERED": "1"})

        for line in iter(proc.stdout.readline, ""):
            line = line.rstrip("\n")
            if line:
                yield f"data: {json.dumps({'line': line})}\n\n"

        proc.wait()
        status = "SUCCESS" if proc.returncode == 0 else "FAILED"
        yield f"data: {json.dumps({'done': True, 'status': status})}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no",
                             "Connection": "keep-alive"})


# ── VM control ────────────────────────────────────────────────────────

@app.route("/api/stop/<name>", methods=["POST"])
def stop_vm(name):
    if not name.startswith(CLUSTER + "-w-"):
        return jsonify({"error": "Can only stop worker nodes"}), 403
    r = subprocess.run(
        [GCLOUD, "compute", "instances", "stop", name,
         f"--zone={ZONE}", f"--project={PROJECT}", "--async"],
        capture_output=True, text=True)
    return jsonify({"ok": r.returncode == 0, "msg": r.stderr.strip()})


@app.route("/api/start/<name>", methods=["POST"])
def start_vm(name):
    if not name.startswith(CLUSTER + "-w-"):
        return jsonify({"error": "Can only start worker nodes"}), 403
    r = subprocess.run(
        [GCLOUD, "compute", "instances", "start", name,
         f"--zone={ZONE}", f"--project={PROJECT}", "--async"],
        capture_output=True, text=True)
    return jsonify({"ok": r.returncode == 0, "msg": r.stderr.strip()})


# ── Analytics results ─────────────────────────────────────────────────

@app.route("/api/refresh-results", methods=["POST"])
def refresh_results():
    """Download latest parquet outputs from GCS to local /tmp."""
    os.makedirs(RESULTS_LOCAL, exist_ok=True)
    dirs = ["funnel_metrics_daily", "attributed_orders",
            "daily_anomalies", "category_anomalies"]
    errs = []
    for d in dirs:
        r = subprocess.run(
            [GSUTIL, "-m", "rsync", "-r", "-d",
             f"{BUCKET}/results/demo_run/{d}/",
             f"{RESULTS_LOCAL}/{d}/"],
            capture_output=True, text=True)
        if r.returncode != 0:
            errs.append(f"{d}: {r.stderr.strip()[:120]}")
    return jsonify({"ok": len(errs) == 0, "errors": errs})


@app.route("/api/results/analytics")
def results_analytics():
    """Read local parquet results and return aggregated analytics."""
    try:
        import pandas as pd

        base = RESULTS_LOCAL

        # ── Funnel ──────────────────────────────────────────────────
        funnel = pd.read_parquet(f"{base}/funnel_metrics_daily")

        # Overall daily totals
        daily = (funnel
                 .groupby("date", as_index=False)
                 .agg(views=("total_sessions_with_view", "sum"),
                      carts=("total_sessions_with_cart", "sum"),
                      purchases=("total_sessions_with_purchase", "sum"))
                 .sort_values("date"))
        daily["date"] = daily["date"].astype(str)
        daily["v2c"]  = (daily["carts"]     / daily["views"]     * 100).round(2)
        daily["c2p"]  = (daily["purchases"] / daily["carts"]     * 100).round(2)

        # By device
        by_device = (funnel
                     .groupby("device", as_index=False)
                     .agg(views=("total_sessions_with_view", "sum"),
                          carts=("total_sessions_with_cart", "sum"),
                          purchases=("total_sessions_with_purchase", "sum")))
        by_device["v2c"] = (by_device["carts"]     / by_device["views"]     * 100).round(2)
        by_device["c2p"] = (by_device["purchases"] / by_device["carts"]     * 100).round(2)

        # By referrer
        by_ref = (funnel
                  .groupby("referrer", as_index=False)
                  .agg(views=("total_sessions_with_view", "sum"),
                       carts=("total_sessions_with_cart", "sum"),
                       purchases=("total_sessions_with_purchase", "sum")))

        # Top categories by purchases
        top_cat = (funnel
                   .groupby("category_code", as_index=False)
                   .agg(purchases=("total_sessions_with_purchase", "sum"))
                   .sort_values("purchases", ascending=False)
                   .head(10))

        # ── Attribution ──────────────────────────────────────────────
        attr = pd.read_parquet(f"{base}/attributed_orders")

        by_referrer = (attr
                       .groupby("attributed_referrer", as_index=False)
                       .agg(orders=("order_id", "count"),
                            revenue=("order_amount", "sum"))
                       .sort_values("revenue", ascending=False))
        by_referrer["revenue"] = by_referrer["revenue"].round(2)

        by_dev = (attr
                  .groupby("attribution_device", as_index=False)
                  .agg(orders=("order_id", "count"),
                       revenue=("order_amount", "sum"))
                  .sort_values("revenue", ascending=False))
        by_dev["revenue"] = by_dev["revenue"].round(2)

        # Revenue over time (daily)
        attr["day"] = pd.to_datetime(attr["order_time"]).dt.date.astype(str)
        rev_daily = (attr
                     .groupby("day", as_index=False)
                     .agg(orders=("order_id", "count"),
                          revenue=("order_amount", "sum"))
                     .sort_values("day"))
        rev_daily["revenue"] = rev_daily["revenue"].round(2)

        # ── Anomalies ────────────────────────────────────────────────
        anom = (pd.read_parquet(f"{base}/daily_anomalies")
                .sort_values("date"))
        anom["date"]         = anom["date"].astype(str)
        anom["rolling_avg"]  = anom["rolling_avg"].round(1)
        anom["z_score"]      = anom["z_score"].round(2)

        # Category anomaly summary
        cat_anom = pd.read_parquet(f"{base}/category_anomalies")
        cat_summary = (cat_anom[cat_anom["is_anomaly"]]
                       .groupby("category_code", as_index=False)
                       .size()
                       .rename(columns={"size": "anomaly_count"})
                       .sort_values("anomaly_count", ascending=False)
                       .head(10))

        return jsonify({
            "funnel": {
                "daily":     _df_to_records(daily),
                "by_device": _df_to_records(by_device),
                "by_ref":    _df_to_records(by_ref),
                "top_cat":   _df_to_records(top_cat),
            },
            "attribution": {
                "by_referrer": _df_to_records(by_referrer),
                "by_device":   _df_to_records(by_dev),
                "rev_daily":   _df_to_records(rev_daily),
            },
            "anomalies": {
                "daily":       _df_to_records(anom),
                "cat_summary": _df_to_records(cat_summary),
            },
        })

    except FileNotFoundError as e:
        return jsonify({"error": f"Results not yet downloaded: {e}"}), 404
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


if __name__ == "__main__":
    print(f"\n  DSS Dashboard → http://localhost:5000\n")
    app.run(host="0.0.0.0", port=5000, threaded=True, debug=False)
