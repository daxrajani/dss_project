#!/bin/bash
# =============================================================================
#  DSS PROJECT DEMO SCRIPT
#  COEN 6731 — Dax Rajani (40294118) & Harsh Ahuja (40301748)
#
#  Usage:
#    bash demo.sh cloud       — Submit live pipeline job to GCP Dataproc
#    bash demo.sh stream      — Start real-time streaming demo (local)
#    bash demo.sh results     — Show benchmark results summary
#    bash demo.sh all         — Cloud demo + results (no streaming)
# =============================================================================

# ── Resolve gcloud/gsutil from PATH or common locations ──────────────
_find_tool() {
    local name="$1"
    command -v "$name" 2>/dev/null && return
    for p in "$HOME/google-cloud-sdk/bin/$name" \
             "/usr/lib/google-cloud-sdk/bin/$name" \
             "/snap/bin/$name" \
             "/usr/local/bin/$name"; do
        [ -f "$p" ] && echo "$p" && return
    done
    echo "$name"   # bare name — will fail with a clear error if missing
}

PYTHON=$(command -v python3 2>/dev/null || echo python3)
GCLOUD=$(_find_tool gcloud)
GSUTIL=$(_find_tool gsutil)

# ── GCP config — override via environment variables ──────────────────
# Set these before running:
#   export GCP_PROJECT=your-project-id
#   export GCS_BUCKET=gs://your-bucket
#   export DATAPROC_CLUSTER=your-cluster-name
CLUSTER="${DATAPROC_CLUSTER:-dss-demo-5w-1gb}"
REGION="${GCP_REGION:-us-central1}"
PROJECT="${GCP_PROJECT:-}"
BUCKET="${GCS_BUCKET:-}"
INPUT="${GCS_INPUT:-${BUCKET}/data/2019-Oct.csv}"
SCRIPTS="${GCS_SCRIPTS:-${BUCKET}/scripts}"
RESULTS_DIR="results"

# ── Colours ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; RESET='\033[0m'

header() { echo -e "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"; echo -e "${BOLD}${CYAN}  $1${RESET}"; echo -e "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}\n"; }

# =============================================================================
#  CLOUD DEMO — run full pipeline on GCP Dataproc
# =============================================================================
demo_cloud() {
    header "DSS CLOUD PIPELINE DEMO — GCP Dataproc"

    echo -e "${YELLOW}Cluster  :${RESET} $CLUSTER"
    echo -e "${YELLOW}Workers  :${RESET} 5 x n2-standard-2"
    echo -e "${YELLOW}Data     :${RESET} $INPUT"
    echo -e "${YELLOW}Sample   :${RESET} 20%  (~8.5M rows)"
    echo -e "${YELLOW}Output   :${RESET} $BUCKET/results/demo_run"
    echo ""

    # ── Ensure cluster is running (create / start / reuse) ──────────────
    _pf() { [[ -n "$PROJECT" ]] && echo "--project=$PROJECT" || echo ""; }

    STATE=$($GCLOUD dataproc clusters describe $CLUSTER \
        --region=$REGION $(_pf) \
        --format="value(status.state)" 2>/dev/null || echo "NOT_FOUND")

    echo -e "${GREEN}Cluster state: ${BOLD}${STATE}${RESET}"

    if [[ "$STATE" == "NOT_FOUND" ]]; then
        echo -e "${GREEN}Cluster not found — creating...${RESET}"
        $GCLOUD dataproc clusters create $CLUSTER \
            --region=$REGION $(_pf) \
            --master-machine-type=n2-standard-2 \
            --num-workers=5 \
            --worker-machine-type=n2-standard-2 \
            --image-version=2.1-debian11 \
            --optional-components=JUPYTER \
            --enable-component-gateway
        echo -e "${GREEN}Cluster created.${RESET}"
    elif [[ "$STATE" == "STOPPED" ]]; then
        echo -e "${GREEN}Cluster is stopped — starting...${RESET}"
        $GCLOUD dataproc clusters start $CLUSTER \
            --region=$REGION $(_pf)
        echo -e "${GREEN}Cluster started.${RESET}"
    else
        echo -e "${GREEN}Cluster is ${STATE} — proceeding.${RESET}"
    fi
    echo ""

    # ── Submit pipeline job ──────────────────────────────────────────────
    echo -e "${GREEN}Submitting pipeline job to Dataproc...${RESET}\n"
    $GCLOUD dataproc jobs submit pyspark $SCRIPTS/cloud_pipeline.py \
        --cluster=$CLUSTER \
        --region=$REGION $(_pf) \
        -- --input  $INPUT \
           --output $BUCKET/results/demo_run \
           --sample-fraction 0.2

    echo -e "\n${GREEN}Pipeline complete! Results at: $BUCKET/results/demo_run${RESET}"
    echo ""
    echo -e "${YELLOW}Listing output files in GCS:${RESET}"
    $GSUTIL ls $BUCKET/results/demo_run/ 2>/dev/null

    # ── Stop cluster (keep for next run) ─────────────────────────────────
    echo ""
    echo -e "${GREEN}Stopping cluster (keeping it for next run)...${RESET}"
    $GCLOUD dataproc clusters stop $CLUSTER \
        --region=$REGION $(_pf)
    echo -e "${GREEN}Cluster stopped. Run again to reuse without recreating.${RESET}"
}

# =============================================================================
#  STREAMING DEMO — real-time anomaly detection
# =============================================================================
demo_stream() {
    header "DSS REAL-TIME STREAMING DEMO"

    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    STREAM_IN="$SCRIPT_DIR/data/raw/stream_input"
    STREAM_OUT="$SCRIPT_DIR/data/processed/stream_output"
    CHECKPOINT="$SCRIPT_DIR/data/processed/stream_checkpoints"

    echo -e "${YELLOW}Cleaning up previous stream state...${RESET}"
    rm -rf "$STREAM_IN" "$STREAM_OUT" "$CHECKPOINT"
    mkdir -p "$STREAM_IN"

    echo -e "${GREEN}Architecture:${RESET}"
    echo "  stream_producer.py  →  data/raw/stream_input/  →  streaming_job.py"
    echo "  (drops CSV batches)         (watch dir)            (Spark Structured Streaming)"
    echo ""
    echo -e "${YELLOW}What to watch:${RESET}"
    echo "  - Purchase counts per category per 5-minute window"
    echo "  - SPIKE alert when purchase count > threshold"
    echo "  - Spike injected every 7th batch automatically"
    echo ""
    echo -e "${GREEN}Starting streaming job (Ctrl+C to stop)...${RESET}\n"

    # Start producer in background
    $PYTHON "$SCRIPT_DIR/src/stream_producer.py" --spike-every 5 --interval 2 &
    PRODUCER_PID=$!

    # Start streaming job (foreground — shows output)
    $PYTHON "$SCRIPT_DIR/src/streaming_job.py" --test-timeout 60

    # Cleanup
    kill $PRODUCER_PID 2>/dev/null
    echo -e "\n${GREEN}Streaming demo complete.${RESET}"
}

# =============================================================================
#  RESULTS — show benchmark summary table
# =============================================================================
demo_results() {
    header "DSS BENCHMARK RESULTS SUMMARY"

    echo -e "${YELLOW}Scaling Study — 2/4/5 workers × 1GB/5GB/10GB (9 runs total):${RESET}\n"
    if [ -f "$RESULTS_DIR/benchmark_summary.csv" ]; then
        column -t -s',' "$RESULTS_DIR/benchmark_summary.csv" | head -20
    fi

    echo ""
    echo -e "${YELLOW}Optimization Experiments:${RESET}\n"
    echo -e "  ${BOLD}Broadcast Join${RESET}"
    grep -E "(Shuffle|Broadcast|Speedup|Time)" "$RESULTS_DIR/broadcast_join_results.log" 2>/dev/null | head -6

    echo ""
    echo -e "  ${BOLD}Partition Analysis${RESET}"
    grep -E "(Speedup|Single|7-day)" "$RESULTS_DIR/partition_analysis_results.log" 2>/dev/null | head -6

    echo ""
    echo -e "  ${BOLD}Skew Analysis${RESET}"
    grep -E "(Without|With salting|Speedup|Top-1)" "$RESULTS_DIR/skew_analysis_results.log" 2>/dev/null | head -5

    echo ""
    echo -e "${YELLOW}Charts available in results/charts/:${RESET}"
    ls "$RESULTS_DIR/charts/"*.png 2>/dev/null | xargs -I{} basename {}

    echo ""
    echo -e "${YELLOW}GitHub repo:${RESET} https://github.com/daxrajani/dss_project"
}

# =============================================================================
#  PERF — generate performance charts from benchmark results
# =============================================================================
demo_perf() {
    header "DSS PERFORMANCE CHARTS"

    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

    echo -e "${YELLOW}Generating all 6 performance charts from results/benchmark_summary.csv ...${RESET}"
    echo ""
    $PYTHON "$SCRIPT_DIR/src/generate_perf_charts.py" --parse-logs

    echo ""
    echo -e "${YELLOW}Charts saved to:${RESET} results/charts/"
    echo ""
    ls "$RESULTS_DIR/charts/"*.png 2>/dev/null | xargs -I{} basename {}
}

# =============================================================================
#  ANALYZE — download GCS pipeline output and show analytics results
# =============================================================================
demo_analyze() {
    header "DSS PIPELINE ANALYTICS RESULTS"

    GCS_OUTPUT="${2:-${BUCKET}/results/demo_run}"

    if [[ -z "$BUCKET" && -z "$2" ]]; then
        echo -e "${YELLOW}Usage: bash demo.sh analyze gs://YOUR_BUCKET/results/demo_run${RESET}"
        echo ""
        echo "  Or set GCS_BUCKET and re-run:"
        echo "    export GCS_BUCKET=gs://your-bucket"
        echo "    bash demo.sh analyze"
        exit 1
    fi

    echo -e "${YELLOW}GCS output path : ${RESET}$GCS_OUTPUT"
    echo -e "${YELLOW}Charts will be saved to : ${RESET}results/demo_analysis/"
    echo ""

    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    $PYTHON "$SCRIPT_DIR/src/analyze_gcs_results.py" --gcs-output "$GCS_OUTPUT"
}

# =============================================================================
#  ENTRY POINT
# =============================================================================
case "${1:-all}" in
    cloud)   demo_cloud ;;
    stream)  demo_stream ;;
    results) demo_results ;;
    analyze) demo_analyze "$@" ;;
    perf)    demo_perf ;;
    all)
        demo_cloud
        echo ""
        demo_results
        ;;
    *)
        echo "Usage: bash demo.sh [cloud|stream|results|analyze|perf|all]"
        echo ""
        echo "  cloud   — Submit live pipeline to GCP Dataproc (shows all 5 stages running)"
        echo "  stream  — Real-time streaming anomaly detection demo (local, 60s)"
        echo "  results — Print benchmark table + optimization results"
        echo "  analyze — Download GCS output + show funnel/attribution/anomaly results + charts"
        echo "  perf    — Generate all 6 performance charts from scaling study results"
        echo "  all     — Cloud + results (default)"
        echo ""
        echo "  analyze usage:"
        echo "    bash demo.sh analyze gs://YOUR_BUCKET/results/demo_run"
        ;;
esac
