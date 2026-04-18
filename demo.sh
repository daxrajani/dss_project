#!/bin/bash
# =============================================================================
#  DSS PROJECT DEMO SCRIPT
#  COEN 6731 — Dax Rajani (40294118) & Harsh Ahuja (40301748)
#
#  Usage:
#    bash demo.sh results     — Show pre-computed benchmark results (no GCP needed)
#    bash demo.sh stream      — Real-time streaming anomaly detection (no GCP needed)
#    bash demo.sh perf        — Regenerate all performance charts (no GCP needed)
#    bash demo.sh cloud       — Submit live pipeline to GCP Dataproc (prompts if needed)
#    bash demo.sh analyze     — Download GCS results and show analytics + charts
#    bash demo.sh all         — Cloud + results (default)
#
#  GCP project and bucket are pre-configured — no setup needed.
#  Only prerequisite for cloud/analyze modes: gcloud auth login
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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="results"

# ── Colours ───────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; RESET='\033[0m'

# ── GCP defaults (pre-configured — no setup needed) ──────────────────────────
#   The project, bucket, and data are already deployed on GCP.
#   Just run:  bash demo.sh cloud   — and it works immediately.
#   Override any value by setting the matching environment variable beforehand.
_gcp_setup() {
    PROJECT="${GCP_PROJECT:-project-a0b2f7d8-d4bf-4557-923}"
    BUCKET="${GCS_BUCKET:-gs://dss-project-dax}"
    INPUT="${GCS_INPUT:-${BUCKET}/data/full/2019-Oct.csv}"
    SCRIPTS="${GCS_SCRIPTS:-${BUCKET}/scripts}"
    CLUSTER="${DATAPROC_CLUSTER:-dss-demo-5w}"
    export PROJECT BUCKET INPUT SCRIPTS CLUSTER
}

# ── Region fallback list — tried in order until one has capacity ──────────────
_FALLBACK_REGIONS=(
    "us-central1" "us-east1" "us-east4" "us-west1" "us-west2"
    "europe-west1" "europe-west4" "asia-east1"
)

_pf() { [[ -n "$PROJECT" ]] && echo "--project=$PROJECT" || echo ""; }

# Find cluster across all regions → prints "REGION STATE" or "NOT_FOUND"
_find_cluster() {
    local name="$1"
    for r in "${_FALLBACK_REGIONS[@]}"; do
        local st
        st=$($GCLOUD dataproc clusters describe "$name" \
            --region="$r" $(_pf) \
            --format="value(status.state)" 2>/dev/null || true)
        [[ -n "$st" ]] && echo "$r $st" && return
    done
    echo "NOT_FOUND"
}

# Create cluster, trying each region until one has resources
_create_cluster() {
    local name="$1"; shift
    local extra_args=("$@")
    for r in "${_FALLBACK_REGIONS[@]}"; do
        echo -e "${GREEN}  Trying region ${r} ...${RESET}"
        local out
        out=$($GCLOUD dataproc clusters create "$name" \
            --region="$r" $(_pf) "${extra_args[@]}" 2>&1)
        local code=$?
        if [[ $code -eq 0 ]]; then
            REGION="$r"
            echo -e "${GREEN}  Cluster created in ${REGION}.${RESET}"
            return 0
        elif echo "$out" | grep -qiE \
            "does not have enough resources|UNAVAILABLE|RESOURCE_EXHAUSTED|quota exceeded|insufficient"; then
            echo -e "${YELLOW}  Region ${r} out of resources — trying next ...${RESET}"
        else
            echo "$out"
            return $code
        fi
    done
    echo "ERROR: No region had available resources." >&2
    return 1
}

header() { echo -e "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"; echo -e "${BOLD}${CYAN}  $1${RESET}"; echo -e "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}\n"; }

# =============================================================================
#  CLOUD DEMO — run full pipeline on GCP Dataproc
# =============================================================================
demo_cloud() {
    _gcp_setup

    header "DSS CLOUD PIPELINE DEMO — GCP Dataproc"

    echo -e "${YELLOW}Project  :${RESET} $PROJECT"
    echo -e "${YELLOW}Bucket   :${RESET} $BUCKET"
    echo -e "${YELLOW}Cluster  :${RESET} $CLUSTER"
    echo -e "${YELLOW}Workers  :${RESET} 5 x n2-standard-2"
    echo -e "${YELLOW}Data     :${RESET} $INPUT"
    echo -e "${YELLOW}Sample   :${RESET} 20%  (~8.5M rows)"
    echo -e "${YELLOW}Output   :${RESET} $BUCKET/results/demo_run"
    echo ""

    # ── Ensure cluster is running ────────────────────────────────────────
    FOUND=$(_find_cluster "$CLUSTER")

    if [[ "$FOUND" == "NOT_FOUND" ]]; then
        echo -e "${GREEN}Cluster not found — creating (auto-selecting region)...${RESET}"
        _create_cluster "$CLUSTER" \
            --master-machine-type=n2-standard-2 \
            --num-workers=5 \
            --worker-machine-type=n2-standard-2 \
            --image-version=2.1-debian11 \
            --optional-components=JUPYTER \
            --enable-component-gateway
    elif [[ "$FOUND" =~ ^([^ ]+)\ STOPPED$ ]]; then
        REGION="${BASH_REMATCH[1]}"
        echo -e "${GREEN}Cluster found STOPPED in ${REGION} — starting...${RESET}"
        $GCLOUD dataproc clusters start $CLUSTER --region=$REGION $(_pf)
        echo -e "${GREEN}Cluster started.${RESET}"
    elif [[ "$FOUND" =~ ^([^ ]+)\ RUNNING$ ]]; then
        REGION="${BASH_REMATCH[1]}"
        echo -e "${GREEN}Cluster already RUNNING in ${REGION} — reusing.${RESET}"
    else
        REGION=$(echo "$FOUND" | awk '{print $1}')
        echo -e "${GREEN}Cluster is in state $(echo "$FOUND" | awk '{print $2}') in ${REGION}.${RESET}"
    fi

    echo -e "${CYAN}Region   : ${REGION}${RESET}\n"

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
    $GCLOUD dataproc clusters stop $CLUSTER --region=$REGION $(_pf)
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
    # If a GCS path was passed as argument use it directly; otherwise auto-detect
    if [[ -n "${2:-}" ]]; then
        GCS_OUTPUT="$2"
    else
        _gcp_setup
        GCS_OUTPUT="${BUCKET}/results/demo_run"
    fi

    header "DSS PIPELINE ANALYTICS RESULTS"

    echo -e "${YELLOW}GCS output path        :${RESET} $GCS_OUTPUT"
    echo -e "${YELLOW}Charts will be saved to:${RESET} results/demo_analysis/"
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
        echo "Usage: bash demo.sh [results|stream|perf|cloud|analyze|all]"
        echo ""
        echo "  No GCP required:"
        echo "    results  — Print benchmark table + optimization results"
        echo "    stream   — Real-time streaming anomaly detection (local, ~60s)"
        echo "    perf     — Regenerate all 6 performance charts from saved results"
        echo ""
        echo "  Requires GCP:"
        echo "    cloud    — Submit live pipeline to GCP Dataproc (all 5 stages)"
        echo "    analyze  — Download GCS output and show funnel/attribution/anomaly results + charts"
        echo "    all      — cloud + results (default)"
        echo ""
        echo "  GCP config is pre-set — no configuration needed."
        echo "  The project and bucket are already baked in."
        echo "  Only prerequisite: gcloud auth login (to authenticate with GCP)."
        ;;
esac
