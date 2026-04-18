#!/bin/bash
# cloud_submit.sh — Ensure Dataproc cluster is running, run benchmark, then stop it.
#
# Cluster lifecycle:
#   - If cluster does not exist  → create it
#   - If cluster is STOPPED      → start it
#   - If cluster is RUNNING      → use it as-is (no recreation needed)
#   After the job finishes       → stop the cluster (not delete)
#
# Usage:
#   bash src/cloud_submit.sh <CLUSTER_SIZE> <DATA_SIZE>
#   CLUSTER_SIZE : 2 | 4 | 6   (number of worker nodes)
#   DATA_SIZE    : 1gb | 5gb | 10gb
#
# Example:
#   bash src/cloud_submit.sh 4 5gb

set -euo pipefail

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <CLUSTER_SIZE> <DATA_SIZE>"
    echo "  CLUSTER_SIZE : 2 | 4 | 6"
    echo "  DATA_SIZE    : 1gb | 5gb | 10gb"
    exit 1
fi

CLUSTER_SIZE="$1"
DATA_SIZE="$2"

if [[ "$CLUSTER_SIZE" != "2" && "$CLUSTER_SIZE" != "4" && "$CLUSTER_SIZE" != "6" ]]; then
    echo "ERROR: CLUSTER_SIZE must be 2, 4, or 6. Got: $CLUSTER_SIZE"
    exit 1
fi

if [[ "$DATA_SIZE" != "1gb" && "$DATA_SIZE" != "5gb" && "$DATA_SIZE" != "10gb" ]]; then
    echo "ERROR: DATA_SIZE must be 1gb, 5gb, or 10gb. Got: $DATA_SIZE"
    exit 1
fi

# ---------------------------------------------------------------------------
# Config — override via environment variables
#   export GCP_PROJECT=your-project-id
#   export GCS_BUCKET=gs://your-bucket
#   export GCP_REGION=us-central1
# ---------------------------------------------------------------------------
PROJECT="${GCP_PROJECT:-}"
REGION="${GCP_REGION:-us-central1}"
ZONE="${GCP_ZONE:-us-central1-a}"
BUCKET="${GCS_BUCKET:-}"
CLUSTER_NAME="dss-bench-${CLUSTER_SIZE}w"
MACHINE_TYPE="n1-standard-4"

INPUT_PATH="${BUCKET}/data/${DATA_SIZE}/"
OUTPUT_PATH="${BUCKET}/results/${CLUSTER_SIZE}w-${DATA_SIZE}/"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="${REPO_ROOT}/results"
LOG_FILE="${LOG_DIR}/cloud_${CLUSTER_SIZE}w_${DATA_SIZE}.log"

mkdir -p "$LOG_DIR"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

_project_flag() { [[ -n "$PROJECT" ]] && echo "--project=$PROJECT" || echo ""; }

_cluster_state() {
    gcloud dataproc clusters describe "$CLUSTER_NAME" \
        --region="$REGION" $(_project_flag) \
        --format="value(status.state)" 2>/dev/null || echo "NOT_FOUND"
}

# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------
log "========================================================"
log "  DSS CLOUD BENCHMARK"
log "  Cluster : ${CLUSTER_NAME}  (1 master + ${CLUSTER_SIZE} workers, ${MACHINE_TYPE})"
log "  Input   : ${INPUT_PATH}"
log "  Output  : ${OUTPUT_PATH}"
log "  Log     : ${LOG_FILE}"
log "========================================================"

# ---------------------------------------------------------------------------
# 1. Ensure cluster is running
# ---------------------------------------------------------------------------
STATE=$(_cluster_state)
log ">>> [1/4] Cluster state: ${STATE}"

if [[ "$STATE" == "NOT_FOUND" ]]; then
    log "    Cluster not found — creating ..."
    gcloud dataproc clusters create "$CLUSTER_NAME" \
        $(_project_flag) \
        --region="$REGION" \
        --zone="$ZONE" \
        --master-machine-type="$MACHINE_TYPE" \
        --master-boot-disk-size=100GB \
        --num-workers="$CLUSTER_SIZE" \
        --worker-machine-type="$MACHINE_TYPE" \
        --worker-boot-disk-size=100GB \
        --image-version=2.1-debian11 \
        --optional-components=JUPYTER \
        --enable-component-gateway \
        2>&1 | tee -a "$LOG_FILE"
    log "    Cluster created."
elif [[ "$STATE" == "STOPPED" ]]; then
    log "    Cluster is stopped — starting ..."
    gcloud dataproc clusters start "$CLUSTER_NAME" \
        --region="$REGION" $(_project_flag) \
        2>&1 | tee -a "$LOG_FILE"
    log "    Cluster started."
elif [[ "$STATE" == "RUNNING" ]]; then
    log "    Cluster already running — reusing existing cluster."
else
    log "    Cluster is in state: ${STATE} — waiting for it to become RUNNING ..."
    gcloud dataproc clusters wait "$CLUSTER_NAME" \
        --region="$REGION" $(_project_flag) 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# 2. Submit PySpark benchmark job
# ---------------------------------------------------------------------------
log ">>> [2/4] Submitting PySpark job ..."

gcloud dataproc jobs submit pyspark \
    "${SCRIPT_DIR}/benchmark_job.py" \
    $(_project_flag) \
    --region="$REGION" \
    --cluster="$CLUSTER_NAME" \
    --properties="spark.sql.shuffle.partitions=200" \
    -- \
    --input "$INPUT_PATH" \
    --output "$OUTPUT_PATH" \
    2>&1 | tee -a "$LOG_FILE"

JOB_EXIT=${PIPESTATUS[0]}
log "    Job complete (exit code ${JOB_EXIT})."

# ---------------------------------------------------------------------------
# 3. Stop cluster (keep it for next run — no need to recreate)
# ---------------------------------------------------------------------------
log ">>> [3/4] Stopping cluster: ${CLUSTER_NAME} (not deleting — reuse next time) ..."
gcloud dataproc clusters stop "$CLUSTER_NAME" \
    --region="$REGION" $(_project_flag) \
    2>&1 | tee -a "$LOG_FILE"
log "    Cluster stopped."

# ---------------------------------------------------------------------------
# 4. Done
# ---------------------------------------------------------------------------
log ">>> [4/4] Done."
log "    Log saved to: ${LOG_FILE}"
log "    Results at  : ${OUTPUT_PATH}"
log "========================================================"

exit ${JOB_EXIT}
