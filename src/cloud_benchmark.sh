#!/usr/bin/env bash
# cloud_benchmark.sh — Ensure Dataproc cluster is running, submit cloud_pipeline.py, then stop it.
#
# Cluster lifecycle:
#   - Searches all fallback regions for an existing cluster first
#   - If found (STOPPED)  → starts it in the region it lives in
#   - If found (RUNNING)  → reuses it
#   - If not found        → creates in the first region with available resources
#   After the job finishes → stops the cluster (not delete — reuse next time)
#
# Usage:
#   bash src/cloud_benchmark.sh <WORKERS> <DATA_VARIANT>

set -euo pipefail

WORKERS="${1:?Usage: $0 <WORKERS> <DATA_VARIANT>}"
DATA_VARIANT="${2:?Usage: $0 <WORKERS> <DATA_VARIANT>}"

# ── Config (pre-configured — no setup needed) ────────────────────────────────
PROJECT="${GCP_PROJECT:-project-a0b2f7d8-d4bf-4557-923}"
BUCKET="${GCS_BUCKET:-gs://dss-project-dax}"
INPUT_CSV="${GCS_INPUT:-${BUCKET}/data/full/2019-Oct.csv}"
SCRIPT_URI="${GCS_SCRIPTS:-${BUCKET}/scripts}/cloud_pipeline.py"
MASTER_TYPE="n2-standard-2"
WORKER_TYPE="n2-standard-2"
IMAGE_VERSION="2.1-debian11"
CLUSTER_NAME="dss-bench-${WORKERS}w-${DATA_VARIANT}"
RESULTS_DIR="results"

# ── Region fallback list — tried in order until one has capacity ──────────────
FALLBACK_REGIONS=(
    "us-central1"
    "us-east1"
    "us-east4"
    "us-west1"
    "us-west2"
    "europe-west1"
    "europe-west4"
    "asia-east1"
)

mkdir -p "${RESULTS_DIR}"
LOG_FILE="${RESULTS_DIR}/cloud_${WORKERS}w_${DATA_VARIANT}.log"

case "${DATA_VARIANT}" in
  1gb)  FRACTION="0.2" ;;
  5gb)  FRACTION="1.0" ;;
  10gb) FRACTION="2.0" ;;
  *)
    echo "ERROR: DATA_VARIANT must be 1gb, 5gb, or 10gb (got '${DATA_VARIANT}')"
    exit 1 ;;
esac

OUTPUT_PATH="${BUCKET}/results/${WORKERS}w_${DATA_VARIANT}"

echo "================================================================"
echo " DSS Cloud Benchmark"
echo "   Workers      : ${WORKERS}"
echo "   Data variant : ${DATA_VARIANT}  (fraction=${FRACTION})"
echo "   Cluster      : ${CLUSTER_NAME}"
echo "   Output       : ${OUTPUT_PATH}"
echo "   Log          : ${LOG_FILE}"
echo "================================================================"

_pf() { [[ -n "$PROJECT" ]] && echo "--project=$PROJECT" || echo ""; }

# ── Find cluster in any region, return "region state" or "NOT_FOUND" ─────────
_find_cluster() {
    for r in "${FALLBACK_REGIONS[@]}"; do
        state=$(gcloud dataproc clusters describe "${CLUSTER_NAME}" \
            --region="$r" $(_pf) \
            --format="value(status.state)" 2>/dev/null || true)
        if [[ -n "$state" ]]; then
            echo "$r $state"
            return
        fi
    done
    echo "NOT_FOUND"
}

# ── Create cluster, trying each region until one succeeds ─────────────────────
_create_cluster() {
    for r in "${FALLBACK_REGIONS[@]}"; do
        echo "[$(date '+%H:%M:%S')] Trying region ${r} ..."
        output=$(gcloud dataproc clusters create "${CLUSTER_NAME}" \
            --region="$r" $(_pf) \
            --master-machine-type="${MASTER_TYPE}" \
            --master-boot-disk-size=50GB \
            --num-workers="${WORKERS}" \
            --worker-machine-type="${WORKER_TYPE}" \
            --worker-boot-disk-size=50GB \
            --image-version="${IMAGE_VERSION}" \
            --optional-components=JUPYTER \
            --enable-component-gateway \
            2>&1)
        exit_code=$?

        if [[ $exit_code -eq 0 ]]; then
            echo "$output" | tee -a "${LOG_FILE}"
            REGION="$r"
            echo "[$(date '+%H:%M:%S')] Cluster created in region ${REGION}."
            return 0
        elif echo "$output" | grep -qiE \
            "does not have enough resources|UNAVAILABLE|RESOURCE_EXHAUSTED|quota exceeded|insufficient"; then
            echo "  Region ${r} has insufficient resources — trying next region ..."
            echo "$output" >> "${LOG_FILE}"
        else
            # Unrelated error — fail immediately
            echo "$output" | tee -a "${LOG_FILE}"
            return $exit_code
        fi
    done
    echo "ERROR: Could not create cluster in any region. All regions exhausted." | tee -a "${LOG_FILE}"
    return 1
}

# ── Ensure cluster is running ─────────────────────────────────────────────────
FOUND=$(_find_cluster)

if [[ "$FOUND" == "NOT_FOUND" ]]; then
    echo "[$(date '+%H:%M:%S')] Cluster not found — creating ..."
    _create_cluster

elif [[ "$FOUND" =~ ^([^ ]+)\ STOPPED$ ]]; then
    REGION="${BASH_REMATCH[1]}"
    echo "[$(date '+%H:%M:%S')] Cluster found STOPPED in ${REGION} — starting ..."
    gcloud dataproc clusters start "${CLUSTER_NAME}" \
        --region="${REGION}" $(_pf) \
        2>&1 | tee -a "${LOG_FILE}"
    echo "[$(date '+%H:%M:%S')] Cluster started."

elif [[ "$FOUND" =~ ^([^ ]+)\ RUNNING$ ]]; then
    REGION="${BASH_REMATCH[1]}"
    echo "[$(date '+%H:%M:%S')] Cluster already RUNNING in ${REGION} — reusing."

else
    # Some other state (CREATING, UPDATING, ERROR…) — extract region and wait
    REGION=$(echo "$FOUND" | awk '{print $1}')
    STATE=$(echo "$FOUND"  | awk '{print $2}')
    echo "[$(date '+%H:%M:%S')] Cluster is ${STATE} in ${REGION} — waiting ..."
    sleep 30
fi

echo "[$(date '+%H:%M:%S')] Using region: ${REGION}"

# ── Submit PySpark job ────────────────────────────────────────────────────────
echo "[$(date '+%H:%M:%S')] Submitting PySpark job ..."
gcloud dataproc jobs submit pyspark "${SCRIPT_URI}" \
    --cluster="${CLUSTER_NAME}" \
    --region="${REGION}" $(_pf) \
    -- \
    --input="${INPUT_CSV}" \
    --output="${OUTPUT_PATH}" \
    --sample-fraction="${FRACTION}" \
    2>&1 | tee -a "${LOG_FILE}"

JOB_EXIT=${PIPESTATUS[0]}
echo "[$(date '+%H:%M:%S')] Job finished with exit code ${JOB_EXIT}."

# ── Stop cluster (keep for next run) ──────────────────────────────────────────
echo "[$(date '+%H:%M:%S')] Stopping cluster in ${REGION} ..."
gcloud dataproc clusters stop "${CLUSTER_NAME}" \
    --region="${REGION}" $(_pf) \
    2>&1 | tee -a "${LOG_FILE}"

echo "[$(date '+%H:%M:%S')] Cluster stopped."
echo "================================================================"
echo " Done. Region used: ${REGION}"
echo " Log saved to ${LOG_FILE}"
echo "================================================================"

exit ${JOB_EXIT}
