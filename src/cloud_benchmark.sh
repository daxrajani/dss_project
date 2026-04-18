#!/usr/bin/env bash
# cloud_benchmark.sh — Ensure Dataproc cluster is running, submit cloud_pipeline.py, then stop it.
#
# Cluster lifecycle:
#   - If cluster does not exist  → create it
#   - If cluster is STOPPED      → start it
#   - If cluster is RUNNING      → use it as-is (no recreation needed)
#   After the job finishes       → stop the cluster (not delete)
#
# Usage:
#   bash src/cloud_benchmark.sh <WORKERS> <DATA_VARIANT>
#
# Arguments:
#   WORKERS       — number of worker nodes (2, 4, or 6)
#   DATA_VARIANT  — one of: 1gb, 5gb, 10gb
#
# Prerequisites:
#   gcloud CLI authenticated with a project that has Dataproc + GCS access.
#   Scripts and data already uploaded to your GCS bucket (set GCS_BUCKET env var).

set -euo pipefail

WORKERS="${1:?Usage: $0 <WORKERS> <DATA_VARIANT>}"
DATA_VARIANT="${2:?Usage: $0 <WORKERS> <DATA_VARIANT>}"

# ── Config (pre-configured — no setup needed) ────────────────────────────────
#   Defaults point to the project and bucket already deployed for this submission.
#   Override any value by setting the matching environment variable beforehand.
PROJECT="${GCP_PROJECT:-project-a0b2f7d8-d4bf-4557-923}"
BUCKET="${GCS_BUCKET:-gs://dss-project-dax}"
INPUT_CSV="${GCS_INPUT:-${BUCKET}/data/full/2019-Oct.csv}"
SCRIPT_URI="${GCS_SCRIPTS:-${BUCKET}/scripts}/cloud_pipeline.py"
REGION="${GCP_REGION:-us-central1}"
MASTER_TYPE="n2-standard-2"
WORKER_TYPE="n2-standard-2"
IMAGE_VERSION="2.1-debian11"
# Stable cluster name (no PID suffix) so the same cluster is reused across runs
CLUSTER_NAME="dss-bench-${WORKERS}w-${DATA_VARIANT}"
RESULTS_DIR="results"

mkdir -p "${RESULTS_DIR}"
LOG_FILE="${RESULTS_DIR}/cloud_${WORKERS}w_${DATA_VARIANT}.log"

# ── Map data variant → sample fraction ───────────────────────────────────────
case "${DATA_VARIANT}" in
  1gb)  FRACTION="0.2"  ;;
  5gb)  FRACTION="1.0"  ;;
  10gb) FRACTION="2.0"  ;;
  *)
    echo "ERROR: DATA_VARIANT must be 1gb, 5gb, or 10gb (got '${DATA_VARIANT}')"
    exit 1
    ;;
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

# ── Helpers ───────────────────────────────────────────────────────────────────
_pf() { [[ -n "$PROJECT" ]] && echo "--project=$PROJECT" || echo ""; }

_cluster_state() {
    gcloud dataproc clusters describe "${CLUSTER_NAME}" \
        --region="${REGION}" $(_pf) \
        --format="value(status.state)" 2>/dev/null || echo "NOT_FOUND"
}

# ── Ensure cluster is running ─────────────────────────────────────────────────
STATE=$(_cluster_state)
echo "[$(date '+%H:%M:%S')] Cluster state: ${STATE}"

if [[ "$STATE" == "NOT_FOUND" ]]; then
    echo "[$(date '+%H:%M:%S')] Cluster not found — creating ..."
    gcloud dataproc clusters create "${CLUSTER_NAME}" \
      --region="${REGION}" $(_pf) \
      --master-machine-type="${MASTER_TYPE}" \
      --master-boot-disk-size=50GB \
      --num-workers="${WORKERS}" \
      --worker-machine-type="${WORKER_TYPE}" \
      --worker-boot-disk-size=50GB \
      --image-version="${IMAGE_VERSION}" \
      --optional-components=JUPYTER \
      --enable-component-gateway \
      2>&1 | tee -a "${LOG_FILE}"
    echo "[$(date '+%H:%M:%S')] Cluster created."
elif [[ "$STATE" == "STOPPED" ]]; then
    echo "[$(date '+%H:%M:%S')] Cluster is stopped — starting ..."
    gcloud dataproc clusters start "${CLUSTER_NAME}" \
      --region="${REGION}" $(_pf) \
      2>&1 | tee -a "${LOG_FILE}"
    echo "[$(date '+%H:%M:%S')] Cluster started."
elif [[ "$STATE" == "RUNNING" ]]; then
    echo "[$(date '+%H:%M:%S')] Cluster already running — reusing existing cluster."
else
    echo "[$(date '+%H:%M:%S')] Cluster is in state ${STATE} — waiting ..."
    gcloud dataproc clusters wait "${CLUSTER_NAME}" \
      --region="${REGION}" $(_pf) 2>/dev/null || true
fi

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

# ── Stop cluster (keep it for next run — no need to recreate) ─────────────────
echo "[$(date '+%H:%M:%S')] Stopping cluster ${CLUSTER_NAME} (not deleting — reuse next time) ..."
gcloud dataproc clusters stop "${CLUSTER_NAME}" \
  --region="${REGION}" $(_pf) \
  2>&1 | tee -a "${LOG_FILE}"

echo "[$(date '+%H:%M:%S')] Cluster stopped."
echo "================================================================"
echo " Done. Log saved to ${LOG_FILE}"
echo "================================================================"

exit ${JOB_EXIT}
