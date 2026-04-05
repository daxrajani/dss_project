#!/usr/bin/env bash
# cloud_benchmark.sh — Create a Dataproc cluster, run cloud_pipeline.py, tear down.
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
#   Scripts and data already uploaded to gs://dss-project-dax/.

set -euo pipefail

WORKERS="${1:?Usage: $0 <WORKERS> <DATA_VARIANT>}"
DATA_VARIANT="${2:?Usage: $0 <WORKERS> <DATA_VARIANT>}"

# ── Config ────────────────────────────────────────────────────────────────────
BUCKET="gs://dss-project-dax"
INPUT_CSV="${BUCKET}/data/full/2019-Oct.csv"
SCRIPT_URI="${BUCKET}/scripts/cloud_pipeline.py"
REGION="us-central1"
# GCP free-tier global quota: CPUS_ALL_REGIONS = 12 (covers all machine families).
# n2-standard-2 (2 vCPU, 8 GB): 2w=6 CPUs, 4w=10 CPUs, 5w=12 CPUs — all within quota.
MASTER_TYPE="n2-standard-2"
WORKER_TYPE="n2-standard-2"
IMAGE_VERSION="2.1-debian11"
CLUSTER_NAME="dss-bench-${WORKERS}w-${DATA_VARIANT}-$$"
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

# ── Create cluster ────────────────────────────────────────────────────────────
echo "[$(date '+%H:%M:%S')] Creating Dataproc cluster ..."
gcloud dataproc clusters create "${CLUSTER_NAME}" \
  --region="${REGION}" \
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

# ── Submit PySpark job ────────────────────────────────────────────────────────
echo "[$(date '+%H:%M:%S')] Submitting PySpark job ..."
gcloud dataproc jobs submit pyspark "${SCRIPT_URI}" \
  --cluster="${CLUSTER_NAME}" \
  --region="${REGION}" \
  -- \
  --input="${INPUT_CSV}" \
  --output="${OUTPUT_PATH}" \
  --sample-fraction="${FRACTION}" \
  2>&1 | tee -a "${LOG_FILE}"

JOB_EXIT=${PIPESTATUS[0]}
echo "[$(date '+%H:%M:%S')] Job finished with exit code ${JOB_EXIT}."

# ── Delete cluster (always, even on job failure) ──────────────────────────────
echo "[$(date '+%H:%M:%S')] Deleting cluster ${CLUSTER_NAME} ..."
gcloud dataproc clusters delete "${CLUSTER_NAME}" \
  --region="${REGION}" \
  --quiet \
  2>&1 | tee -a "${LOG_FILE}"

echo "[$(date '+%H:%M:%S')] Cluster deleted."
echo "================================================================"
echo " Done. Log saved to ${LOG_FILE}"
echo "================================================================"

exit ${JOB_EXIT}
