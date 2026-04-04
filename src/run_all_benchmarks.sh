#!/bin/bash
# run_all_benchmarks.sh — Run all 9 benchmark combinations sequentially.
#
# Combinations: 3 cluster sizes (2/4/6 workers) x 3 data sizes (1gb/5gb/10gb)
#
# Usage:
#   bash src/run_all_benchmarks.sh
#
# Results are saved to results/cloud_<Nw>_<size>.log for each combination.
# A summary table is printed at the end.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="${REPO_ROOT}/results"
SUMMARY_FILE="${LOG_DIR}/benchmark_summary.txt"

mkdir -p "$LOG_DIR"

CLUSTER_SIZES=(2 4 6)
DATA_SIZES=(1gb 5gb 10gb)

START_ALL=$(date +%s)

echo "========================================================"
echo "  DSS SCALING BENCHMARK — ALL 9 COMBINATIONS"
echo "  $(date)"
echo "========================================================"

# Track pass/fail for each run
declare -A STATUS

for CLUSTER_SIZE in "${CLUSTER_SIZES[@]}"; do
    for DATA_SIZE in "${DATA_SIZES[@]}"; do
        COMBO="${CLUSTER_SIZE}w_${DATA_SIZE}"
        echo ""
        echo "--------------------------------------------------------"
        echo "  Running: ${CLUSTER_SIZE} workers x ${DATA_SIZE}"
        echo "  Started: $(date '+%H:%M:%S')"
        echo "--------------------------------------------------------"

        RUN_START=$(date +%s)

        if bash "${SCRIPT_DIR}/cloud_submit.sh" "$CLUSTER_SIZE" "$DATA_SIZE"; then
            RUN_END=$(date +%s)
            ELAPSED=$(( RUN_END - RUN_START ))
            STATUS[$COMBO]="OK  (${ELAPSED}s)"
            echo "  DONE in ${ELAPSED}s"
        else
            RUN_END=$(date +%s)
            ELAPSED=$(( RUN_END - RUN_START ))
            STATUS[$COMBO]="FAIL"
            echo "  FAILED after ${ELAPSED}s — check results/cloud_${COMBO}.log"
        fi
    done
done

END_ALL=$(date +%s)
TOTAL=$(( END_ALL - START_ALL ))

# ---------------------------------------------------------------------------
# Print summary table
# ---------------------------------------------------------------------------
{
echo ""
echo "========================================================"
echo "  BENCHMARK SUMMARY"
echo "  Total wall time: ${TOTAL}s"
echo "========================================================"
printf "  %-12s %-8s %-8s %-8s\n" "Workers \\" "1gb" "5gb" "10gb"
echo "  --------------------------------------------"
for CLUSTER_SIZE in "${CLUSTER_SIZES[@]}"; do
    ROW="  ${CLUSTER_SIZE} workers   "
    for DATA_SIZE in "${DATA_SIZES[@]}"; do
        COMBO="${CLUSTER_SIZE}w_${DATA_SIZE}"
        ROW+="$(printf '%-12s' "${STATUS[$COMBO]}")"
    done
    echo "$ROW"
done
echo "========================================================"
echo ""
echo "  Logs: results/cloud_<Nw>_<size>.log"
echo "  Results on GCS: gs://dss-project-dax/results/<Nw>-<size>/"
echo "========================================================"
} | tee "$SUMMARY_FILE"

echo ""
echo "Summary saved to: ${SUMMARY_FILE}"
