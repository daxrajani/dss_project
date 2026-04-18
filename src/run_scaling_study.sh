#!/usr/bin/env bash
# run_scaling_study.sh — Run all 9 (workers x data_variant) benchmark combinations.
#
# Usage:
#   bash src/run_scaling_study.sh
#
# Runs sequentially to avoid Dataproc quota issues.
# Each combination: creates cluster → submits job → deletes cluster → logs to results/.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "================================================================"
echo " DSS Scaling Study — 9 benchmark combinations"
echo " $(date)"
echo "================================================================"

# Workers: 2/4/5 — 6w requires CPUS_ALL_REGIONS > 12 (GCP free-tier global cap).
# n2-standard-2: 2w=6 CPUs, 4w=10 CPUs, 5w=12 CPUs (exact quota limit).
for workers in 2 4 5; do
  for data in 1gb 5gb 10gb; do
    echo ""
    echo "----------------------------------------------------------------"
    echo " Starting: ${workers} workers  x  ${data}"
    echo " $(date)"
    echo "----------------------------------------------------------------"
    bash "${SCRIPT_DIR}/cloud_benchmark.sh" "${workers}" "${data}"
    echo " Finished: ${workers} workers  x  ${data}  at $(date)"
  done
done

echo ""
echo "================================================================"
echo " ALL 9 COMBINATIONS COMPLETE"
echo " $(date)"
echo " Logs in: results/"
echo "================================================================"

# ── Generate all performance charts from the fresh log files ─────────────────
echo ""
echo "Generating performance charts from benchmark logs ..."
PYTHON=$(command -v python3 2>/dev/null || echo python3)
"$PYTHON" "${SCRIPT_DIR}/generate_perf_charts.py" --parse-logs

echo ""
echo "================================================================"
echo " Charts saved to: results/charts/"
echo "================================================================"
