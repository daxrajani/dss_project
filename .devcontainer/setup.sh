#!/bin/bash
# Runs once when the Codespace is first created.
# Sets up everything needed to run the DSS demo — no manual steps required.

set -e

echo "========================================================"
echo "  DSS Project — Codespace Setup"
echo "========================================================"

# ── Python packages ──────────────────────────────────────────────────
echo ""
echo ">>> Installing Python packages ..."
pip install --quiet \
    pyspark \
    pandas \
    pyarrow \
    matplotlib \
    flask \
    requests

echo "    Done."

# ── JAVA_HOME for PySpark ────────────────────────────────────────────
echo ""
echo ">>> Configuring Java for PySpark ..."
JAVA_PATH=$(which java 2>/dev/null || true)
if [[ -n "$JAVA_PATH" ]]; then
    JAVA_HOME_DIR=$(dirname $(dirname $(readlink -f "$JAVA_PATH")))
    echo "export JAVA_HOME=${JAVA_HOME_DIR}" >> ~/.bashrc
    echo "export JAVA_HOME=${JAVA_HOME_DIR}" >> ~/.profile
    echo "    JAVA_HOME=${JAVA_HOME_DIR}"
else
    echo "    [WARN] java not found — streaming demo may not work"
fi

# ── Make scripts executable ──────────────────────────────────────────
echo ""
echo ">>> Making scripts executable ..."
chmod +x demo.sh dashboard.sh 2>/dev/null || true
chmod +x src/*.sh 2>/dev/null || true
echo "    Done."

# ── Print welcome message ────────────────────────────────────────────
echo ""
echo "========================================================"
echo "  Setup complete! Commands you can run:"
echo ""
echo "  bash demo.sh results   — benchmark results + charts"
echo "  bash demo.sh stream    — real-time streaming demo"
echo "  bash demo.sh perf      — performance charts"
echo "  bash demo.sh cloud     — run on GCP (needs: gcloud auth login)"
echo "  bash demo.sh analyze   — show analytics from GCS"
echo ""
echo "  python src/dashboard/app.py  — web dashboard (port 5000)"
echo "========================================================"
