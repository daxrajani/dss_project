#!/bin/bash
# Runs once when the Codespace is first created.
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
JAVA_BIN=$(which java 2>/dev/null || true)
if [[ -n "$JAVA_BIN" ]]; then
    JAVA_HOME_DIR=$(dirname $(dirname $(readlink -f "$JAVA_BIN")))
    echo "export JAVA_HOME=${JAVA_HOME_DIR}" >> ~/.bashrc
    echo "export JAVA_HOME=${JAVA_HOME_DIR}" >> ~/.profile
    export JAVA_HOME="${JAVA_HOME_DIR}"
    echo "    JAVA_HOME=${JAVA_HOME_DIR}"
else
    echo "    [WARN] java not found in PATH"
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
echo "  bash demo.sh results   — benchmark results (no GCP)"
echo "  bash demo.sh stream    — real-time streaming demo (no GCP)"
echo "  bash demo.sh perf      — performance charts (no GCP)"
echo "  bash dashboard.sh      — web dashboard on port 5000"
echo ""
echo "  GCP commands (need: gcloud auth login):"
echo "  bash demo.sh cloud     — run full pipeline on GCP Dataproc"
echo "  bash demo.sh analyze   — download GCS results + analytics"
echo "========================================================"
