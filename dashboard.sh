#!/bin/bash
# DSS Demo Dashboard — start the web UI
# Open http://localhost:5000 in your browser after running this.

PYTHON=$(command -v python3 2>/dev/null || command -v python 2>/dev/null || echo python3)
SCRIPT="$(cd "$(dirname "$0")" && pwd)/src/dashboard/app.py"

echo ""
echo "  ┌─────────────────────────────────────────────┐"
echo "  │  DSS Pipeline Dashboard                      │"
echo "  │  Open → http://localhost:5000                │"
echo "  │  Ctrl+C to stop                              │"
echo "  └─────────────────────────────────────────────┘"
echo ""

$PYTHON "$SCRIPT"
