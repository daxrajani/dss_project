#!/bin/bash
# DSS Demo Dashboard — start the web UI
# Open http://localhost:5000 in your browser after running this.

VENV="$HOME/dss_venv/bin/python3"
SCRIPT="$(dirname "$0")/src/dashboard/app.py"

echo ""
echo "  ┌─────────────────────────────────────────────┐"
echo "  │  DSS Pipeline Dashboard                      │"
echo "  │  Open → http://localhost:5000                │"
echo "  │  Ctrl+C to stop                              │"
echo "  └─────────────────────────────────────────────┘"
echo ""

$VENV "$SCRIPT"
