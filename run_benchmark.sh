#!/bin/bash
set -e

echo "════════════════════════════════════════════════════"
echo "  DSS PIPELINE BENCHMARK"
echo "════════════════════════════════════════════════════"

# 1. Virtual environment
if [ ! -d "dss_env" ]; then
    echo ">>> Creating virtual environment..."
    python3 -m venv dss_env
fi
source dss_env/bin/activate

# 2. Dependencies
echo ">>> Installing dependencies..."
pip install -r requirements.txt --quiet

# 3. Clean only processed output — raw data and the Kaggle CSV are preserved
echo ">>> Cleaning processed output..."
rm -rf data/processed/*
mkdir -p data/processed

# 4. Resolve input: prefer Kaggle CSV, fall back to test slice (correct schema)
KAGGLE_CSV="data/data_set/2019-Oct.csv"
FALLBACK_CSV="data/raw/test_slice.csv"
if [ ! -f "$KAGGLE_CSV" ]; then
    echo ">>> Kaggle dataset not found at $KAGGLE_CSV"
    echo ">>> Falling back to test slice: $FALLBACK_CSV"
    INPUT_ARG="--input $FALLBACK_CSV"
else
    INPUT_ARG=""
fi

# 5. Run the complete pipeline benchmark
#    Any extra args (e.g. --input gs://... --output gs://...) are forwarded as-is.
echo ">>> Launching pipeline..."
python src/benchmark_job.py $INPUT_ARG "$@"

echo "════════════════════════════════════════════════════"
echo "  Benchmark complete."
echo "════════════════════════════════════════════════════"
