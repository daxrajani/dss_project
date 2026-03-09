#!/bin/bash

echo ">>> Starting DSS Project Setup..."

# 1. Create and activate a fresh virtual environment
if [ ! -d "dss_env" ]; then
    echo ">>> Creating virtual environment (this may take a moment)..."
    python3 -m venv dss_env
fi
source dss_env/bin/activate

# 2. Install dependencies
echo ">>> Installing dependencies..."
pip install -r requirements.txt

# 3. Clean old data and ensure directories exist
echo ">>> Cleaning old data directories..."
rm -rf data/raw/* data/processed/*
mkdir -p data/raw data/processed

# 4. Generate Data
echo ">>> Generating massive dataset for benchmark..."
python src/generator.py

# 5. Run the Benchmark (We will add the PySpark job here next)
python src/benchmark_job.py

echo ">>> Setup complete! You are ready to benchmark."
