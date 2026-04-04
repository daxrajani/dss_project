"""
prepare_benchmark_data.py — Create three size-variant Parquet datasets for
the GCP Dataproc scaling benchmark.

Source : REES46 2019-Oct.csv  (~42.4M rows, 5.3 GB as CSV)
Targets (CSV-equivalent input size):
  1gb  — random 19% sample   →  ~8M rows   ≈ 1 GB CSV equivalent
  5gb  — full dataset        →  ~42M rows  ≈ 5.3 GB CSV equivalent
  10gb — 2× replicated       →  ~85M rows  ≈ 10+ GB CSV equivalent

All three are saved as Parquet (raw string schema) so etl_enrichment.py
can read them via its parquet branch. The 10gb variant offsets user_id and
user_session in the duplicate copy so sessions remain logically distinct.

Usage:
    python src/prepare_benchmark_data.py
    python src/prepare_benchmark_data.py \\
        --input  gs://dss-project-dax/raw/2019-Oct.csv \\
        --output gs://dss-project-dax/data
"""

import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit


# ---------------------------------------------------------------------------
# Size config
# ---------------------------------------------------------------------------
SIZES = {
    "1gb":  {"mode": "sample", "fraction": 0.19,  "seed": 42},
    "5gb":  {"mode": "full"},
    "10gb": {"mode": "replicate", "copies": 2},
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_raw(spark, input_path):
    """Read source CSV keeping all columns as strings (matches ETL expectations)."""
    if input_path.endswith(".csv") or input_path.endswith(".csv.gz"):
        return spark.read.csv(input_path, header=True, inferSchema=False)
    return spark.read.parquet(input_path)


def _make_distinct_copy(df, copy_index):
    """
    Return a copy of df with user_id and user_session prefixed so that
    replicated rows form logically separate users/sessions in window functions.
    """
    prefix = f"r{copy_index}_"
    return (
        df
        .withColumn("user_id",      concat(lit(prefix), col("user_id")))
        .withColumn("user_session",  concat(lit(prefix), col("user_session")))
    )


def build_variant(raw_df, size_name, cfg):
    """Apply sampling / replication to produce the variant DataFrame."""
    mode = cfg["mode"]

    if mode == "sample":
        return raw_df.sample(withReplacement=False,
                             fraction=cfg["fraction"],
                             seed=cfg["seed"])

    if mode == "full":
        return raw_df

    if mode == "replicate":
        copies = cfg["copies"]
        # original copy + (copies-1) prefixed duplicates
        parts = [raw_df] + [_make_distinct_copy(raw_df, i)
                            for i in range(1, copies)]
        result = parts[0]
        for part in parts[1:]:
            result = result.union(part)
        return result

    raise ValueError(f"Unknown mode: {mode}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(input_path, output_dir, spark):
    total_start = time.time()

    print("\n>>> Reading source dataset ...")
    t0 = time.time()
    raw = _read_raw(spark, input_path)
    src_count = raw.cache().count()
    print(f"    Source rows : {src_count:,}  ({time.time() - t0:.1f}s)")

    results = {}

    for size_name, cfg in SIZES.items():
        print(f"\n{'=' * 54}")
        print(f"  Building variant: {size_name}  (mode={cfg['mode']})")
        print(f"{'=' * 54}")
        t0 = time.time()

        variant = build_variant(raw, size_name, cfg)

        out_path = os.path.join(output_dir, size_name)
        variant.write.mode("overwrite").parquet(out_path)

        elapsed = time.time() - t0
        # Count after write so Spark doesn't re-read source for every plan
        row_count = spark.read.parquet(out_path).count()
        results[size_name] = {"rows": row_count, "path": out_path, "time": elapsed}

        print(f"    Rows written : {row_count:,}")
        print(f"    Output path  : {out_path}")
        print(f"    Elapsed      : {elapsed:.1f}s")

    # Summary
    total = time.time() - total_start
    print("\n" + "=" * 54)
    print("  PREPARE BENCHMARK DATA — COMPLETE")
    print("=" * 54)
    print(f"  {'Variant':<8}  {'Rows':>12}  {'Path'}")
    print(f"  {'-'*8}  {'-'*12}  {'-'*30}")
    for name, info in results.items():
        print(f"  {name:<8}  {info['rows']:>12,}  {info['path']}")
    print(f"\n  Total time : {total:.1f}s")
    print("=" * 54)
    print()
    print("Next step — upload to GCS:")
    print(f"  gsutil -m cp -r {output_dir}/1gb  gs://dss-project-dax/data/")
    print(f"  gsutil -m cp -r {output_dir}/5gb  gs://dss-project-dax/data/")
    print(f"  gsutil -m cp -r {output_dir}/10gb gs://dss-project-dax/data/")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _default_input():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "data", "data_set", "2019-Oct.csv")


def _default_output():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "data", "benchmark")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DSS Benchmark Data Preparation")
    parser.add_argument(
        "--input",
        default=_default_input(),
        help="Source REES46 CSV path (local or gs://)",
    )
    parser.add_argument(
        "--output",
        default=_default_output(),
        help="Output directory for benchmark Parquet variants (local or gs://)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("DSS_Prepare_Benchmark_Data")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "=" * 54)
    print("  DSS BENCHMARK DATA PREPARATION")
    print("=" * 54)
    print(f"  Input  : {args.input}")
    print(f"  Output : {args.output}")
    print("=" * 54)

    run(args.input, args.output, spark)

    spark.stop()
