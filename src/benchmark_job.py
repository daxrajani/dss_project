import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

def run_job(master_config, job_name):
    print(f"\n>>> [{job_name}] Initializing Spark with '{master_config}'...")
    
    # We configure Spark dynamically based on the master_config passed in
    spark = SparkSession.builder \
        .appName(job_name) \
        .master(master_config) \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_file = os.path.join(base_dir, 'data', 'raw', 'massive_events.csv')
    
    start_time = time.time()
    
    print(f">>> [{job_name}] Reading 2 million rows...")
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    
    # DSS Concept: Show how the data is partitioned across the nodes/cores
    print(f">>> [{job_name}] Data partitioned into {df.rdd.getNumPartitions()} blocks.")
    
    print(f">>> [{job_name}] Executing heavy shuffle and aggregation...")
    # This simulates a heavy workload by forcing Spark to shuffle data across partitions
    result_df = df.groupBy("user_id", "event_type") \
                  .agg(count("*").alias("event_count")) \
                  .orderBy("event_count", ascending=False)
    
    # PySpark is lazy. We must call an action like .count() to actually trigger the computation
    total_grouped = result_df.count()
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f">>> [{job_name}] Finished! Processed down to {total_grouped} unique user-event groups.")
    
    spark.stop()
    return duration

if __name__ == "__main__":
    print("\n==================================================")
    print(" DSS BENCHMARK: SINGLE-CORE VS MULTI-CORE")
    print("==================================================")
    
    # Test 1: Restrict Spark to 1 Core (No Parallelism)
    time_single = run_job("local[1]", "Single-Core-Worker")
    
    # Test 2: Allow Spark to use all available CPU Cores (Distributed Parallelism)
    time_multi = run_job("local[*]", "Multi-Core-Cluster")
    
    print("\n==================================================")
    print(" FINAL BENCHMARK RESULTS")
    print("==================================================")
    print(f" Single-Core Time : {time_single:.2f} seconds")
    print(f" Multi-Core Time  : {time_multi:.2f} seconds")
    
    if time_single > time_multi:
        speedup = time_single / time_multi
        print(f" Performance Gain : {speedup:.2f}x Faster with DSS Parallelism!")
    print("==================================================\n")
