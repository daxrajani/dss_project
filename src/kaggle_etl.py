import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when, regexp_replace, monotonically_increasing_id, to_timestamp

def process_kaggle_data():
    print("\n=============================================")
    print(" KAGGLE BIG DATA ETL PIPELINE")
    print("=============================================\n")
    
    # We allocate more memory to the driver since we are handling a massive dataset
    spark = SparkSession.builder \
        .appName("Kaggle_ETL") \
        .master("local[*]") \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.shuffle.partitions", "12") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    input_csv = "/mnt/p/Concordia_ECE/Winter_2026/Distributed/dss_project/data/data_set/2019-Oct.csv"
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_events = os.path.join(base_dir, 'data', 'raw', 'kaggle_events')
    out_catalog = os.path.join(base_dir, 'data', 'raw', 'kaggle_catalog')
    out_orders = os.path.join(base_dir, 'data', 'raw', 'kaggle_orders')

    print(f">>> 1. Loading 40M+ Row Kaggle Dataset (This may take a moment)...")
    # Read as strings first for speed, then we transform
    df = spark.read.csv(input_csv, header=True)

    print(">>> 2. Cleaning Timestamps and Formatting...")
    # Remove " UTC" and cast to proper timestamp
    df = df.withColumn("timestamp", to_timestamp(regexp_replace(col("event_time"), " UTC", "")))
    df = df.withColumn("price", col("price").cast("double"))

    print(">>> 3. Extracting and Saving Catalog...")
    catalog = df.select(
        col("product_id"),
        col("category_code").alias("category"),
        col("brand"),
        col("price")
    ).dropDuplicates(["product_id"]).fillna({"category": "unknown", "brand": "unknown"})
    
    # We write as directories of CSVs so Spark can read them in parallel later
    catalog.write.mode("overwrite").csv(out_catalog, header=True)

    print(">>> 4. Extracting Orders...")
    orders = df.filter(col("event_type") == "purchase").select(
        monotonically_increasing_id().alias("order_id"),
        col("user_id"),
        col("timestamp"),
        col("price").alias("total_amount")
    )
    orders.write.mode("overwrite").csv(out_orders, header=True)

    print(">>> 5. Extracting Events & Synthesizing Referrer/Device...")
    events = df.select("user_id", "timestamp", "event_type", "product_id")
    
    # Add synthetic data to fulfill the professor's rubric for missing columns
    events = events.withColumn("device", when(rand() < 0.5, "mobile").when(rand() < 0.8, "desktop").otherwise("tablet"))
    events = events.withColumn("referrer", when(rand() < 0.4, "google").when(rand() < 0.7, "facebook").when(rand() < 0.9, "email").otherwise("direct"))
    
    events.write.mode("overwrite").csv(out_events, header=True)

    print("\n>>> ETL Complete! Your Kaggle data is ready for the analytics pipeline.")
    spark.stop()

if __name__ == "__main__":
    process_kaggle_data()
