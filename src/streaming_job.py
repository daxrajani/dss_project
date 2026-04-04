import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def run_streaming_job():
    print("\n>>> Initializing DSS Real-Time Streaming Pipeline...")
    
    spark = SparkSession.builder \
        .appName("DSS-Live-Stream") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Define relative paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_dir = os.path.join(base_dir, 'data', 'raw', 'stream_input')
    output_dir = os.path.join(base_dir, 'data', 'processed', 'stream_output')
    checkpoint_dir = os.path.join(base_dir, 'data', 'processed', 'stream_checkpoints')

    # Ensure directories exist
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # Streaming requires a strict schema definition
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("value", DoubleType(), True)
    ])

    print(f">>> Listening for new data drops in: {input_dir}")
    
    # 1. READ STREAM: Watch the folder for new CSV files
    streaming_df = spark.readStream \
        .schema(schema) \
        .csv(input_dir, header=False) # Assuming live drops might not have headers

    # 2. TRANSFORM: Simple aggregation to show processing
    # Note: We group by event_type to keep the stream lightweight
    transformed_stream = streaming_df.groupBy("event_type").count()

    # 3. WRITE STREAM: Output results to the console in real-time
    print(">>> Starting micro-batch processing. Waiting for data...\n")
    query = transformed_stream.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime="5 seconds") \
        .start()

    # Keep the stream running until manually stopped
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n>>> Stream gracefully terminated by user.")
        query.stop()

if __name__ == "__main__":
    run_streaming_job()
