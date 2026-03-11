import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, unix_timestamp, lag, when, sum as _sum, max as _max, concat_ws, to_date, round, last, row_number, expr

# --- PHASE A: SESSIONIZATION ---
def build_sessions(spark, events_path):
    df = spark.read.csv(events_path, header=True, inferSchema=True)
    user_window = Window.partitionBy("user_id").orderBy("timestamp")
    
    df = df.withColumn("prev_timestamp", lag("timestamp").over(user_window))
    df = df.withColumn("time_diff_seconds", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))
    
    df = df.withColumn("is_new_session", when(col("time_diff_seconds") > 1800, 1).when(col("prev_timestamp").isNull(), 1).otherwise(0))
    df = df.withColumn("session_idx", _sum("is_new_session").over(user_window))
    
    return df.withColumn("session_id", concat_ws("_", col("user_id"), col("session_idx"))) \
             .drop("prev_timestamp", "time_diff_seconds", "is_new_session", "session_idx")

# --- PHASE B: FUNNEL ANALYSIS ---
def analyze_funnels(spark, sessionized_events, catalog_path):
    catalog_df = spark.read.csv(catalog_path, header=True, inferSchema=True)
    enriched_df = sessionized_events.join(catalog_df, on="product_id", how="left")
    enriched_df = enriched_df.withColumn("date", to_date("timestamp"))
    
    funnel_flags = enriched_df.withColumn("is_view", when(col("event_type") == "view", 1).otherwise(0)) \
                              .withColumn("is_cart", when(col("event_type") == "add_to_cart", 1).otherwise(0)) \
                              .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0))
    
    session_funnel = funnel_flags.groupBy("session_id", "date", "category", "device", "referrer") \
                                 .agg(_max("is_view").alias("has_view"),
                                      _max("is_cart").alias("has_cart"),
                                      _max("is_purchase").alias("has_purchase"))
    
    final_funnel = session_funnel.groupBy("date", "category", "device", "referrer") \
                                 .agg(_sum("has_view").alias("total_sessions_with_view"),
                                      _sum("has_cart").alias("total_sessions_with_cart"),
                                      _sum("has_purchase").alias("total_sessions_with_purchase"))
                                      
    final_rates = final_funnel.withColumn(
        "view_to_cart_pct", when(col("total_sessions_with_view") == 0, 0.0).otherwise(round((col("total_sessions_with_cart") / col("total_sessions_with_view")) * 100, 2))
    ).withColumn(
        "cart_to_buy_pct", when(col("total_sessions_with_cart") == 0, 0.0).otherwise(round((col("total_sessions_with_purchase") / col("total_sessions_with_cart")) * 100, 2))
    )
                              
    return final_rates.orderBy("date", "category", ascending=False)

# --- PHASE C: LAST-TOUCH ATTRIBUTION ---
def attribute_orders(spark, sessionized_events, orders_path):
    print(">>> Executing Complex Attribution Join...")
    orders_df = spark.read.csv(orders_path, header=True, inferSchema=True)
    
    # We only care about events that could have driven the sale (not direct traffic)
    # The professor specified: "most recent non-direct referrer"
    valid_events = sessionized_events.filter(col("referrer") != "direct")
    
    # Join Orders with valid events on user_id where the event happened BEFORE the order
    joined_df = orders_df.alias("o").join(
        valid_events.alias("e"),
        on="user_id",
        how="left"
    ).filter(col("e.timestamp") <= col("o.timestamp"))
    
    # Optional constraint: Filter out events older than 24 hours (86400 seconds) from the order
    time_constrained_df = joined_df.filter(
        (unix_timestamp("o.timestamp") - unix_timestamp("e.timestamp")) <= 86400
    )
    
    # Window to find the MOST RECENT event for each order
    attribution_window = Window.partitionBy("o.order_id").orderBy(col("e.timestamp").desc())
    
    ranked_df = time_constrained_df.withColumn("rank", row_number().over(attribution_window))
    
    # Filter to only the #1 most recent non-direct event per order
    final_attribution = ranked_df.filter(col("rank") == 1).select(
        col("o.order_id"),
        col("o.user_id"),
        col("o.timestamp").alias("order_time"),
        col("o.total_amount"),
        col("e.referrer").alias("attributed_referrer"),
        col("e.timestamp").alias("referral_time")
    )
    
    return final_attribution

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Ecommerce_Analytics") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    events_path = os.path.join(base_dir, 'data', 'raw', 'events.csv')
    catalog_path = os.path.join(base_dir, 'data', 'raw', 'catalog.csv')
    orders_path = os.path.join(base_dir, 'data', 'raw', 'orders.csv')
    
    print("\n=============================================")
    print(" DSS E-COMMERCE ANALYTICS PIPELINE")
    print("=============================================\n")
    
    print(">>> Phase A: Building Sessions...")
    sessionized_events = build_sessions(spark, events_path)
    # Cache this DataFrame because we use it in both Phase B and Phase C
    sessionized_events.cache()
    
    print("\n>>> Phase B: Building Conversion Funnels...")
    funnel_metrics = analyze_funnels(spark, sessionized_events, catalog_path)
    print(">>> Sample Funnel Metrics:")
    funnel_metrics.show(5, truncate=False)
    
    print("\n>>> Phase C: Executing Last-Touch Attribution...")
    attribution_results = attribute_orders(spark, sessionized_events, orders_path)
    print(">>> Order Attribution Results:")
    attribution_results.show(10, truncate=False)
    
    spark.stop()
