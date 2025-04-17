from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
import psutil  # For CPU usage
import time

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaTwitterStream") \
    .getOrCreate()

topics = "twitter-stream-1,twitter-stream-2,twitter-stream-3"

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topics) \
    .load()

# Kafka gives key/value as bytes, convert value to string
tweets = df.selectExpr("CAST(value AS STRING)").withColumnRenamed("value", "tweet")

# Transformation: tweet length
processed = tweets.withColumn("length", length(col("tweet")))

# Function to print metrics
def print_metrics(batch_df, batch_id):
    count = batch_df.count()
    cpu = psutil.cpu_percent(interval=1)
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

    print(f"\n=== Batch {batch_id} Metrics ===")
    print(f"Tweet Count: {count}")
    print(f"CPU Usage: {cpu}%")
    print(f"Timestamp: {timestamp}")
    print("===========================\n")

    # Save batch metrics to a log file for comparison
    with open("streaming_metrics.log", "a") as f:
        f.write(f"{timestamp},Batch {batch_id},{count},{cpu}\n")

    batch_df.show(truncate=False)


# Start streaming query to console
query = processed.writeStream \
    .foreachBatch(print_metrics) \
    .outputMode("append") \
    .start()

query.awaitTermination()
