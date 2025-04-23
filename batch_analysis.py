import psycopg2
import time
import psutil
import pandas as pd
from datetime import datetime

# Performance tracking
start_time = time.time()
cpu_start = psutil.cpu_percent(interval=1)

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="twitter_data",
    user="twitteruser",
    password="twitterpass",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

print("=== Batch SQL Analysis ===")

# Execute the SQL query to find most common tweets
print("Running SQL analysis: Finding most common tweets...")
query_start = time.time()
cur.execute("""
    SELECT tweet, COUNT(*) 
    FROM tweets_topic1 
    GROUP BY tweet 
    ORDER BY COUNT(*) DESC 
    LIMIT 10
""")
most_common_tweets = cur.fetchall()
query_time = time.time() - query_start

print(f"\nTop 10 most common tweets in tweets_topic1:")
for tweet, count in most_common_tweets:
    print(f"Count: {count} - Tweet: {tweet[:50]}..." if len(tweet) > 50 else f"Count: {count} - Tweet: {tweet}")

# Additional analysis - Average tweet length
print("\nRunning SQL analysis: Average tweet length...")
query2_start = time.time()
cur.execute("""
    SELECT AVG(LENGTH(tweet)) 
    FROM tweets_topic1
""")
avg_length = cur.fetchone()[0]
query2_time = time.time() - query2_start

print(f"Average tweet length: {avg_length:.2f} characters")

# Count tweets per topic table
print("\nRunning SQL analysis: Tweet count per topic...")
query3_start = time.time()
tweet_counts = {}
for table in ['tweets_topic1', 'tweets_topic2', 'tweets_topic3']:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    tweet_counts[table] = count
query3_time = time.time() - query3_start

print("Tweet counts per topic:")
for table, count in tweet_counts.items():
    print(f"{table}: {count} tweets")

# Final performance metrics for batch processing
end_time = time.time()
cpu_end = psutil.cpu_percent(interval=1)
total_duration = end_time - start_time
avg_cpu = (cpu_start + cpu_end) / 2

print("\n=== Batch Processing Performance Metrics ===")
print(f"Total Execution Time: {total_duration:.2f} seconds")
print(f"Most Common Tweets Query Time: {query_time:.2f} seconds")
print(f"Average Length Query Time: {query2_time:.2f} seconds")
print(f"Count Query Time: {query3_time:.2f} seconds")
print(f"Average CPU Usage: {avg_cpu:.2f}%")
print(f"Memory Usage: {psutil.Process().memory_info().rss / (1024 * 1024):.2f} MB")

# Read streaming metrics for comparison
streaming_metrics = []
try:
    with open("streaming_metrics.log", "r") as f:
        lines = f.readlines()
        for line in lines:
            parts = line.strip().split(',')
            if len(parts) >= 4:
                streaming_metrics.append({
                    'timestamp': parts[0],
                    'batch': parts[1],
                    'count': int(parts[2]),
                    'cpu': float(parts[3])
                })
except FileNotFoundError:
    print("No streaming metrics found. Run spark_streaming_app.py first.")

# Compare batch vs streaming performance
print("\n=== Streaming vs Batch Comparison ===")
if streaming_metrics:
    avg_streaming_cpu = sum(metric['cpu'] for metric in streaming_metrics) / len(streaming_metrics)
    avg_streaming_count = sum(metric['count'] for metric in streaming_metrics) / len(streaming_metrics)
    
    print(f"Batch Processing - Avg CPU: {avg_cpu:.2f}%, Total Time: {total_duration:.2f} sec")
    print(f"Stream Processing - Avg CPU: {avg_streaming_cpu:.2f}%, Avg Batch Size: {avg_streaming_count:.2f}")
    
    # Calculate efficiency metrics
    print("\nEfficiency Comparison:")
    if avg_streaming_count > 0:
        batch_efficiency = total_duration / sum(tweet_counts.values()) if sum(tweet_counts.values()) > 0 else 0
        print(f"Batch: {batch_efficiency:.6f} seconds per tweet")
        print(f"Streaming advantage: {'Yes' if batch_efficiency > 0.1 else 'No'}")
    
    # Save comparison data to file
    with open("performance_comparison.txt", "w") as f:
        f.write(f"Comparison done on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("BATCH PROCESSING:\n")
        f.write(f"Total Execution Time: {total_duration:.2f} seconds\n")
        f.write(f"Average CPU Usage: {avg_cpu:.2f}%\n")
        f.write(f"Total Records Processed: {sum(tweet_counts.values())}\n\n")
        
        f.write("STREAMING PROCESSING:\n")
        f.write(f"Average CPU Usage: {avg_streaming_cpu:.2f}%\n")
        f.write(f"Average Batch Size: {avg_streaming_count:.2f}\n")
    
    print("\nDetailed comparison saved to 'performance_comparison.txt'")
else:
    print("No streaming metrics available for comparison.")

# Close connections
cur.close()
conn.close()
print("\nAnalysis complete. Database connection closed.")
