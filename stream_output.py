import psycopg2
import time
import psutil
import pandas as pd
from datetime import datetime

# PostgreSQL setup
conn = psycopg2.connect(
    dbname="twitter_data",
    user="twitteruser",
    password="twitterpass",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

topics = ['twitter-stream-1', 'twitter-stream-2', 'twitter-stream-3']
tables = ['tweets_topic1', 'tweets_topic2', 'tweets_topic3']

print("\n=== Accuracy Check ===")
for topic, table in zip(topics, tables):
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    print(f"{topic} -> {table}: {count} tweets")

# Performance metrics for streaming mode
print("\n=== Streaming Metrics Log ===")
streaming_metrics = []
try:
    with open("streaming_metrics.log", "r") as f:
        lines = f.readlines()
        # Print last 10 lines
        print("Last 10 streaming batches:")
        for line in lines[-10:]:
            print(line.strip())
        
        # Process all metrics
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
    print("Streaming metrics log not found. Run spark_streaming_app.py first.")

# Calculate streaming performance metrics
if streaming_metrics:
    avg_cpu = sum(metric['cpu'] for metric in streaming_metrics) / len(streaming_metrics)
    avg_count = sum(metric['count'] for metric in streaming_metrics) / len(streaming_metrics)
    total_processed = sum(metric['count'] for metric in streaming_metrics)
    
    print("\n=== Streaming Performance Summary ===")
    print(f"Total Batches Processed: {len(streaming_metrics)}")
    print(f"Total Records Processed: {total_processed}")
    print(f"Average Batch Size: {avg_count:.2f}")
    print(f"Average CPU Usage: {avg_cpu:.2f}%")

# Run batch mode SQL analysis for comparison
print("\n=== Running Batch SQL Analysis for Comparison ===")
start_time = time.time()
cpu_start = psutil.cpu_percent(interval=1)

# Execute common SQL analysis queries
print("Finding most common tweets...")
cur.execute("""
    SELECT tweet, COUNT(*) 
    FROM tweets_topic1 
    GROUP BY tweet 
    ORDER BY COUNT(*) DESC 
    LIMIT 5
""")
top_tweets = cur.fetchall()
print("Top 5 most common tweets:")
for tweet, count in top_tweets:
    tweet_preview = tweet[:40] + "..." if len(tweet) > 40 else tweet
    print(f"  Count: {count} - Tweet: {tweet_preview}")

# Calculate final batch metrics
end_time = time.time()
cpu_end = psutil.cpu_percent(interval=1)
batch_duration = end_time - start_time
batch_cpu = (cpu_start + cpu_end) / 2

print("\n=== Batch vs Streaming Comparison ===")
print(f"Batch SQL Analysis Time: {batch_duration:.2f} seconds")
print(f"Batch CPU Usage: {batch_cpu:.2f}%")

if streaming_metrics:
    print(f"Stream Processing - Avg CPU: {avg_cpu:.2f}%")
    print("\nPerformance Conclusion:")
    if batch_cpu > avg_cpu:
        print(f"Streaming mode used {batch_cpu - avg_cpu:.2f}% less CPU on average")
    else:
        print(f"Batch mode used {avg_cpu - batch_cpu:.2f}% less CPU on average")
    
    print("\nUse Case Recommendations:")
    print("- Streaming is better for real-time analysis and immediate insights")
    print("- Batch processing is better for comprehensive historical analysis")
    
    # Save comparison summary to file
    with open("mode_comparison_summary.txt", "w") as f:
        f.write(f"Comparison generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("STREAMING MODE:\n")
        f.write(f"Average CPU Usage: {avg_cpu:.2f}%\n")
        f.write(f"Total Records Processed: {total_processed}\n")
        f.write(f"Average Batch Size: {avg_count:.2f}\n\n")
        
        f.write("BATCH MODE:\n")
        f.write(f"Analysis Time: {batch_duration:.2f} seconds\n")
        f.write(f"CPU Usage: {batch_cpu:.2f}%\n\n")
        
        f.write("CONCLUSION:\n")
        if batch_cpu > avg_cpu:
            f.write(f"Streaming mode was more CPU efficient by {batch_cpu - avg_cpu:.2f}%\n")
        else:
            f.write(f"Batch mode was more CPU efficient by {avg_cpu - batch_cpu:.2f}%\n")
    
    print("\nDetailed comparison saved to 'mode_comparison_summary.txt'")

# Close connection
cur.close()
conn.close()
