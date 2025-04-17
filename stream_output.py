import psycopg2
import time
import psutil

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

cur.close()
conn.close()

print("\n=== Streaming Metrics Log ===")
try:
    with open("streaming_metrics.log", "r") as f:
        lines = f.readlines()
        for line in lines[-10:]:  # Show last 10 streaming batches
            print(line.strip())
except FileNotFoundError:
    print("Streaming metrics log not found. Run spark_streaming_app.py first.")

