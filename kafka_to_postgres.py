from kafka import KafkaConsumer
import psycopg2
import time
import psutil

# Kafka Consumer
consumer = KafkaConsumer(
    'twitter-stream-1',
    'twitter-stream-2',
    'twitter-stream-3',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: m.decode('utf-8')
)

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="twitter_data",
    user="twitteruser",
    password="twitterpass",  # replace with your password
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Batch size and interval settings
batch_size = 100
batch = []

#  Performance tracking
start_time = time.time()
cpu_start = psutil.cpu_percent(interval=1)
total_tweets_processed = 0

print("Consuming tweets in batch...")

try:
    for message in consumer:
        tweet = message.value
        topic = message.topic

        batch.append((topic, tweet))

        if len(batch) >= batch_size:
            for topic, tweet in batch:
                if topic == 'twitter-stream-1':
                    cur.execute("INSERT INTO tweets_topic1 (tweet, processed_time) VALUES (%s, NOW())", (tweet,))
                elif topic == 'twitter-stream-2':
                    cur.execute("INSERT INTO tweets_topic2 (tweet, processed_time) VALUES (%s, NOW())", (tweet,))
                elif topic == 'twitter-stream-3':
                    cur.execute("INSERT INTO tweets_topic3 (tweet, processed_time) VALUES (%s, NOW())", (tweet,))
            
            conn.commit()
            total_tweets_processed += batch_size  #  Update tweet counter
            print(f"Processed a batch of {batch_size} tweets.")
            batch.clear()

        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopped by user.")
except Exception as e:
    print(f"Error processing batch: {e}")
finally:
    #  Final performance metrics
    end_time = time.time()
    cpu_end = psutil.cpu_percent(interval=1)
    duration = end_time - start_time
    avg_cpu = (cpu_start + cpu_end) / 2

    print("\n=== Batch Processing Metrics ===")
    print(f"Total Tweets Processed: {total_tweets_processed}")
    print(f"Total Time: {duration:.2f} sec")
    print(f"Avg CPU Usage: {avg_cpu}%")
    print("===============================\n")

    cur.close()
    conn.close()
    consumer.close()
    print("Connections closed.")

