from kafka import KafkaProducer
import time
import csv

producer = KafkaProducer(bootstrap_servers='localhost:9092')
file_path = 'training.1600000.processed.noemoticon.csv'
topics = ['twitter-stream-1', 'twitter-stream-2', 'twitter-stream-3']

with open(file_path, encoding='latin-1') as f:
    reader = csv.reader(f)
    i = 0
    for row in reader:
        if len(row) >= 6:
            tweet = row[5]
            topic = topics[i % 3]
            producer.send(topic, tweet.encode('utf-8'))
            print(f"Sent to {topic}: {tweet}")
            time.sleep(0.1)
            i += 1

