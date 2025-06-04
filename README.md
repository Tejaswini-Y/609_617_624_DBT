Real-Time Twitter Analytics
1)kafka-server-start.sh /usr/local/kafka/config/server.properties (apache kafka)
2)sudo /usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties  (zookeeper)
3}kafka-topics.sh --list --bootstrap-server localhost:9092 (to list the topics)
4)spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 --jars /home/pes2ug22cs624/Downloads/postgresql-42.7.3.jar /home/pes2ug22cs624/real_time_streaming/spark_streaming_app.py (streaming command)
5)sudo -u postgres psql (postresql)
\q (to exit that)

terminal 1 : start apache zookeeper
terminal 2 : start apache kafka
terminal 3 : start the streaming command
terminal 4 : start the python3 kafka_to_postgres.py
terminal 5 : start the python3 kafka_producer.py
terminal 6 : start python3 stream_output.py
terminal 7 : start python3 batch_analysis.py

# 🐦 Real-Time Twitter Analytics Pipeline

A dual-mode analytics system for real-time and batch processing of Twitter data using **Apache Kafka**, **Spark**, and **PostgreSQL**. This project demonstrates the power of distributed streaming architectures and compares the performance of streaming vs batch processing.

---

## 🚀 Overview

This pipeline bridges the gap between real-time analytics and reliable long-term storage. Tweets are:

1. **Streamed** into **Kafka** from a preprocessed dataset
2. **Processed in real-time** with **Spark Structured Streaming**
3. **Persisted** to **PostgreSQL** for further analysis

A secondary batch-mode run enables direct comparison between real-time and historical data processing.

---

## ⚙️ Architecture


- **Kafka**: Manages tweet streams across multiple topics
- **Spark Structured Streaming**: Handles transformations (e.g., hashtag counting, content filtering)
- **PostgreSQL**: Stores processed data
- **Monitoring**: Real-time performance logging (CPU, batch throughput)

---

## 📦 Tech Stack

| Tool         | Role                                   |
|--------------|----------------------------------------|
| Apache Kafka | Message broker for tweet streaming     |
| Apache Spark | Real-time & batch processing engine    |
| PostgreSQL   | Persistent storage backend             |
| Zookeeper    | Kafka cluster coordination             |

---

## 📥 Input Dataset

- **Name**: Twitter Sentiment Analysis Dataset  
- **File**: `training.1600000.processed.noemoticon.csv`  
- **Size**: 1.6 million tweets  
- **Format**: CSV  
- **Source**: Stanford University  

---

## 🔁 Streaming Mode

### ⏱ Configuration

- Kafka Topics: `twitter-stream-1`, `twitter-stream-2`, `twitter-stream-3`
- Interval: 0.1s between tweets
- Sliding Window: 5 min duration, 1 min slide, 1 min watermark
- Micro-Batch Size: 100 tweets

### 📊 Result

- Avg. Throughput: ~3.4 tweets/sec
- Successfully handled 350 input rows per batch
- Enables real-time tweet counting and trend visualization

---

## 📂 Batch Mode

- Operates on the full dataset of 1.6M tweets
- Processes data in optimized chunks
- Designed for historical trend analysis and sentiment evaluation

### 📊 Result

- Processed 100 tweets in 103.34s
- Avg. CPU Usage: 46.7%

---

## ⚖️ Performance Comparison

| Mode     | Tweets Processed | Avg. CPU | Execution Time |
|----------|------------------|----------|----------------|
| Batch    | 22,000+          | 13.15%   | 1.15 sec       |
| Streaming| 2,350            | 42.42%   | -              |

**Batch mode** is faster and more CPU-efficient.  
**Streaming mode** is ideal for real-time insights.

---

## 🛠 Installation

### Prerequisites

- Java 8+
- Python 3.7+
- Apache Kafka (with Zookeeper)
- Apache Spark (pre-built with Hadoop)
- PostgreSQL (with CLI)

### Setup Steps

```bash
# Start Zookeeper and Kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

# Create Kafka topics
bin/kafka-topics.sh --create --topic twitter-stream-1 --bootstrap-server localhost:9092
# Repeat for other topics...

# Run Spark Streaming script
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 your_streaming_script.py
