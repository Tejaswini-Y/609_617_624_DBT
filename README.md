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
