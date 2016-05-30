# Basic sentiment Analytics using Spark streaming API

Description: 
* This project demonstrates a basic sentiment application using the spark streaming API and Kafka in python.
* Kafka provides a distributed queuing service, which in this project is used to buffer the tweets before processing.
* Kafka comes in handy when the data creation rate is more than the processing rate.

Instructions to run:

Start Zookeeper service
* $ bin/zookeeper-server-start.sh config/zookeeper.properties
Start kafka service:
* $ bin/kafka-server-start.sh config/server.properties
Create a topic named twitterstream in kafka:
* $bin/kafka-topics.sh --create --zookeeper localhost:2181 -- replication-factor 1 --partitions 1 --topic twitterstream

Start downloading tweets from the twitter streaming API and push them to the twitterstream topic in Kafka:
* $python twitter_to_kafka.py
Running the sentiment analysis program
* $ $SPARK/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.1 twitterStream.py
