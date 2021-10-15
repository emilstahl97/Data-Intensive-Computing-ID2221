#!/bin/bash
#Script for reading files into kafka

/home/emilstahl/kafka_2.11-2.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $1
cat $2 | /home/emilstahl/kafka_2.11-2.0.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic $1
