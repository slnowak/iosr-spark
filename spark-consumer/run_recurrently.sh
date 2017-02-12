#!/bin/bash

while true; do
  sh /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 /spark/most_popular_airports.py
  sleep 300
done
