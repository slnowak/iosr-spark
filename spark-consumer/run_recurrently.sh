#!/bin/bash

while true; do
  sh /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 /spark/populate_view_model.py
  sleep 180
done
