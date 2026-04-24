#!/bin/bash
set -e

echo "Creating Kafka topics..."

kafka-topics --create --if-not-exists \
  --bootstrap-server kafka1:9092 \
  --topic movie-events \
  --partitions 3 \
  --replication-factor 2 \
  --config min.insync.replicas=1

echo "Topic movie-events created successfully."

kafka-topics --describe \
  --bootstrap-server kafka1:9092 \
  --topic movie-events