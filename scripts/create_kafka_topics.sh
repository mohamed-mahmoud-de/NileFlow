#!/bin/bash
# Create Kafka topics for NileFlow
# Run: docker exec nileflow-kafka bash /scripts/create_kafka_topics.sh

KAFKA_BIN="kafka-topics --bootstrap-server localhost:9092"

echo "Creating Kafka topics..."

$KAFKA_BIN --create --topic traffic_events \
    --partitions 3 --replication-factor 1 --if-not-exists

$KAFKA_BIN --create --topic weather_events \
    --partitions 1 --replication-factor 1 --if-not-exists

$KAFKA_BIN --create --topic vehicle_position_events \
    --partitions 3 --replication-factor 1 --if-not-exists

echo "Listing topics:"
$KAFKA_BIN --list

echo "Done."
