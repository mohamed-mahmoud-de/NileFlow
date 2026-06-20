#!/bin/bash
# Create Elasticsearch indices for NileFlow
# Run after Elasticsearch is fully up:
#   docker exec nileflow-elasticsearch bash /init/create_indices.sh
# Or from host:
#   bash scripts/init_elasticsearch.sh

ES_URL="http://localhost:9200"

echo "Waiting for Elasticsearch to be ready..."
until curl -s "$ES_URL/_cluster/health" | grep -q '"status":"green"\|"status":"yellow"'; do
    sleep 2
done
echo "Elasticsearch is ready."

echo "Creating congestion_alerts index..."
curl -s -X PUT "$ES_URL/congestion_alerts" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "corridor_id":      { "type": "keyword" },
      "alert_type":       { "type": "keyword" },
      "severity":         { "type": "keyword" },
      "congestion_index": { "type": "double" },
      "speed_kmh":        { "type": "double" },
      "baseline_speed":   { "type": "double" },
      "message":          { "type": "text" },
      "event_time":       { "type": "date" },
      "created_at":       { "type": "date" }
    }
  }
}'
echo ""

echo "Creating pipeline_events index..."
curl -s -X PUT "$ES_URL/pipeline_events" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "event_type":   { "type": "keyword" },
      "source":       { "type": "keyword" },
      "corridor_id":  { "type": "keyword" },
      "message":      { "type": "text" },
      "details":      { "type": "object", "enabled": true },
      "event_time":   { "type": "date" },
      "created_at":   { "type": "date" }
    }
  }
}'
echo ""

echo "Elasticsearch indices created successfully."
