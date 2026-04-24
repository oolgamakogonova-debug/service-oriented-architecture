#!/bin/bash
set -e

echo "Registering Avro schema..."

SCHEMA=$(cat avro/movie_event.avsc | sed 's/"/\\"/g' | tr -d '\n')

curl -s -X POST http://localhost:8081/subjects/movie-events-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schemaType\": \"AVRO\", \"schema\": \"$SCHEMA\"}" | python3 -m json.tool

echo ""
echo "Schema registered successfully."