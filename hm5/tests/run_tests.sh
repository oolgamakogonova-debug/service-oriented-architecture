#!/bin/bash
set -e

echo "🧪 Running integration tests..."
echo "================================"

cd "$(dirname "$0")"

export PRODUCER_URL="http://localhost:8000"
export AGGREGATION_URL="http://localhost:8001"
export SCHEMA_REGISTRY_URL="http://localhost:8081"
export CLICKHOUSE_HOST="localhost"
export CLICKHOUSE_PORT_EXT="8123"
export MINIO_ENDPOINT_EXT="http://localhost:9001"

pip install -q -r requirements.txt 2>/dev/null

pytest -v --tb=short -s test_pipeline_integration.py

echo ""
echo "================================"
echo "✅ All tests passed!"