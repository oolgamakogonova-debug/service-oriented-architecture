#!/bin/bash
set -e

echo "Running ClickHouse migrations..."

for f in /migrations/*.sql; do
    echo "Applying migration: $f"
    clickhouse-client --multiquery < "$f"
done

echo "Migrations completed."