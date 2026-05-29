#!/usr/bin/env bash
# Ждёт готовности стенда: WMS-API, consumer, Prometheus, kafka-exporter.
# Используется в CI и локально (make wait).
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
CONSUMER_URL="${CONSUMER_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
KAFKA_EXPORTER_URL="${KAFKA_EXPORTER_URL:-http://localhost:9308}"
TIMEOUT="${TIMEOUT:-300}"

wait_for() {
  local name="$1" url="$2" deadline=$((SECONDS + TIMEOUT))
  echo "Waiting for ${name} (${url}) ..."
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "ERROR: ${name} не поднялся за ${TIMEOUT}s" >&2
      return 1
    fi
    sleep 3
  done
  echo "OK: ${name}"
}

wait_for "WMS-API"        "${API_URL}/health"
wait_for "consumer"       "${CONSUMER_URL}/metrics"
wait_for "Prometheus"     "${PROM_URL}/-/ready"
wait_for "kafka-exporter" "${KAFKA_EXPORTER_URL}/metrics"

# Дать Prometheus сделать хотя бы пару scrape-итераций.
sleep 8
echo "Стенд готов."
