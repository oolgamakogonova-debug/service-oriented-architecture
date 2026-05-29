"""Конфигурация WMS-API сервиса (берётся из переменных окружения)."""
from __future__ import annotations

import os
from pathlib import Path


class Config:
    # Kafka / Schema Registry
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "warehouse-events")

    # Cassandra (read-path для GET /inventory)
    CASSANDRA_HOSTS = [
        h.strip()
        for h in os.environ.get("CASSANDRA_HOSTS", "cassandra-1").split(",")
        if h.strip()
    ]
    CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
    CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "warehouse")
    CASSANDRA_LOCAL_DC = os.environ.get("CASSANDRA_LOCAL_DC", "dc1")

    # HTTP
    HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

    # Режим запуска: "api" (по умолчанию) или "demo" (одноразовый сценарий и выход)
    MODE = os.environ.get("MODE", "api").lower()

    SCHEMAS_DIR = Path(os.environ.get("SCHEMAS_DIR", "/app/schemas"))


cfg = Config()
