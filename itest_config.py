"""Адреса стенда для integration/e2e тестов (из env, с дефолтами под docker compose)."""
import os

API_URL = os.environ.get("API_URL", "http://localhost:8080")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "warehouse")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "warehouse-events-dlq")
