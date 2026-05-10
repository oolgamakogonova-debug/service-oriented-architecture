import os


class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "warehouse-events")
    DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "warehouse-events-dlq")
    CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "warehouse-state-consumer")

    CASSANDRA_HOSTS = [
        h.strip() for h in os.environ.get("CASSANDRA_HOSTS", "cassandra-1,cassandra-2,cassandra-3").split(",")
    ]
    CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
    CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "warehouse")

    HTTP_PORT = int(os.environ.get("HTTP_PORT", "8000"))
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

    POLL_TIMEOUT_SEC = float(os.environ.get("POLL_TIMEOUT_SEC", "1.0"))
    MAX_POLL_RECORDS = int(os.environ.get("MAX_POLL_RECORDS", "100"))


cfg = Config()
