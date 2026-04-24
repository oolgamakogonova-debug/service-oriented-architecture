import os

KAFKA_BOOTSTRAP_SERVERS = f"{os.getenv('KAFKA_BROKER_1', 'kafka1:9092')},{os.getenv('KAFKA_BROKER_2', 'kafka2:9093')}"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "movie-events")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

PRODUCER_HOST = os.getenv("PRODUCER_HOST", "0.0.0.0")
PRODUCER_PORT = int(os.getenv("PRODUCER_PORT", "8000"))

GENERATOR_ENABLED = os.getenv("GENERATOR_ENABLED", "true").lower() == "true"
GENERATOR_EVENTS_PER_SECOND = int(os.getenv("GENERATOR_EVENTS_PER_SECOND", "10"))
GENERATOR_NUM_USERS = int(os.getenv("GENERATOR_NUM_USERS", "100"))
GENERATOR_NUM_MOVIES = int(os.getenv("GENERATOR_NUM_MOVIES", "50"))