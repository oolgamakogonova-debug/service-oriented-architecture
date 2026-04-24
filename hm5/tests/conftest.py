import os
import time
import pytest
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://localhost:8000")
AGGREGATION_URL = os.getenv("AGGREGATION_URL", "http://localhost:8001")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=cinema_aggregates user=cinema password=cinema_secret",
)
MINIO_URL = os.getenv("MINIO_URL", "http://localhost:9001")


@retry(stop=stop_after_attempt(30), wait=wait_fixed(2))
def wait_for_service(url: str, name: str):
    resp = requests.get(url, timeout=5)
    assert resp.status_code == 200, f"{name} not ready: {resp.status_code}"
    print(f"✅ {name} is ready")


@pytest.fixture(scope="session", autouse=True)
def wait_for_all_services():
    print("\n⏳ Waiting for services to be ready...")
    wait_for_service(f"{PRODUCER_URL}/health", "Producer")
    wait_for_service(f"{AGGREGATION_URL}/health", "Aggregation")
    wait_for_service(f"{SCHEMA_REGISTRY_URL}/subjects", "Schema Registry")
    wait_for_service(f"{CLICKHOUSE_URL}/ping", "ClickHouse")
    # Give some time for events to flow
    print("⏳ Waiting 15s for events to flow through pipeline...")
    time.sleep(15)
    print("✅ All services ready")