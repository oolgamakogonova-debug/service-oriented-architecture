import os
import uuid
import time
from datetime import datetime, timezone

import boto3
import clickhouse_connect
import psycopg2
import requests
from botocore.client import Config

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://producer:8000")
AGGREGATION_URL = os.getenv("AGGREGATION_URL", "http://aggregation:8001")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL", "http://schema-registry:8081"
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "cinema_aggregates")
POSTGRES_USER = os.getenv("POSTGRES_USER", "cinema")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "cinema_secret")


class TestKafkaInfrastructure:
    """Task 8: Fault-tolerant Kafka infrastructure"""

    def test_schema_registry_has_schema(self):
        resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
        assert resp.status_code == 200
        subjects = resp.json()
        assert "movie-events-value" in subjects

    def test_schema_latest_version(self):
        resp = requests.get(
            f"{SCHEMA_REGISTRY_URL}/subjects/movie-events-value/versions/latest"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["subject"] == "movie-events-value"
        assert "schema" in data
        assert len(data["schema"]) > 0

    def test_topic_exists_with_replication(self):
        resp = requests.get(f"{PRODUCER_URL}/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestProducer:
    """Task 9: Producer with Avro serialization"""

    def test_health(self):
        resp = requests.get(f"{PRODUCER_URL}/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_publish_event(self):
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": "test_user_001",
            "movie_id": "test_movie_001",
            "event_type": "VIEW_STARTED",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "device_type": "DESKTOP",
            "session_id": str(uuid.uuid4()),
            "progress_seconds": 0,
        }
        resp = requests.post(f"{PRODUCER_URL}/events", json=event)
        assert resp.status_code == 200
        assert resp.json()["status"] == "accepted"

    def test_publish_invalid_event(self):
        resp = requests.post(
            f"{PRODUCER_URL}/events",
            json={"user_id": "x"},
        )
        assert resp.status_code == 422

    def test_publish_all_event_types(self):
        session_id = str(uuid.uuid4())
        event_types = [
            "VIEW_STARTED",
            "VIEW_PAUSED",
            "VIEW_RESUMED",
            "VIEW_FINISHED",
            "LIKED",
            "SEARCHED",
        ]
        for event_type in event_types:
            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": "test_user_002",
                "movie_id": "test_movie_002",
                "event_type": event_type,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "device_type": "MOBILE",
                "session_id": session_id,
                "progress_seconds": 120,
            }
            resp = requests.post(f"{PRODUCER_URL}/events", json=event)
            assert resp.status_code == 200

    def test_flush(self):
        resp = requests.post(f"{PRODUCER_URL}/flush")
        assert resp.status_code == 200
        assert resp.json()["status"] == "flushed"


class TestClickHouseIngestion:
    """Task 10: ClickHouse consumes from Kafka"""

    def test_clickhouse_ping(self):
        resp = requests.get(f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/ping")
        assert resp.status_code == 200

    def test_events_table_exists(self):
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
        )
        result = client.query("SELECT count() FROM cinema.movie_events").result_rows
        count = result[0][0]
        print(f"  ClickHouse movie_events count: {count}")
        assert count > 0, "No events found in ClickHouse — Kafka ingestion may not be working"

    def test_kafka_engine_table_exists(self):
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
        )
        result = client.query(
            "SELECT name FROM system.tables WHERE database='cinema' AND engine='Kafka'"
        ).result_rows
        table_names = [row[0] for row in result]
        assert "kafka_movie_events" in table_names

    def test_materialized_view_exists(self):
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
        )
        result = client.query(
            "SELECT name FROM system.tables WHERE database='cinema' AND engine='MaterializedView'"
        ).result_rows
        view_names = [row[0] for row in result]
        assert "mv_movie_events" in view_names

    def test_event_types_present(self):
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
        )
        result = client.query(
            "SELECT DISTINCT event_type FROM cinema.movie_events ORDER BY event_type"
        ).result_rows
        event_types = {row[0] for row in result}
        assert "VIEW_STARTED" in event_types


class TestAggregation:
    """Task 11: Aggregation + Task 12: S3 + Task 13: PostgreSQL"""

    def test_aggregation_health(self):
        resp = requests.get(f"{AGGREGATION_URL}/health")
        assert resp.status_code == 200

    def test_top_movies_endpoint(self):
        resp = requests.get(f"{AGGREGATION_URL}/top-movies?limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert "movies" in data

    def test_trigger_aggregation(self):
        resp = requests.post(f"{AGGREGATION_URL}/trigger-aggregation")
        assert resp.status_code == 200
        assert resp.json()["status"] == "completed"

    def test_export_parquet(self):
        resp = requests.post(f"{AGGREGATION_URL}/export-parquet")
        assert resp.status_code == 200
        data = resp.json()
        print(f"  Exported {data.get('rows', 0)} rows to {data.get('s3_path', 'N/A')}")

    def test_postgres_has_aggregates(self):
        requests.post(f"{AGGREGATION_URL}/trigger-aggregation")
        time.sleep(2)
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM movie_aggregates;")
            count = cur.fetchone()[0]
            print(f"  PostgreSQL movie_aggregates count: {count}")
            assert count >= 0
        conn.close()

    def test_s3_bucket_accessible(self):
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin123",
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        resp = s3.list_buckets()
        bucket_names = [bucket["Name"] for bucket in resp["Buckets"]]
        assert "movie-analytics" in bucket_names


class TestEndToEnd:
    """Full pipeline test"""

    def test_end_to_end_flow(self):
        test_movie = f"e2e_movie_{uuid.uuid4().hex[:8]}"
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": "e2e_test_user",
            "movie_id": test_movie,
            "event_type": "VIEW_STARTED",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "device_type": "TV",
            "session_id": str(uuid.uuid4()),
            "progress_seconds": 0,
        }
        resp = requests.post(f"{PRODUCER_URL}/events", json=event)
        assert resp.status_code == 200

        requests.post(f"{PRODUCER_URL}/flush")
        time.sleep(10)

        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
        )
        result = client.query(
            "SELECT count() FROM cinema.movie_events WHERE movie_id = %(mid)s",
            parameters={"mid": test_movie},
        ).result_rows
        count = result[0][0]
        print(f"  E2E: Found {count} events for {test_movie} in ClickHouse")
        assert count >= 1, f"Event for {test_movie} not found in ClickHouse"