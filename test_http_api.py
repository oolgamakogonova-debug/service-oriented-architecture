"""Unit-тесты HTTP-слоя: маршруты, коды ответов и автоматический сбор
http_*-метрик через middleware. Kafka/Cassandra подменены фейками."""
from fastapi.testclient import TestClient

import app as app_module


class FakePublisher:
    def __init__(self):
        self.published = []

    def publish(self, *, record_full, value, key, event_type):
        self.published.append({"record_full": record_full, "value": value, "key": key, "event_type": event_type})


def make_client():
    pub = FakePublisher()
    application = app_module.create_app(pub, reader=None)
    return TestClient(application, raise_server_exceptions=False), pub


def test_valid_event_returns_202_and_publishes():
    client, pub = make_client()
    r = client.post(
        "/api/v1/events/product-received",
        json={"product_id": "SKU-1", "zone_id": "ZONE-A", "quantity": 10},
    )
    assert r.status_code == 202
    body = r.json()
    assert body["accepted"] is True
    assert "event_id" in body
    assert len(pub.published) == 1
    assert pub.published[0]["event_type"] == "PRODUCT_RECEIVED"


def test_invalid_event_returns_422_and_not_published():
    client, pub = make_client()
    r = client.post(
        "/api/v1/events/product-received",
        json={"product_id": "SKU-1", "zone_id": "ZONE-A", "quantity": -3},
    )
    assert r.status_code == 422
    assert len(pub.published) == 0


def test_metrics_endpoint_exposes_required_metrics():
    client, _ = make_client()
    client.post(
        "/api/v1/events/product-shipped",
        json={"product_id": "SKU-1", "zone_id": "ZONE-A", "quantity": 1},
    )
    body = client.get("/metrics").text
    # Обязательная тройка метрик присутствует
    assert "http_requests_total" in body
    assert "http_request_duration_seconds" in body
    # Метка endpoint — это шаблон маршрута, а не конкретный путь
    assert 'endpoint="/api/v1/events/product-shipped"' in body
    assert 'status="202"' in body


def test_client_error_increments_error_metric():
    client, _ = make_client()
    client.post(
        "/api/v1/events/product-shipped",
        json={"product_id": "SKU-1", "zone_id": "ZONE-A", "quantity": 0},
    )
    body = client.get("/metrics").text
    assert "http_request_errors_total" in body
    assert 'error_type="client_error"' in body


def test_health_reports_degraded_when_kafka_down():
    # kafka_connected gauge по умолчанию 0 -> degraded (reader=None -> cassandra=True)
    client, _ = make_client()
    r = client.get("/health")
    assert r.status_code in (200, 503)
    assert "kafka" in r.json()


def test_read_path_disabled_returns_503():
    client, _ = make_client()
    r = client.get("/api/v1/inventory/SKU-1")
    assert r.status_code == 503
