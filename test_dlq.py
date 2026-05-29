"""Интеграционный тест Dead Letter Queue.

Сценарий: PRODUCT_SHIPPED для свежего товара, которого нет на складе ->
consumer бросает ValidationError (insufficient available) -> событие уходит
в DLQ, при этом пайплайн не блокируется и последующее валидное событие
обрабатывается нормально.
"""
import json
import time
import uuid

import requests
from confluent_kafka import Consumer

from itest_helpers import poll_zone

from itest_config import KAFKA_BOOTSTRAP, DLQ_TOPIC


def _drain_dlq(timeout=40.0):
    """Возвращает список JSON-сообщений из DLQ за отведённое время."""
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"dlq-checker-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([DLQ_TOPIC])
    out = []
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                out.append(json.loads(msg.value().decode("utf-8")))
            except Exception:  # noqa: BLE001
                pass
    finally:
        consumer.close()
    return out


def test_invalid_event_goes_to_dlq_and_pipeline_survives(api_url, cass_session, wait_for_api, cleanup):
    bad_pid = f"DLQ-{uuid.uuid4().hex[:10]}"
    good_pid = f"OK-{uuid.uuid4().hex[:10]}"
    zone = "ZONE-A"
    cleanup.append((good_pid, zone))

    # 1) Отгрузка несуществующего остатка -> consumer -> DLQ (insufficient available)
    r = requests.post(
        f"{api_url}/api/v1/events/product-shipped",
        json={"product_id": bad_pid, "zone_id": zone, "quantity": 999},
        timeout=5,
    )
    assert r.status_code == 202  # API принял (бизнес-валидация — на стороне consumer)

    # 2) Сразу следом валидное событие — оно должно обработаться, несмотря на ошибку выше
    r = requests.post(
        f"{api_url}/api/v1/events/product-received",
        json={"product_id": good_pid, "zone_id": zone, "quantity": 10},
        timeout=5,
    )
    assert r.status_code == 202

    # Пайплайн жив: валидное событие доехало до Cassandra
    assert poll_zone(cass_session, good_pid, zone) is not None, "пайплайн заблокировался после ошибки"

    # Проблемное событие попало в DLQ с понятным error_code
    msgs = _drain_dlq()
    related = [
        m for m in msgs
        if isinstance(m.get("original_event"), dict)
        and m["original_event"].get("product_id") == bad_pid
    ]
    assert related, "ожидалось сообщение в DLQ для проблемного события"
    assert related[0]["error_code"] in ("VALIDATION_ERROR", "HANDLER_ERROR")
    assert "kafka_metadata" in related[0]
