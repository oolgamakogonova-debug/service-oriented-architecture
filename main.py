"""Точка входа WMS-API сервиса.

MODE=api  (по умолчанию) — поднимает HTTP-сервер (FastAPI/uvicorn).
MODE=demo — регистрирует схемы, прогоняет короткий демо-сценарий и выходит
            (используется как «warm-up», чтобы в Grafana сразу были данные).
"""
from __future__ import annotations

import logging
import sys
import time

import structlog
import uvicorn

from config import cfg

logging.basicConfig(level=cfg.LOG_LEVEL, format="%(message)s", stream=sys.stdout)
structlog.configure(
    processors=[structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()],
)
log = structlog.get_logger("wms-api")


def _wait_schema_registry(publisher) -> None:
    for _ in range(60):
        try:
            publisher.sr.get_subjects()
            return
        except Exception:  # noqa: BLE001
            time.sleep(2)
    log.warning("schema_registry_not_ready")


def _build_publisher():
    from kafka_publisher import EventPublisher

    pub = EventPublisher()
    _wait_schema_registry(pub)
    pub.register_schemas()
    log.info("schemas_registered")
    return pub


def _build_reader():
    from cassandra_reader import InventoryReader

    reader = InventoryReader()
    for attempt in range(30):
        try:
            reader.connect()
            return reader
        except Exception as e:  # noqa: BLE001
            log.warning("cassandra_connect_retry", attempt=attempt, error=str(e))
            time.sleep(3)
    log.error("cassandra_unavailable_read_path_disabled")
    return None


def run_demo(publisher) -> None:
    import models

    pid = "SKU-DEMO-001"
    for built in (
        models.build_product_event("PRODUCT_RECEIVED", {"product_id": pid, "zone_id": "ZONE-A", "quantity": 100}),
        models.build_product_event("PRODUCT_RESERVED", {"product_id": pid, "zone_id": "ZONE-A", "quantity": 30}),
        models.build_move_event({"product_id": pid, "from_zone_id": "ZONE-A", "to_zone_id": "ZONE-B", "quantity": 20}),
        models.build_product_event("PRODUCT_SHIPPED", {"product_id": pid, "zone_id": "ZONE-A", "quantity": 10}),
    ):
        publisher.publish(
            record_full=built.record_full,
            value=built.value,
            key=built.key,
            event_type=built.value["event_type"],
        )
        time.sleep(0.5)
    publisher.flush(10)
    log.info("demo_complete")


def main() -> None:
    log.info("wms_api_starting", mode=cfg.MODE, topic=cfg.OUTPUT_TOPIC)
    publisher = _build_publisher()

    if cfg.MODE == "demo":
        run_demo(publisher)
        return

    reader = _build_reader()

    from app import create_app

    app = create_app(publisher, reader)
    uvicorn.run(app, host="0.0.0.0", port=cfg.HTTP_PORT, log_level=cfg.LOG_LEVEL.lower(), access_log=False)


if __name__ == "__main__":
    main()
