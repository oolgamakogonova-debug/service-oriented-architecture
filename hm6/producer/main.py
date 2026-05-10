from __future__ import annotations
import datetime as dt
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import requests
import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer


# ── Конфиг ──────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SR_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = os.environ.get("OUTPUT_TOPIC", "warehouse-events")
MODE = os.environ.get("MODE", "demo").lower()
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
SCHEMAS_DIR = Path(os.environ.get("SCHEMAS_DIR", "/app/schemas"))

logging.basicConfig(level=LOG_LEVEL, format="%(message)s", stream=sys.stdout)
structlog.configure(
    processors=[structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()],
)
log = structlog.get_logger("producer")


# ── Регистрация схем ────────────────────────────────────────────
# Subject naming: TopicRecordNameStrategy → "<topic>-<record_fullname>".
# Так разные record-типы могут жить в одном топике, у каждого своя схема.
SCHEMA_FILES = {
    # subject_suffix (полное имя record): файл схемы
    "warehouse.events.v1.ProductReceived":  ("product_received_v1.avsc", "product_received_v2.avsc"),
    "warehouse.events.v1.ProductShipped":   ("product_shipped.avsc",),
    "warehouse.events.v1.ProductMoved":     ("product_moved.avsc",),
    "warehouse.events.v1.ProductReserved":  ("product_reserved.avsc",),
    "warehouse.events.v1.ProductReleased":  ("product_released.avsc",),
    "warehouse.events.v1.InventoryCounted": ("inventory_counted.avsc",),
    "warehouse.events.v1.OrderCreated":     ("order_created.avsc",),
    "warehouse.events.v1.OrderCompleted":   ("order_completed.avsc",),
}


def _set_compatibility(subject: str, level: str = "BACKWARD") -> None:
    url = f"{SR_URL}/config/{subject}"
    try:
        r = requests.put(url, json={"compatibility": level}, timeout=5)
        r.raise_for_status()
        log.info("compatibility_set", subject=subject, level=level)
    except Exception as e:
        log.warning("compatibility_set_failed", subject=subject, error=str(e))


def register_schemas(sr: SchemaRegistryClient) -> dict[str, dict[str, Any]]:
    """Регистрирует все схемы. Для PRODUCT_RECEIVED регистрирует V1, затем V2.

    Возвращает map record_fullname -> {"schema_id": ..., "schema_str_v_latest": ...}
    """
    out: dict[str, dict[str, Any]] = {}
    for record_full, files in SCHEMA_FILES.items():
        subject = f"{TOPIC}-{record_full}"
        # Compatibility должна быть выставлена ДО регистрации второй версии.
        _set_compatibility(subject, "BACKWARD")
        latest_str = None
        latest_id = None
        for f in files:
            schema_str = (SCHEMAS_DIR / f).read_text(encoding="utf-8")
            schema_obj = Schema(schema_str, schema_type="AVRO")
            schema_id = sr.register_schema(subject, schema_obj)
            log.info("schema_registered", subject=subject, file=f, schema_id=schema_id)
            latest_str = schema_str
            latest_id = schema_id
        out[record_full] = {"schema_id": latest_id, "schema_str": latest_str, "subject": subject}
    return out


# ── Сериализация ───────────────────────────────────────────────
def make_serializer(sr: SchemaRegistryClient, schema_str: str) -> AvroSerializer:
    return AvroSerializer(
        sr,
        schema_str=schema_str,
        # TopicRecordNameStrategy:
        conf={"subject.name.strategy": _topic_record_name_strategy},
    )


def _topic_record_name_strategy(ctx: SerializationContext, record_name: str) -> str:
    """Confluent Python: subject.name.strategy callable получает record_name (str)."""
    return f"{ctx.topic}-{record_name}"


# ── Producer ────────────────────────────────────────────────────
def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 20,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "client.id": "wms-producer",
    })


def _now_ms() -> int:
    return int(dt.datetime.utcnow().timestamp() * 1000)


def _delivery_cb(err, msg):
    if err is not None:
        log.error("delivery_failed", error=str(err))
    else:
        log.info("delivered", topic=msg.topic(), partition=msg.partition(), offset=msg.offset())


def send(producer: Producer, sr: SchemaRegistryClient, schema_map: dict, *,
         record_full: str, value: dict[str, Any], key: str | None = None) -> None:
    info = schema_map[record_full]
    serializer = make_serializer(sr, info["schema_str"])
    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    payload = serializer(value, ctx)
    producer.produce(
        TOPIC,
        key=(key.encode() if key else None),
        value=payload,
        on_delivery=_delivery_cb,
    )
    producer.poll(0)


# ── Демо-сценарий ──────────────────────────────────────────────
def run_demo(producer: Producer, sr: SchemaRegistryClient, schema_map: dict) -> None:
    log.info("demo_start")
    # Сценарий 1: базовый цикл склада (SKU-001).
    pid = "SKU-001"
    send(producer, sr, schema_map,
         record_full="warehouse.events.v1.ProductReceived",
         value={"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_RECEIVED",
                "timestamp": _now_ms(), "product_id": pid, "zone_id": "ZONE-A",
                "quantity": 100, "supplier_id": None},
         key=pid)
    time.sleep(1.5)
    send(producer, sr, schema_map,
         record_full="warehouse.events.v1.ProductReserved",
         value={"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_RESERVED",
                "timestamp": _now_ms(), "product_id": pid, "zone_id": "ZONE-A",
                "quantity": 30, "order_id": None},
         key=pid)
    time.sleep(1.0)
    send(producer, sr, schema_map,
         record_full="warehouse.events.v1.ProductMoved",
         value={"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_MOVED",
                "timestamp": _now_ms(), "product_id": pid,
                "from_zone_id": "ZONE-A", "to_zone_id": "ZONE-B", "quantity": 20},
         key=pid)
    time.sleep(1.0)
    send(producer, sr, schema_map,
         record_full="warehouse.events.v1.ProductShipped",
         value={"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_SHIPPED",
                "timestamp": _now_ms(), "product_id": pid, "zone_id": "ZONE-A",
                "quantity": 10},
         key=pid)
    producer.flush(10)
    log.info("demo_complete")


# ── main ────────────────────────────────────────────────────────
def main() -> None:
    log.info("producer_starting", mode=MODE, topic=TOPIC, sr=SR_URL)

    sr = SchemaRegistryClient({"url": SR_URL})

    # Ждём поднятия Schema Registry
    for _ in range(60):
        try:
            sr.get_subjects()
            break
        except Exception:
            time.sleep(2)

    schema_map = register_schemas(sr)
    producer = _make_producer()

    if MODE == "demo":
        run_demo(producer, sr, schema_map)
    elif MODE == "idle":
        log.info("producer_idle_waiting_for_external_use")
        # Бесконечно ждём — контейнер живой, можно отдельно вызывать send_events.py
        while True:
            time.sleep(60)
    else:
        log.warning("unknown_mode", mode=MODE)


if __name__ == "__main__":
    main()
