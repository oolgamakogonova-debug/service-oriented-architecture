"""Публикация warehouse-событий в Kafka через Confluent Schema Registry.

Subject naming: TopicRecordNameStrategy -> "<topic>-<record_fullname>".
Так разные record-типы живут в одном топике, у каждого своя схема.
"""
from __future__ import annotations

import threading
from typing import Any

import requests
import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from config import cfg
from metrics import events_published_total, kafka_connected

log = structlog.get_logger("publisher")

# record_fullname -> кортеж файлов схем (для PRODUCT_RECEIVED: V1, затем V2)
SCHEMA_FILES: dict[str, tuple[str, ...]] = {
    "warehouse.events.v1.ProductReceived":  ("product_received_v1.avsc", "product_received_v2.avsc"),
    "warehouse.events.v1.ProductShipped":   ("product_shipped.avsc",),
    "warehouse.events.v1.ProductMoved":     ("product_moved.avsc",),
    "warehouse.events.v1.ProductReserved":  ("product_reserved.avsc",),
    "warehouse.events.v1.ProductReleased":  ("product_released.avsc",),
    "warehouse.events.v1.InventoryCounted": ("inventory_counted.avsc",),
    "warehouse.events.v1.OrderCreated":     ("order_created.avsc",),
    "warehouse.events.v1.OrderCompleted":   ("order_completed.avsc",),
}


def _topic_record_name_strategy(ctx: SerializationContext, record_name: str) -> str:
    return f"{ctx.topic}-{record_name}"


class EventPublisher:
    def __init__(self) -> None:
        self.sr = SchemaRegistryClient({"url": cfg.SCHEMA_REGISTRY_URL})
        self.producer = Producer(
            {
                "bootstrap.servers": cfg.KAFKA_BOOTSTRAP_SERVERS,
                "linger.ms": 10,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "lz4",
                "client.id": "wms-api",
            }
        )
        self._schema_str: dict[str, str] = {}
        self._serializers: dict[str, AvroSerializer] = {}
        self._lock = threading.Lock()

    # ── compatibility + регистрация ────────────────────────────────
    def _set_compatibility(self, subject: str, level: str = "BACKWARD") -> None:
        url = f"{cfg.SCHEMA_REGISTRY_URL}/config/{subject}"
        try:
            r = requests.put(url, json={"compatibility": level}, timeout=5)
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001
            log.warning("compatibility_set_failed", subject=subject, error=str(e))

    def register_schemas(self) -> None:
        for record_full, files in SCHEMA_FILES.items():
            subject = f"{cfg.OUTPUT_TOPIC}-{record_full}"
            self._set_compatibility(subject, "BACKWARD")
            latest_str = None
            for f in files:
                schema_str = (cfg.SCHEMAS_DIR / f).read_text(encoding="utf-8")
                self.sr.register_schema(subject, Schema(schema_str, schema_type="AVRO"))
                latest_str = schema_str
                log.info("schema_registered", subject=subject, file=f)
            if latest_str is not None:
                self._schema_str[record_full] = latest_str
        kafka_connected.set(1)

    def _serializer(self, record_full: str) -> AvroSerializer:
        if record_full not in self._serializers:
            self._serializers[record_full] = AvroSerializer(
                self.sr,
                schema_str=self._schema_str[record_full],
                conf={"subject.name.strategy": _topic_record_name_strategy},
            )
        return self._serializers[record_full]

    # ── публикация ─────────────────────────────────────────────────
    def publish(self, *, record_full: str, value: dict[str, Any], key: str, event_type: str) -> None:
        serializer = self._serializer(record_full)
        ctx = SerializationContext(cfg.OUTPUT_TOPIC, MessageField.VALUE)
        payload = serializer(value, ctx)
        with self._lock:
            self.producer.produce(cfg.OUTPUT_TOPIC, key=key.encode(), value=payload)
            self.producer.poll(0)
        events_published_total.labels(event_type=event_type).inc()

    def flush(self, timeout: float = 5.0) -> None:
        self.producer.flush(timeout)

    def healthy(self) -> bool:
        try:
            # Лёгкая проверка: список метаданных кластера.
            self.producer.list_topics(timeout=3.0)
            kafka_connected.set(1)
            return True
        except Exception:  # noqa: BLE001
            kafka_connected.set(0)
            return False
