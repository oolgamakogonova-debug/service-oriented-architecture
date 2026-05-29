from __future__ import annotations
import datetime as dt
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

import structlog

log = structlog.get_logger(__name__)


class EventDeserializer:
    """Универсальный Avro-десериализатор для warehouse-events.

    Использует AvroDeserializer без явной schema-string: берёт схему по
    schema_id из сообщения (writer schema) и не требует reader schema —
    confluent_kafka применит её как есть.
    """

    def __init__(self, schema_registry_url: str) -> None:
        self.sr = SchemaRegistryClient({"url": schema_registry_url})
        self.deser = AvroDeserializer(self.sr)

    def deserialize(self, raw: bytes, topic: str) -> dict[str, Any]:
        ctx = SerializationContext(topic, MessageField.VALUE)
        obj = self.deser(raw, ctx)
        if obj is None:
            raise ValueError("Failed to deserialize Avro payload (None result)")

        # Приведение к единому виду:
        ev = dict(obj)
        # Avro logicalType=timestamp-millis десериализуется в datetime.
        # Если timestamp — int, преобразуем.
        ts = ev.get("timestamp")
        if isinstance(ts, int):
            ev["timestamp"] = dt.datetime.utcfromtimestamp(ts / 1000.0)
        elif isinstance(ts, dt.datetime):
            # confluent-kafka возвращает aware datetime; приведём к naive UTC,
            # чтобы корректно сравнивать с тем, что лежит в Cassandra.
            if ts.tzinfo is not None:
                ev["timestamp"] = ts.astimezone(dt.timezone.utc).replace(tzinfo=None)

        # supplier_id может отсутствовать (V1), либо быть Avro union ['null', 'string'].
        # Приведём к плоскому виду:
        if "supplier_id" in ev and isinstance(ev["supplier_id"], dict):
            ev["supplier_id"] = ev["supplier_id"].get("string")
        ev.setdefault("supplier_id", None)

        return ev
