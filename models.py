"""Модели запросов WMS-API и чистая логика построения Avro-событий.

`build_event` намеренно вынесена в отдельную чистую функцию (без FastAPI и Kafka),
чтобы её можно было покрыть unit-тестами без поднятия инфраструктуры.
"""
from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from pydantic import BaseModel, Field, field_validator


# ── Спецификация каждого типа события ──────────────────────────────
# event_type -> (record_fullname, файл схемы, поле для Kafka-ключа)
EVENT_SPECS: dict[str, tuple[str, str, str]] = {
    "PRODUCT_RECEIVED":  ("warehouse.events.v1.ProductReceived",  "product_received_v2.avsc", "product_id"),
    "PRODUCT_SHIPPED":   ("warehouse.events.v1.ProductShipped",   "product_shipped.avsc",     "product_id"),
    "PRODUCT_MOVED":     ("warehouse.events.v1.ProductMoved",     "product_moved.avsc",       "product_id"),
    "PRODUCT_RESERVED":  ("warehouse.events.v1.ProductReserved",  "product_reserved.avsc",    "product_id"),
    "PRODUCT_RELEASED":  ("warehouse.events.v1.ProductReleased",  "product_released.avsc",    "product_id"),
    "INVENTORY_COUNTED": ("warehouse.events.v1.InventoryCounted", "inventory_counted.avsc",   "product_id"),
    "ORDER_CREATED":     ("warehouse.events.v1.OrderCreated",     "order_created.avsc",       "order_id"),
    "ORDER_COMPLETED":   ("warehouse.events.v1.OrderCompleted",   "order_completed.avsc",     "order_id"),
}


def _now_ms() -> int:
    return int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)


# ── Pydantic-модели (валидируют форму JSON на входе) ───────────────
class ProductEvent(BaseModel):
    product_id: str = Field(min_length=1, max_length=128)
    zone_id: str = Field(min_length=1, max_length=128)
    quantity: int = Field(gt=0, description="Положительное целое")
    supplier_id: str | None = Field(default=None, max_length=128)
    order_id: str | None = Field(default=None, max_length=128)


class MoveEvent(BaseModel):
    product_id: str = Field(min_length=1, max_length=128)
    from_zone_id: str = Field(min_length=1, max_length=128)
    to_zone_id: str = Field(min_length=1, max_length=128)
    quantity: int = Field(gt=0)

    @field_validator("to_zone_id")
    @classmethod
    def zones_differ(cls, v: str, info):
        if v == info.data.get("from_zone_id"):
            raise ValueError("from_zone_id и to_zone_id должны различаться")
        return v


class InventoryCountEvent(BaseModel):
    product_id: str = Field(min_length=1, max_length=128)
    zone_id: str = Field(min_length=1, max_length=128)
    counted_quantity: int = Field(ge=0)


class OrderItem(BaseModel):
    product_id: str = Field(min_length=1, max_length=128)
    zone_id: str = Field(min_length=1, max_length=128)
    quantity: int = Field(gt=0)


class CreateOrder(BaseModel):
    order_id: str | None = Field(default=None, max_length=128)
    items: list[OrderItem] = Field(min_length=1)


# ── Построение Avro-значения ───────────────────────────────────────
class BuiltEvent(BaseModel):
    record_full: str
    schema_file: str
    key: str
    value: dict[str, Any]


def _spec(event_type: str) -> tuple[str, str, str]:
    if event_type not in EVENT_SPECS:
        raise ValueError(f"Неизвестный event_type: {event_type}")
    return EVENT_SPECS[event_type]


def build_product_event(event_type: str, payload: dict[str, Any], *, ts_ms: int | None = None) -> BuiltEvent:
    """PRODUCT_RECEIVED / SHIPPED / RESERVED / RELEASED."""
    model = ProductEvent(**payload)
    record_full, schema_file, _ = _spec(event_type)
    value: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": ts_ms if ts_ms is not None else _now_ms(),
        "product_id": model.product_id,
        "zone_id": model.zone_id,
        "quantity": model.quantity,
    }
    if event_type == "PRODUCT_RECEIVED":
        value["supplier_id"] = model.supplier_id
    if event_type in ("PRODUCT_RESERVED", "PRODUCT_RELEASED"):
        value["order_id"] = model.order_id
    return BuiltEvent(record_full=record_full, schema_file=schema_file, key=model.product_id, value=value)


def build_move_event(payload: dict[str, Any], *, ts_ms: int | None = None) -> BuiltEvent:
    model = MoveEvent(**payload)
    record_full, schema_file, _ = _spec("PRODUCT_MOVED")
    value = {
        "event_id": str(uuid.uuid4()),
        "event_type": "PRODUCT_MOVED",
        "timestamp": ts_ms if ts_ms is not None else _now_ms(),
        "product_id": model.product_id,
        "from_zone_id": model.from_zone_id,
        "to_zone_id": model.to_zone_id,
        "quantity": model.quantity,
    }
    return BuiltEvent(record_full=record_full, schema_file=schema_file, key=model.product_id, value=value)


def build_inventory_count_event(payload: dict[str, Any], *, ts_ms: int | None = None) -> BuiltEvent:
    model = InventoryCountEvent(**payload)
    record_full, schema_file, _ = _spec("INVENTORY_COUNTED")
    value = {
        "event_id": str(uuid.uuid4()),
        "event_type": "INVENTORY_COUNTED",
        "timestamp": ts_ms if ts_ms is not None else _now_ms(),
        "product_id": model.product_id,
        "zone_id": model.zone_id,
        "counted_quantity": model.counted_quantity,
    }
    return BuiltEvent(record_full=record_full, schema_file=schema_file, key=model.product_id, value=value)


def build_order_created_event(payload: dict[str, Any], *, ts_ms: int | None = None) -> BuiltEvent:
    model = CreateOrder(**payload)
    record_full, schema_file, _ = _spec("ORDER_CREATED")
    order_id = model.order_id or f"ORD-{uuid.uuid4().hex[:10]}"
    value = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "timestamp": ts_ms if ts_ms is not None else _now_ms(),
        "order_id": order_id,
        "items": [
            {"product_id": it.product_id, "zone_id": it.zone_id, "quantity": it.quantity}
            for it in model.items
        ],
    }
    return BuiltEvent(record_full=record_full, schema_file=schema_file, key=order_id, value=value)


def build_order_completed_event(order_id: str, *, ts_ms: int | None = None) -> BuiltEvent:
    if not order_id:
        raise ValueError("order_id обязателен")
    record_full, schema_file, _ = _spec("ORDER_COMPLETED")
    value = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_COMPLETED",
        "timestamp": ts_ms if ts_ms is not None else _now_ms(),
        "order_id": order_id,
    }
    return BuiltEvent(record_full=record_full, schema_file=schema_file, key=order_id, value=value)
