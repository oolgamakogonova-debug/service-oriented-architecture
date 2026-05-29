from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import Any, Callable

import structlog

from cassandra_client import CassandraClient
from metrics import events_skipped_total

log = structlog.get_logger(__name__)


# ──────────────────────────────────────────────────────────────────
class ValidationError(Exception):
    """Событие невалидно, например отрицательное quantity."""
    code = "VALIDATION_ERROR"


class HandlerError(Exception):
    """Ошибка бизнес-логики, не приводящая к падению консьюмера."""
    code = "HANDLER_ERROR"


# ──────────────────────────────────────────────────────────────────
@dataclass
class ZoneState:
    available: int = 0
    reserved: int = 0
    last_ts: dt.datetime | None = None
    last_event_id: str | None = None
    supplier_id: str | None = None


def _read_zone(cass: CassandraClient, product_id: str, zone_id: str) -> ZoneState:
    row = cass.get_zone_state(product_id, zone_id)

    if row is None:
        return ZoneState()

    return ZoneState(
        available=int(row.available_quantity or 0),
        reserved=int(row.reserved_quantity or 0),
        last_ts=row.last_event_timestamp,
        last_event_id=row.last_event_id,
        supplier_id=row.supplier_id,
    )


def _is_stale(event_ts: dt.datetime, current_ts: dt.datetime | None) -> bool:
    if current_ts is None:
        return False
    return event_ts <= current_ts


def _validate_quantity(q: int) -> None:
    if q is None or q < 0:
        raise ValidationError(f"Invalid quantity: {q} (must be non-negative)")


# ──────────────────────────────────────────────────────────────────
def _zone_writes(
    cass: CassandraClient,
    *,
    product_id: str,
    zone_id: str,
    new_available: int,
    new_reserved: int,
    event_id: str,
    event_ts: dt.datetime,
    supplier_id: str | None = None,
) -> list[tuple]:
    return [
        (
            cass.stmt("upsert_zone"),
            (
                product_id,
                zone_id,
                new_available,
                new_reserved,
                event_id,
                event_ts,
                supplier_id,
            ),
        ),
        (
            cass.stmt("upsert_product"),
            (
                product_id,
                zone_id,
                new_available,
                new_reserved,
                event_ts,
            ),
        ),
        (
            cass.stmt("upsert_zone_idx"),
            (
                zone_id,
                product_id,
                new_available,
                new_reserved,
                event_ts,
            ),
        ),
    ]


def _history_write(cass: CassandraClient, ev: dict[str, Any]) -> tuple:
    ts: dt.datetime = ev["timestamp"]

    return (
        cass.stmt("insert_history"),
        (
            ts.date(),
            ts,
            ev["event_id"],
            ev["event_type"],
            ev.get("product_id"),
            ev.get("zone_id"),
            int(ev.get("quantity") or 0),
            json.dumps(_jsonable(ev), default=str, ensure_ascii=False),
        ),
    )


def _jsonable(d: dict[str, Any]) -> dict[str, Any]:
    out = {}

    for k, v in d.items():
        if isinstance(v, dt.datetime):
            out[k] = v.isoformat()
        elif isinstance(v, list):
            out[k] = [_jsonable(x) if isinstance(x, dict) else x for x in v]
        elif isinstance(v, dict):
            out[k] = _jsonable(v)
        else:
            out[k] = v

    return out


Handler = Callable[[CassandraClient, dict[str, Any], int, int], list[tuple] | None]


# ──────────────────────────────────────────────────────────────────
def handle_product_received(cass, ev, partition, offset):
    _validate_quantity(int(ev["quantity"]))

    pid = ev["product_id"]
    zid = ev["zone_id"]
    ts = ev["timestamp"]

    state = _read_zone(cass, pid, zid)

    if _is_stale(ts, state.last_ts):
        events_skipped_total.labels(reason="out_of_order").inc()
        log.info(
            "event_out_of_order_skipped",
            event_id=ev["event_id"],
            event_type=ev["event_type"],
            event_ts=ts.isoformat(),
            last_ts=state.last_ts.isoformat() if state.last_ts else None,
        )
        return None

    new_available = state.available + int(ev["quantity"])
    supplier = ev.get("supplier_id") if ev.get("supplier_id") is not None else state.supplier_id

    return [
        *_zone_writes(
            cass,
            product_id=pid,
            zone_id=zid,
            new_available=new_available,
            new_reserved=state.reserved,
            event_id=ev["event_id"],
            event_ts=ts,
            supplier_id=supplier,
        ),
        _history_write(cass, ev),
    ]


def handle_product_shipped(cass, ev, partition, offset):
    _validate_quantity(int(ev["quantity"]))

    pid = ev["product_id"]
    zid = ev["zone_id"]
    ts = ev["timestamp"]

    state = _read_zone(cass, pid, zid)

    if _is_stale(ts, state.last_ts):
        events_skipped_total.labels(reason="out_of_order").inc()
        return None

    qty = int(ev["quantity"])

    if state.available < qty:
        raise ValidationError(
            f"Insufficient available quantity in zone {zid} for product {pid}: "
            f"have {state.available}, need {qty}"
        )

    new_available = state.available - qty

    return [
        *_zone_writes(
            cass,
            product_id=pid,
            zone_id=zid,
            new_available=new_available,
            new_reserved=state.reserved,
            event_id=ev["event_id"],
            event_ts=ts,
            supplier_id=state.supplier_id,
        ),
        _history_write(cass, ev),
    ]


def handle_product_moved(cass, ev, partition, offset):
    _validate_quantity(int(ev["quantity"]))

    pid = ev["product_id"]
    from_zone = ev["from_zone_id"]
    to_zone = ev["to_zone_id"]
    qty = int(ev["quantity"])
    ts = ev["timestamp"]

    if from_zone == to_zone:
        raise ValidationError("from_zone_id == to_zone_id")

    src = _read_zone(cass, pid, from_zone)
    dst = _read_zone(cass, pid, to_zone)

    if _is_stale(ts, src.last_ts) or _is_stale(ts, dst.last_ts):
        events_skipped_total.labels(reason="out_of_order").inc()
        return None

    if src.available < qty:
        raise ValidationError(
            f"Insufficient available in {from_zone} for {pid}: "
            f"have {src.available}, need {qty}"
        )

    new_src_available = src.available - qty
    new_dst_available = dst.available + qty

    return [
        *_zone_writes(
            cass,
            product_id=pid,
            zone_id=from_zone,
            new_available=new_src_available,
            new_reserved=src.reserved,
            event_id=ev["event_id"],
            event_ts=ts,
            supplier_id=src.supplier_id,
        ),
        *_zone_writes(
            cass,
            product_id=pid,
            zone_id=to_zone,
            new_available=new_dst_available,
            new_reserved=dst.reserved,
            event_id=ev["event_id"],
            event_ts=ts,
            supplier_id=dst.supplier_id,
        ),
        _history_write(cass, {**ev, "zone_id": from_zone}),
    ]


def handle_product_reserved(cass, ev, partition, offset):
    _validate_quantity(int(ev["quantity"]))

    pid = ev["product_id"]
    zid = ev["zone_id"]
    ts = ev["timestamp"]
    qty = int(ev["quantity"])

    state = _read_zone(cass, pid, zid)

    if _is_stale(ts, state.last_ts):
        events_skipped_total.labels(reason="out_of_order").inc()
        return None

    if state.available < qty:
        raise ValidationError(
            f"Insufficient available to reserve in zone {zid} for {pid}: "
            f"have {state.available}, need {qty}"
        )

    new_available = state.available - qty
    new_reserved = state.reserved + qty

    return [
        *_zone_writes(
            cass,
            product_id=pid,
            zone_id=zid,
            new_available=new_available,
            new_reserved=new_reserved,
            event_id=ev["event_id"],
            event_ts=ts,
            supplier_id=state.supplier_id,
        ),
        _history_write(cass, ev),
    ]


def handle_product_released(cass, ev, partition, offset):
    _validate_quantity(int(ev["quantity"]))

    pid = ev["product_id"]
    zid = ev["zone_id"]
    ts = ev["timestamp"]
    qty = int(ev["quantity"])

    state = _read_zone(cass, pid, zid)

    if _is_stale(ts, state.last_ts):
        events_skipped_total.labels(reason="out_of_order").inc()
        return None

    if state.reserved < qty:
        raise ValidationError(
            f"Insufficient reserved to release in zone {zid} for {pid}: "
            f"have {state.reserved}, need {qty}"
        )

    new_reserved = state.reserved - qty
    new_available = state.available + qty

    return [
        *_zone_writes(
            cass,
            product_id=pid,
            zone_id=zid,
            new_available=new_available,
            new_reserved=new_reserved,
            event_id=ev["event_id"],
            event_ts=ts,
            supplier_id=state.supplier_id,
        ),
        _history_write(cass, ev),
    ]


def handle_inventory_counted(cass, ev, partition, offset):
    pid = ev["product_id"]
    zid = ev["zone_id"]
    ts = ev["timestamp"]
    counted = int(ev["counted_quantity"])

    if counted < 0:
        raise ValidationError(f"counted_quantity is negative: {counted}")

    state = _read_zone(cass, pid, zid)

    if _is_stale(ts, state.last_ts):
        events_skipped_total.labels(reason="out_of_order").inc()
        return None

    return [
        *_zone_writes(
            cass,
            product_id=pid,
            zone_id=zid,
            new_available=counted,
            new_reserved=state.reserved,
            event_id=ev["event_id"],
            event_ts=ts,
            supplier_id=state.supplier_id,
        ),
        _history_write(cass, {**ev, "quantity": counted}),
    ]


def handle_order_created(cass, ev, partition, offset):
    """
    ORDER_CREATED — создаёт заказ + резервирует товары по позициям.
    Семантика как у PRODUCT_RESERVED для каждой позиции.
    """
    order_id = ev["order_id"]
    items = ev["items"]
    ts = ev["timestamp"]

    if not items:
        raise ValidationError("Order has no items")

    ops: list[tuple] = []
    serialized_items: list[dict] = []
    seen: dict[tuple[str, str], ZoneState] = {}

    for it in items:
        pid = it["product_id"]
        zid = it["zone_id"]
        qty = int(it["quantity"])

        _validate_quantity(qty)

        key = (pid, zid)

        if key not in seen:
            seen[key] = _read_zone(cass, pid, zid)

        s = seen[key]

        if _is_stale(ts, s.last_ts):
            events_skipped_total.labels(reason="out_of_order").inc()
            return None

        if s.available < qty:
            raise ValidationError(
                f"Insufficient available for order item: pid={pid}, zone={zid}, "
                f"have {s.available}, need {qty}"
            )

        s.reserved += qty

        serialized_items.append(
            {
                "product_id": pid,
                "zone_id": zid,
                "quantity": str(qty),
            }
        )

    for (pid, zid), s in seen.items():
        ops.extend(
            _zone_writes(
                cass,
                product_id=pid,
                zone_id=zid,
                new_available=s.available,
                new_reserved=s.reserved,
                event_id=ev["event_id"],
                event_ts=ts,
                supplier_id=s.supplier_id,
            )
        )

    ops.append(
        (
            cass.stmt("upsert_order"),
            (order_id, "CREATED", serialized_items, ts, ts),
        )
    )

    ops.append(
        _history_write(
            cass,
            {
                **ev,
                "product_id": None,
                "zone_id": None,
                "quantity": 0,
            },
        )
    )

    return ops


def handle_order_completed(cass, ev, partition, offset):
    """
    ORDER_COMPLETED — отгрузка зарезервированных товаров.

    reserved -= quantity для каждой позиции.
    available не меняется, потому что товар уже был вычтен из available
    при резервировании.
    """
    order_id = ev["order_id"]
    ts = ev["timestamp"]

    order = cass.get_order(order_id)

    if order is None:
        raise ValidationError(f"Order not found: {order_id}")

    if order.status == "COMPLETED":
        return [
            _history_write(
                cass,
                {
                    **ev,
                    "product_id": None,
                    "zone_id": None,
                    "quantity": 0,
                },
            )
        ]

    ops: list[tuple] = []
    seen: dict[tuple[str, str], ZoneState] = {}

    for it in order.items:
        pid = it["product_id"]
        zid = it["zone_id"]
        qty = int(it["quantity"])

        key = (pid, zid)

        if key not in seen:
            seen[key] = _read_zone(cass, pid, zid)

        s = seen[key]

        if _is_stale(ts, s.last_ts):
            events_skipped_total.labels(reason="out_of_order").inc()
            return None

        if s.reserved < qty:
            raise ValidationError(
                f"Order completion: insufficient reserved for pid={pid}, zone={zid}, "
                f"have {s.reserved}, need {qty}"
            )

        s.reserved -= qty

    for (pid, zid), s in seen.items():
        ops.extend(
            _zone_writes(
                cass,
                product_id=pid,
                zone_id=zid,
                new_available=s.available,
                new_reserved=s.reserved,
                event_id=ev["event_id"],
                event_ts=ts,
                supplier_id=s.supplier_id,
            )
        )

    ops.append((cass.stmt("update_order_status"), ("COMPLETED", ts, order_id)))

    ops.append(
        _history_write(
            cass,
            {
                **ev,
                "product_id": None,
                "zone_id": None,
                "quantity": 0,
            },
        )
    )

    return ops


HANDLERS: dict[str, Handler] = {
    "PRODUCT_RECEIVED": handle_product_received,
    "PRODUCT_SHIPPED": handle_product_shipped,
    "PRODUCT_MOVED": handle_product_moved,
    "PRODUCT_RESERVED": handle_product_reserved,
    "PRODUCT_RELEASED": handle_product_released,
    "INVENTORY_COUNTED": handle_inventory_counted,
    "ORDER_CREATED": handle_order_created,
    "ORDER_COMPLETED": handle_order_completed,
}