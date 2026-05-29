"""Unit-тесты бизнес-логики handlers консьюмера.

Cassandra подменяется фейком: handler'ы — чистые функции, которые читают
состояние зоны и возвращают список statement'ов. Инфраструктура не нужна.
"""
import datetime as dt
import types

import pytest

import handlers
from handlers import (
    ValidationError,
    handle_product_received,
    handle_product_shipped,
    handle_product_reserved,
    handle_product_moved,
)


class FakeCass:
    """Имитация CassandraClient: stmt() возвращает имя, get_zone_state — заданную строку."""

    def __init__(self, zones=None, orders=None):
        # zones: {(product_id, zone_id): dict(available, reserved, last_ts, supplier_id)}
        self._zones = zones or {}
        self._orders = orders or {}

    def stmt(self, name):
        return name  # для тестов имя prepared-statement = сам statement

    def get_zone_state(self, product_id, zone_id):
        z = self._zones.get((product_id, zone_id))
        if z is None:
            return None
        return types.SimpleNamespace(
            available_quantity=z.get("available", 0),
            reserved_quantity=z.get("reserved", 0),
            last_event_timestamp=z.get("last_ts"),
            last_event_id=z.get("last_event_id"),
            supplier_id=z.get("supplier_id"),
        )

    def get_order(self, order_id):
        return self._orders.get(order_id)


def _ev(event_type, **kwargs):
    base = {
        "event_id": "ev-1",
        "event_type": event_type,
        "timestamp": dt.datetime(2026, 1, 1, 12, 0, 0),
    }
    base.update(kwargs)
    return base


def _upsert_zone_params(ops):
    """Достаёт параметры первого upsert_zone из списка ops."""
    for stmt, params in ops:
        if stmt == "upsert_zone":
            return params
    raise AssertionError("upsert_zone не найден в ops")


def test_received_into_empty_zone_sets_available():
    cass = FakeCass()
    ops = handle_product_received(
        cass, _ev("PRODUCT_RECEIVED", product_id="P", zone_id="Z", quantity=100, supplier_id=None), 0, 0
    )
    params = _upsert_zone_params(ops)
    # params: (product_id, zone_id, available, reserved, event_id, ts, supplier_id)
    assert params[0] == "P"
    assert params[1] == "Z"
    assert params[2] == 100  # available
    assert params[3] == 0    # reserved


def test_received_accumulates_existing_available():
    cass = FakeCass(zones={("P", "Z"): {"available": 40, "reserved": 5}})
    ops = handle_product_received(
        cass, _ev("PRODUCT_RECEIVED", product_id="P", zone_id="Z", quantity=10, supplier_id=None), 0, 0
    )
    params = _upsert_zone_params(ops)
    assert params[2] == 50  # 40 + 10
    assert params[3] == 5   # reserved не тронут


def test_shipped_insufficient_raises_validation():
    cass = FakeCass(zones={("P", "Z"): {"available": 5, "reserved": 0}})
    with pytest.raises(ValidationError):
        handle_product_shipped(
            cass, _ev("PRODUCT_SHIPPED", product_id="P", zone_id="Z", quantity=10), 0, 0
        )


def test_shipped_decrements_available():
    cass = FakeCass(zones={("P", "Z"): {"available": 30, "reserved": 0}})
    ops = handle_product_shipped(
        cass, _ev("PRODUCT_SHIPPED", product_id="P", zone_id="Z", quantity=10), 0, 0
    )
    assert _upsert_zone_params(ops)[2] == 20


def test_reserved_moves_available_to_reserved():
    cass = FakeCass(zones={("P", "Z"): {"available": 30, "reserved": 0}})
    ops = handle_product_reserved(
        cass, _ev("PRODUCT_RESERVED", product_id="P", zone_id="Z", quantity=12), 0, 0
    )
    params = _upsert_zone_params(ops)
    assert params[2] == 18  # available 30 - 12
    assert params[3] == 12  # reserved 0 + 12


def test_out_of_order_event_is_skipped():
    # last_ts в будущем относительно события -> stale -> None (пропуск)
    future = dt.datetime(2026, 6, 1, 0, 0, 0)
    cass = FakeCass(zones={("P", "Z"): {"available": 10, "reserved": 0, "last_ts": future}})
    result = handle_product_received(
        cass, _ev("PRODUCT_RECEIVED", product_id="P", zone_id="Z", quantity=5, supplier_id=None), 0, 0
    )
    assert result is None


def test_negative_quantity_raises_validation():
    cass = FakeCass()
    with pytest.raises(ValidationError):
        handle_product_received(
            cass, _ev("PRODUCT_RECEIVED", product_id="P", zone_id="Z", quantity=-1, supplier_id=None), 0, 0
        )


def test_move_insufficient_in_source_raises():
    cass = FakeCass(zones={("P", "A"): {"available": 3, "reserved": 0}})
    with pytest.raises(ValidationError):
        handle_product_moved(
            cass, _ev("PRODUCT_MOVED", product_id="P", from_zone_id="A", to_zone_id="B", quantity=5), 0, 0
        )


def test_move_transfers_between_zones():
    cass = FakeCass(zones={("P", "A"): {"available": 50, "reserved": 0}, ("P", "B"): {"available": 5, "reserved": 0}})
    ops = handle_product_moved(
        cass, _ev("PRODUCT_MOVED", product_id="P", from_zone_id="A", to_zone_id="B", quantity=20), 0, 0
    )
    # Первый upsert_zone — для исходной зоны A: 50 - 20 = 30
    assert _upsert_zone_params(ops)[2] == 30
