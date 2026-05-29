"""Unit-тесты чистой логики построения событий (без Kafka/Cassandra)."""
import pytest

import models


def test_product_received_builds_v2_value_with_supplier():
    built = models.build_product_event(
        "PRODUCT_RECEIVED",
        {"product_id": "SKU-1", "zone_id": "ZONE-A", "quantity": 10, "supplier_id": "SUP-1"},
        ts_ms=1000,
    )
    assert built.record_full == "warehouse.events.v1.ProductReceived"
    assert built.schema_file == "product_received_v2.avsc"
    assert built.key == "SKU-1"
    assert built.value["event_type"] == "PRODUCT_RECEIVED"
    assert built.value["quantity"] == 10
    assert built.value["supplier_id"] == "SUP-1"
    assert built.value["timestamp"] == 1000
    assert "event_id" in built.value


def test_product_received_defaults_supplier_to_none():
    built = models.build_product_event(
        "PRODUCT_RECEIVED", {"product_id": "SKU-1", "zone_id": "Z", "quantity": 5}
    )
    assert built.value["supplier_id"] is None


def test_negative_quantity_rejected():
    with pytest.raises(Exception):
        models.build_product_event("PRODUCT_SHIPPED", {"product_id": "S", "zone_id": "Z", "quantity": -5})


def test_zero_quantity_rejected():
    with pytest.raises(Exception):
        models.build_product_event("PRODUCT_SHIPPED", {"product_id": "S", "zone_id": "Z", "quantity": 0})


def test_missing_field_rejected():
    with pytest.raises(Exception):
        models.build_product_event("PRODUCT_SHIPPED", {"product_id": "S", "quantity": 1})


def test_unknown_event_type_rejected():
    with pytest.raises(ValueError):
        models.build_product_event("NONSENSE", {"product_id": "S", "zone_id": "Z", "quantity": 1})


def test_move_same_zone_rejected():
    with pytest.raises(Exception):
        models.build_move_event(
            {"product_id": "S", "from_zone_id": "Z", "to_zone_id": "Z", "quantity": 1}
        )


def test_move_builds_correct_value():
    built = models.build_move_event(
        {"product_id": "S", "from_zone_id": "A", "to_zone_id": "B", "quantity": 7}, ts_ms=42
    )
    assert built.value["from_zone_id"] == "A"
    assert built.value["to_zone_id"] == "B"
    assert built.value["quantity"] == 7
    assert built.key == "S"


def test_inventory_counted_allows_zero():
    built = models.build_inventory_count_event(
        {"product_id": "S", "zone_id": "Z", "counted_quantity": 0}
    )
    assert built.value["counted_quantity"] == 0
    assert built.value["event_type"] == "INVENTORY_COUNTED"


def test_order_created_generates_id_and_serializes_items():
    built = models.build_order_created_event(
        {"items": [{"product_id": "S1", "zone_id": "A", "quantity": 2}]}
    )
    assert built.value["order_id"].startswith("ORD-")
    assert built.key == built.value["order_id"]
    assert built.value["items"][0]["quantity"] == 2


def test_order_created_empty_items_rejected():
    with pytest.raises(Exception):
        models.build_order_created_event({"items": []})


def test_order_completed_requires_id():
    with pytest.raises(ValueError):
        models.build_order_completed_event("")
    built = models.build_order_completed_event("ORD-1")
    assert built.value["order_id"] == "ORD-1"
