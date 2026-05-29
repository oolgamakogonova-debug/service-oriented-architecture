"""Интеграционные тесты: запрос проходит через API-сервис -> Kafka ->
consumer -> Cassandra. Проверяется именно взаимодействие сервисов, а не один
сервис в изоляции.
"""
import requests

from itest_helpers import poll_zone


def test_received_then_reserved_flow(api_url, cass_session, wait_for_api, product_id, cleanup):
    zone = "ZONE-A"
    cleanup.append((product_id, zone))

    # 1) Приёмка 100 шт через HTTP API
    r = requests.post(
        f"{api_url}/api/v1/events/product-received",
        json={"product_id": product_id, "zone_id": zone, "quantity": 100},
        timeout=5,
    )
    assert r.status_code == 202

    row = poll_zone(cass_session, product_id, zone)
    assert row is not None, "остаток не появился в Cassandra"
    assert row.available_quantity == 100

    # 2) Резерв 30 шт -> available=70, reserved=30
    r = requests.post(
        f"{api_url}/api/v1/events/product-reserved",
        json={"product_id": product_id, "zone_id": zone, "quantity": 30},
        timeout=5,
    )
    assert r.status_code == 202

    deadline_row = None
    import time

    for _ in range(60):
        deadline_row = cass_session.execute(
            "SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s",
            (product_id, zone),
        ).one()
        if deadline_row and deadline_row.reserved_quantity == 30:
            break
        time.sleep(0.5)

    assert deadline_row is not None
    assert deadline_row.available_quantity == 70
    assert deadline_row.reserved_quantity == 30


def test_read_path_returns_same_state(api_url, cass_session, wait_for_api, product_id, cleanup):
    zone = "ZONE-C"
    cleanup.append((product_id, zone))

    requests.post(
        f"{api_url}/api/v1/events/product-received",
        json={"product_id": product_id, "zone_id": zone, "quantity": 55},
        timeout=5,
    )
    assert poll_zone(cass_session, product_id, zone) is not None

    # read-path API должен вернуть то же состояние, что записал consumer
    import time

    seen = None
    for _ in range(40):
        resp = requests.get(f"{api_url}/api/v1/inventory/{product_id}", timeout=5)
        assert resp.status_code == 200
        zones = resp.json()["zones"]
        match = [z for z in zones if z["zone_id"] == zone]
        if match and match[0]["available_quantity"] == 55:
            seen = match[0]
            break
        time.sleep(0.5)
    assert seen is not None, "read-path не вернул ожидаемый остаток"


def test_inventory_consistent_across_three_tables(api_url, cass_session, wait_for_api, product_id, cleanup):
    """После одного события все 3 inventory-таблицы должны совпадать (атомарный BATCH)."""
    zone = "ZONE-B"
    cleanup.append((product_id, zone))

    requests.post(
        f"{api_url}/api/v1/events/product-received",
        json={"product_id": product_id, "zone_id": zone, "quantity": 77},
        timeout=5,
    )
    assert poll_zone(cass_session, product_id, zone) is not None

    by_pz = cass_session.execute(
        "SELECT available_quantity FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        (product_id, zone),
    ).one()
    by_p = cass_session.execute(
        "SELECT available_quantity FROM inventory_by_product WHERE product_id=%s AND zone_id=%s",
        (product_id, zone),
    ).one()
    by_z = cass_session.execute(
        "SELECT available_quantity FROM inventory_by_zone WHERE zone_id=%s AND product_id=%s",
        (zone, product_id),
    ).one()

    assert by_pz.available_quantity == by_p.available_quantity == by_z.available_quantity == 77
