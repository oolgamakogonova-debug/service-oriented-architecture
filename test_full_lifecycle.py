"""End-to-End тест: полный пользовательский сценарий склада через публичный API.

Receive -> Order(create=reserve) -> Order complete -> Ship -> проверка
сквозного состояния в Cassandra. Проверяются HTTP-статусы, тело ответа и
финальное состояние в БД.
"""
import time
import uuid

import requests


def _wait(cond, *, timeout=40, interval=0.5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if cond():
                return True
        except Exception:  # noqa: BLE001
            pass
        time.sleep(interval)
    return False


def test_full_lifecycle_through_api(api_url, cass_session, wait_for_api, cleanup):
    pid = f"E2E-{uuid.uuid4().hex[:10]}"
    zone = "ZONE-A"
    cleanup.append((pid, zone))

    # 1) Приёмка 200 шт
    r = requests.post(
        f"{api_url}/api/v1/events/product-received",
        json={"product_id": pid, "zone_id": zone, "quantity": 200},
        timeout=5,
    )
    assert r.status_code == 202
    assert r.json()["accepted"] is True

    assert _wait(lambda: _avail(cass_session, pid, zone) == 200), "приёмка не отразилась"

    # 2) Заказ на 50 шт (резервирование)
    r = requests.post(
        f"{api_url}/api/v1/orders",
        json={"items": [{"product_id": pid, "zone_id": zone, "quantity": 50}]},
        timeout=5,
    )
    assert r.status_code == 202
    order_id = r.json()["key"]
    assert order_id

    assert _wait(lambda: _reserved(cass_session, pid, zone) == 50), "резерв заказа не отразился"
    assert _avail(cass_session, pid, zone) == 150

    # 3) Завершение заказа (reserved -= 50)
    r = requests.post(f"{api_url}/api/v1/orders/{order_id}/complete", timeout=5)
    assert r.status_code == 202

    assert _wait(lambda: _reserved(cass_session, pid, zone) == 0), "заказ не завершился"

    # 4) Отгрузка 100 из доступных 150
    r = requests.post(
        f"{api_url}/api/v1/events/product-shipped",
        json={"product_id": pid, "zone_id": zone, "quantity": 100},
        timeout=5,
    )
    assert r.status_code == 202

    assert _wait(lambda: _avail(cass_session, pid, zone) == 50), "отгрузка не отразилась"

    # 5) Финальная сверка через read-path API
    resp = requests.get(f"{api_url}/api/v1/inventory/{pid}/{zone}", timeout=5)
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body["available_quantity"], int)
    assert body["available_quantity"] == 50
    assert body["reserved_quantity"] == 0


def _avail(session, pid, zone):
    row = session.execute(
        "SELECT available_quantity FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        (pid, zone),
    ).one()
    return None if row is None else row.available_quantity


def _reserved(session, pid, zone):
    row = session.execute(
        "SELECT reserved_quantity FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        (pid, zone),
    ).one()
    return None if row is None else row.reserved_quantity
