"""Хелперы для integration/e2e тестов (без тяжёлых импортов на верхнем уровне)."""
import time


def poll_zone(cass_session, product_id: str, zone_id: str, *, timeout: float = 30.0):
    """Дожидается появления строки остатка в inventory_by_product_zone."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = cass_session.execute(
            "SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s",
            (product_id, zone_id),
        ).one()
        if row is not None:
            return row
        time.sleep(0.5)
    return None
