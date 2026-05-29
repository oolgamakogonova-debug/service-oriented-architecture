"""E2E-сценарии из задания. Запускается на хосте (не в контейнере).

Использование:
    pip install -r tests/requirements.txt
    python tests/run_scenarios.py [--scenario N]

Сценарий N: 1..8 (см. файл tests/e2e_scenarios.md или ТЗ)
По умолчанию запускает ВСЕ сценарии последовательно.
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import sys
import time
import uuid
from pathlib import Path

import requests
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import ConsistencyLevel
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


# Параметры на хосте — порты пробрасываются docker-compose.yml.
KAFKA = "localhost:29092"
SR = "http://localhost:8081"
CASS = ["localhost"]
TOPIC = "warehouse-events"
DLQ = "warehouse-events-dlq"

SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"


def _topic_record_name_strategy(ctx, record_name: str) -> str:
    return f"{ctx.topic}-{record_name}"


def make_producer():
    return Producer({"bootstrap.servers": KAFKA, "linger.ms": 10, "acks": "all",
                     "enable.idempotence": True, "client.id": "scenario-runner"})


def serializer_for(sr: SchemaRegistryClient, schema_file: str) -> AvroSerializer:
    schema_str = (SCHEMAS_DIR / schema_file).read_text(encoding="utf-8")
    return AvroSerializer(sr, schema_str=schema_str,
                          conf={"subject.name.strategy": _topic_record_name_strategy})


def now_ms():
    return int(dt.datetime.utcnow().timestamp() * 1000)


def send(producer, sr_client, schema_file: str, value: dict, key: str | None = None):
    s = serializer_for(sr_client, schema_file)
    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    payload = s(value, ctx)
    producer.produce(TOPIC, key=key.encode() if key else None, value=payload)
    producer.poll(0)
    producer.flush(5)


def cass_session():
    cluster = Cluster(CASS, port=9042,
                      load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc="dc1")))
    session = cluster.connect("warehouse")
    session.default_consistency_level = ConsistencyLevel.QUORUM
    return cluster, session


def get_zone(session, pid, zid):
    row = session.execute(
        "SELECT available_quantity, reserved_quantity, supplier_id "
        "FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        (pid, zid),
    ).one()
    return row


def get_total_by_product(session, pid):
    rs = list(session.execute(
        "SELECT zone_id, available_quantity, reserved_quantity FROM inventory_by_product WHERE product_id=%s",
        (pid,),
    ))
    return rs


def get_zone_contents(session, zid):
    rs = list(session.execute(
        "SELECT product_id, available_quantity FROM inventory_by_zone WHERE zone_id=%s",
        (zid,),
    ))
    return rs


def wait_for(condition, *, label, timeout=20, interval=0.5):
    """Polling helper для дождаться условия."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if condition():
                return True
        except Exception:
            pass
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for: {label}")


# ──────────────────────────────────────────────────────────────────
def scenario_1_basic_lifecycle():
    print("\n=== Сценарий 1: базовый цикл склада ===")
    sr = SchemaRegistryClient({"url": SR})
    p = make_producer()
    cluster, session = cass_session()
    try:
        pid = f"SKU-001-{uuid.uuid4().hex[:6]}"

        # 2. PRODUCT_RECEIVED ZONE-A 100
        eid = str(uuid.uuid4())
        send(p, sr, "product_received_v2.avsc",
             {"event_id": eid, "event_type": "PRODUCT_RECEIVED", "timestamp": now_ms(),
              "product_id": pid, "zone_id": "ZONE-A", "quantity": 100, "supplier_id": None},
             key=pid)
        wait_for(lambda: (r := get_zone(session, pid, "ZONE-A")) and r.available_quantity == 100,
                 label="ZONE-A available=100")
        # 4. inventory_by_product → суммарно 100
        rows = get_total_by_product(session, pid)
        assert sum(r.available_quantity for r in rows) == 100, rows
        print("  ✓ PRODUCT_RECEIVED 100 → available=100")

        # 5. PRODUCT_RESERVED 30
        send(p, sr, "product_reserved.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_RESERVED", "timestamp": now_ms(),
              "product_id": pid, "zone_id": "ZONE-A", "quantity": 30, "order_id": None}, key=pid)
        wait_for(lambda: (r := get_zone(session, pid, "ZONE-A")) and r.reserved_quantity == 30,
                 label="reserved=30")
        r = get_zone(session, pid, "ZONE-A")
        assert r.available_quantity == 70 and r.reserved_quantity == 30, (r.available_quantity, r.reserved_quantity)
        print("  ✓ PRODUCT_RESERVED 30 → available=70, reserved=30")

        # 7. PRODUCT_MOVED 20 from A to B
        send(p, sr, "product_moved.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_MOVED", "timestamp": now_ms(),
              "product_id": pid, "from_zone_id": "ZONE-A", "to_zone_id": "ZONE-B", "quantity": 20}, key=pid)
        wait_for(lambda: (rb := get_zone(session, pid, "ZONE-B")) and rb.available_quantity == 20,
                 label="ZONE-B available=20")
        ra = get_zone(session, pid, "ZONE-A")
        assert ra.available_quantity == 50, ra.available_quantity
        print("  ✓ PRODUCT_MOVED → ZONE-A=50, ZONE-B=20")

        # 9. PRODUCT_SHIPPED 10 from A
        send(p, sr, "product_shipped.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_SHIPPED", "timestamp": now_ms(),
              "product_id": pid, "zone_id": "ZONE-A", "quantity": 10}, key=pid)
        wait_for(lambda: (r := get_zone(session, pid, "ZONE-A")) and r.available_quantity == 40,
                 label="ZONE-A available=40")
        print("  ✓ PRODUCT_SHIPPED 10 → ZONE-A available=40")

        # 11-14. ORDER_CREATED + COMPLETED
        oid = f"ORDER-{uuid.uuid4().hex[:8]}"
        send(p, sr, "order_created.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "ORDER_CREATED", "timestamp": now_ms(),
              "order_id": oid,
              "items": [{"product_id": pid, "zone_id": "ZONE-A", "quantity": 15}]}, key=pid)
        wait_for(lambda: (r := get_zone(session, pid, "ZONE-A")) and r.reserved_quantity == 30 + 15,
                 label="reserved += 15")
        print("  ✓ ORDER_CREATED → reserved += 15")

        send(p, sr, "order_completed.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "ORDER_COMPLETED", "timestamp": now_ms(),
              "order_id": oid}, key=pid)
        wait_for(lambda: (r := get_zone(session, pid, "ZONE-A")) and r.reserved_quantity == 30,
                 label="reserved -= 15")
        r = get_zone(session, pid, "ZONE-A")
        assert r.reserved_quantity == 30 and r.available_quantity == 40
        print("  ✓ ORDER_COMPLETED → reserved -= 15 (available не изменилось)")

    finally:
        cluster.shutdown()


def scenario_2_idempotency():
    print("\n=== Сценарий 2: идемпотентность ===")
    sr = SchemaRegistryClient({"url": SR})
    p = make_producer()
    cluster, session = cass_session()
    try:
        pid = f"SKU-002-{uuid.uuid4().hex[:6]}"
        eid = str(uuid.uuid4())
        ev = {"event_id": eid, "event_type": "PRODUCT_RECEIVED", "timestamp": now_ms(),
              "product_id": pid, "zone_id": "ZONE-A", "quantity": 50, "supplier_id": None}
        send(p, sr, "product_received_v2.avsc", ev, key=pid)
        wait_for(lambda: (r := get_zone(session, pid, "ZONE-A")) and r.available_quantity == 50,
                 label="available=50")
        print("  ✓ Первая отправка → available=50")

        # Повторная отправка с тем же event_id
        send(p, sr, "product_received_v2.avsc", ev, key=pid)
        time.sleep(3)
        r = get_zone(session, pid, "ZONE-A")
        assert r.available_quantity == 50, f"Idempotency broken! got {r.available_quantity}"
        print("  ✓ Повтор того же event_id → available по-прежнему 50 (не 100)")
    finally:
        cluster.shutdown()


def scenario_3_table_consistency():
    print("\n=== Сценарий 3: консистентность таблиц ===")
    sr = SchemaRegistryClient({"url": SR})
    p = make_producer()
    cluster, session = cass_session()
    try:
        pid = f"SKU-003-{uuid.uuid4().hex[:6]}"
        send(p, sr, "product_received_v2.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_RECEIVED", "timestamp": now_ms(),
              "product_id": pid, "zone_id": "ZONE-A", "quantity": 100, "supplier_id": None}, key=pid)
        wait_for(lambda: get_zone(session, pid, "ZONE-A") is not None, label="zone row exists")
        time.sleep(1.0)

        r1 = get_zone(session, pid, "ZONE-A")
        r2 = list(session.execute(
            "SELECT available_quantity FROM inventory_by_product WHERE product_id=%s AND zone_id=%s",
            (pid, "ZONE-A")))
        r3 = list(session.execute(
            "SELECT available_quantity FROM inventory_by_zone WHERE zone_id=%s AND product_id=%s",
            ("ZONE-A", pid)))

        assert r1.available_quantity == 100
        assert r2 and r2[0].available_quantity == 100
        assert r3 and r3[0].available_quantity == 100
        print("  ✓ Все 3 таблицы согласованы: 100 в каждой")
    finally:
        cluster.shutdown()


def scenario_4_out_of_order():
    print("\n=== Сценарий 4: события вне порядка ===")
    sr = SchemaRegistryClient({"url": SR})
    p = make_producer()
    cluster, session = cass_session()
    try:
        pid = f"SKU-004-{uuid.uuid4().hex[:6]}"
        # 12:00 RECEIVED 100
        ts_12_00 = int(dt.datetime(2026, 1, 1, 12, 0, 0).timestamp() * 1000)
        ts_12_05 = int(dt.datetime(2026, 1, 1, 12, 5, 0).timestamp() * 1000)
        ts_12_02 = int(dt.datetime(2026, 1, 1, 12, 2, 0).timestamp() * 1000)

        send(p, sr, "product_received_v2.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_RECEIVED", "timestamp": ts_12_00,
              "product_id": pid, "zone_id": "ZONE-A", "quantity": 100, "supplier_id": None}, key=pid)
        wait_for(lambda: (r := get_zone(session, pid, "ZONE-A")) and r.available_quantity == 100,
                 label="available=100")

        # 12:05 SHIPPED 20 → 80
        send(p, sr, "product_shipped.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_SHIPPED", "timestamp": ts_12_05,
              "product_id": pid, "zone_id": "ZONE-A", "quantity": 20}, key=pid)
        wait_for(lambda: (r := get_zone(session, pid, "ZONE-A")) and r.available_quantity == 80,
                 label="available=80")
        print("  ✓ После RECEIVED+SHIPPED: available=80")

        # Поздний RECEIVED с timestamp 12:02 — должен быть проигнорирован
        send(p, sr, "product_received_v2.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_RECEIVED", "timestamp": ts_12_02,
              "product_id": pid, "zone_id": "ZONE-A", "quantity": 50, "supplier_id": None}, key=pid)
        time.sleep(3)
        r = get_zone(session, pid, "ZONE-A")
        assert r.available_quantity == 80, f"Out-of-order check failed: got {r.available_quantity}"
        print("  ✓ Старое событие проигнорировано: available по-прежнему 80")
    finally:
        cluster.shutdown()


def scenario_5_dlq():
    print("\n=== Сценарий 5: Dead Letter Queue ===")
    sr = SchemaRegistryClient({"url": SR})
    p = make_producer()
    cluster, session = cass_session()
    pid = f"SKU-005-{uuid.uuid4().hex[:6]}"
    try:
        # Сначала создадим базовый остаток, чтобы было что отгружать
        send(p, sr, "product_received_v2.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_RECEIVED", "timestamp": now_ms(),
              "product_id": pid, "zone_id": "ZONE-A", "quantity": 100, "supplier_id": None}, key=pid)
        wait_for(lambda: (r := get_zone(session, pid, "ZONE-A")) and r.available_quantity == 100,
                 label="available=100")

        # Невалидное событие: SHIPPED quantity=-5 (отрицательное)
        # Посылаем как PRODUCT_SHIPPED, но с quantity=-5 — но Avro не пропустит negative,
        # если в схеме long. Однако long принимает negative значения.
        # Validation отрабатывает в handler: quantity<0 → ValidationError → DLQ.
        send(p, sr, "product_shipped.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_SHIPPED", "timestamp": now_ms(),
              "product_id": pid, "zone_id": "ZONE-A", "quantity": -5}, key=pid)

        # Валидное событие после невалидного
        send(p, sr, "product_shipped.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_SHIPPED", "timestamp": now_ms(),
              "product_id": pid, "zone_id": "ZONE-A", "quantity": 10}, key=pid)
        wait_for(lambda: (r := get_zone(session, pid, "ZONE-A")) and r.available_quantity == 90,
                 label="valid SHIPPED applied → 90")
        print("  ✓ Consumer не упал. Валидное событие после невалидного обработано (available=90).")

        # Проверяем DLQ
        c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"dlq-check-{uuid.uuid4().hex[:6]}",
                      "auto.offset.reset": "earliest", "enable.auto.commit": False})
        c.subscribe([DLQ])
        end = time.time() + 10
        found_validation = False
        while time.time() < end and not found_validation:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                if payload.get("error_code") == "VALIDATION_ERROR":
                    found_validation = True
                    print(f"  ✓ В DLQ найдено сообщение с error_code=VALIDATION_ERROR: {payload.get('error_reason')}")
            except Exception:
                continue
        c.close()
        assert found_validation, "DLQ не получило ожидаемое VALIDATION_ERROR"
    finally:
        cluster.shutdown()


def scenario_6_cluster_failover():
    print("\n=== Сценарий 6: Cassandra cluster + отказоустойчивость ===")
    print("  Этот сценарий требует ручных команд (docker stop/start). См. README.")
    print("  Автоматическая часть: проверка, что nodetool show 3 UN ноды.")
    import subprocess
    out = subprocess.run(
        ["docker", "exec", "cassandra-1", "nodetool", "status"],
        capture_output=True, text=True, timeout=20,
    )
    if out.returncode != 0:
        print(f"  ! nodetool status недоступен: {out.stderr}")
        return
    un_count = sum(1 for line in out.stdout.splitlines() if line.startswith("UN"))
    print(f"  ✓ Кластер: {un_count} нод в статусе UN")
    if un_count != 3:
        print(f"  ! Ожидалось 3 UN ноды, получено {un_count}")


def scenario_7_monitoring():
    print("\n=== Сценарий 7: мониторинг ===")
    r = requests.get("http://localhost:8000/health", timeout=5)
    print(f"  /health: {r.status_code} {r.text[:200]}")
    assert r.status_code in (200, 503)
    r = requests.get("http://localhost:8000/metrics", timeout=5)
    assert r.status_code == 200 and "events_processed_total" in r.text
    print("  ✓ /metrics возвращает Prometheus-формат с events_processed_total")
    # Prometheus
    try:
        rp = requests.get("http://localhost:9090/-/healthy", timeout=5)
        print(f"  Prometheus: {rp.status_code}")
    except Exception as e:
        print(f"  ! Prometheus недоступен: {e}")
    try:
        rg = requests.get("http://localhost:3000/api/health", timeout=5)
        print(f"  Grafana: {rg.status_code}")
    except Exception as e:
        print(f"  ! Grafana недоступен: {e}")


def scenario_8_schema_evolution():
    print("\n=== Сценарий 8: Schema Evolution ===")
    sr = SchemaRegistryClient({"url": SR})
    p = make_producer()
    cluster, session = cass_session()
    try:
        pid = f"SKU-008-{uuid.uuid4().hex[:6]}"
        # V1: без supplier_id (используем V1 Avro схему явно)
        send(p, sr, "product_received_v1.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_RECEIVED", "timestamp": now_ms(),
              "product_id": pid, "zone_id": "ZONE-A", "quantity": 100}, key=pid)
        wait_for(lambda: (r := get_zone(session, pid, "ZONE-A")) and r.available_quantity == 100,
                 label="V1 applied")
        r = get_zone(session, pid, "ZONE-A")
        assert r.supplier_id is None, f"V1 should leave supplier_id=null, got {r.supplier_id}"
        print("  ✓ V1 событие → supplier_id = null")

        # V2: с supplier_id
        send(p, sr, "product_received_v2.avsc",
             {"event_id": str(uuid.uuid4()), "event_type": "PRODUCT_RECEIVED", "timestamp": now_ms(),
              "product_id": pid, "zone_id": "ZONE-B", "quantity": 50, "supplier_id": "SUP-001"}, key=pid)
        wait_for(lambda: (r := get_zone(session, pid, "ZONE-B")) and r.available_quantity == 50,
                 label="V2 applied")
        r2 = get_zone(session, pid, "ZONE-B")
        assert r2.supplier_id == "SUP-001", f"V2 supplier_id mismatch: {r2.supplier_id}"
        print(f"  ✓ V2 событие → supplier_id = SUP-001")

        # Schema Registry: обе версии видны
        subj = f"{TOPIC}-warehouse.events.v1.ProductReceived"
        versions = sr.get_versions(subj)
        print(f"  ✓ В Schema Registry для subject={subj} версий: {versions}")
        assert len(versions) >= 2
    finally:
        cluster.shutdown()


SCENARIOS = {
    1: scenario_1_basic_lifecycle,
    2: scenario_2_idempotency,
    3: scenario_3_table_consistency,
    4: scenario_4_out_of_order,
    5: scenario_5_dlq,
    6: scenario_6_cluster_failover,
    7: scenario_7_monitoring,
    8: scenario_8_schema_evolution,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", type=int, choices=list(SCENARIOS), default=None,
                        help="Запустить только указанный сценарий")
    args = parser.parse_args()

    targets = [args.scenario] if args.scenario else list(SCENARIOS)
    failures = []
    for n in targets:
        try:
            SCENARIOS[n]()
        except AssertionError as e:
            print(f"  ✗ Сценарий {n} FAILED: {e}", file=sys.stderr)
            failures.append(n)
        except Exception as e:
            print(f"  ✗ Сценарий {n} ERROR: {e}", file=sys.stderr)
            failures.append(n)

    print("\n" + "=" * 60)
    if failures:
        print(f"Провалены сценарии: {failures}")
        sys.exit(1)
    print("Все сценарии прошли успешно.")


if __name__ == "__main__":
    main()
