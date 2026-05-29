"""Read-path WMS-API: чтение остатков из Cassandra (CQRS — запись через Kafka,
чтение напрямую из материализованных таблиц)."""
from __future__ import annotations

from typing import Any, Optional

import structlog
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import ConsistencyLevel

from config import cfg
from metrics import cassandra_connected

log = structlog.get_logger("reader")


class InventoryReader:
    def __init__(self) -> None:
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None
        self._by_product = None
        self._by_zone = None

    def connect(self) -> None:
        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=cfg.CASSANDRA_LOCAL_DC)
            ),
            # Чтения отчётов терпимы к eventual consistency; ONE даёт минимальную
            # latency. Сильная согласованность здесь не критична (read-path).
            consistency_level=ConsistencyLevel.ONE,
            request_timeout=10.0,
        )
        self.cluster = Cluster(
            contact_points=cfg.CASSANDRA_HOSTS,
            port=cfg.CASSANDRA_PORT,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=5,
        )
        self.session = self.cluster.connect(cfg.CASSANDRA_KEYSPACE)
        self._by_product = self.session.prepare(
            "SELECT product_id, zone_id, available_quantity, reserved_quantity "
            "FROM inventory_by_product WHERE product_id=?"
        )
        self._by_zone = self.session.prepare(
            "SELECT zone_id, product_id, available_quantity, reserved_quantity "
            "FROM inventory_by_zone WHERE zone_id=?"
        )
        cassandra_connected.set(1)
        log.info("cassandra_connected", hosts=cfg.CASSANDRA_HOSTS)

    def close(self) -> None:
        if self.cluster is not None:
            self.cluster.shutdown()
        cassandra_connected.set(0)

    def inventory_by_product(self, product_id: str) -> list[dict[str, Any]]:
        assert self.session is not None
        rows = self.session.execute(self._by_product, (product_id,))
        return [
            {
                "product_id": r.product_id,
                "zone_id": r.zone_id,
                "available_quantity": int(r.available_quantity or 0),
                "reserved_quantity": int(r.reserved_quantity or 0),
            }
            for r in rows
        ]

    def inventory_by_zone(self, zone_id: str) -> list[dict[str, Any]]:
        assert self.session is not None
        rows = self.session.execute(self._by_zone, (zone_id,))
        return [
            {
                "zone_id": r.zone_id,
                "product_id": r.product_id,
                "available_quantity": int(r.available_quantity or 0),
                "reserved_quantity": int(r.reserved_quantity or 0),
            }
            for r in rows
        ]

    def healthy(self) -> bool:
        try:
            assert self.session is not None
            self.session.execute("SELECT now() FROM system.local")
            cassandra_connected.set(1)
            return True
        except Exception:  # noqa: BLE001
            cassandra_connected.set(0)
            return False
