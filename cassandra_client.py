from __future__ import annotations

import datetime as dt
from typing import Optional

from cassandra.cluster import Cluster, Session, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    TokenAwarePolicy,
    ExponentialReconnectionPolicy,
)
from cassandra.query import BatchStatement, BatchType, ConsistencyLevel
import structlog

from config import cfg
from metrics import cassandra_connected, cassandra_write_errors_total

log = structlog.get_logger(__name__)


class CassandraClient:
    def __init__(self) -> None:
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None
        self._prepared: dict[str, object] = {}

    def connect(self) -> None:
        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc="dc1")
            ),
            consistency_level=ConsistencyLevel.QUORUM,
            request_timeout=15.0,
        )

        self.cluster = Cluster(
            contact_points=cfg.CASSANDRA_HOSTS,
            port=cfg.CASSANDRA_PORT,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            reconnection_policy=ExponentialReconnectionPolicy(1.0, 60.0),
            protocol_version=5,
        )

        self.session = self.cluster.connect(cfg.CASSANDRA_KEYSPACE)
        self._prepare_all()

        cassandra_connected.set(1)
        log.info(
            "cassandra_connected",
            hosts=cfg.CASSANDRA_HOSTS,
            keyspace=cfg.CASSANDRA_KEYSPACE,
        )

    def close(self) -> None:
        if self.cluster is not None:
            self.cluster.shutdown()
        cassandra_connected.set(0)

    # ── Prepared statements ────────────────────────────────────────
    def _prepare_all(self) -> None:
        assert self.session is not None
        s = self.session

        self._prepared["select_zone"] = s.prepare(
            "SELECT available_quantity, reserved_quantity, last_event_timestamp, last_event_id, supplier_id "
            "FROM inventory_by_product_zone WHERE product_id=? AND zone_id=?"
        )

        self._prepared["upsert_zone"] = s.prepare(
            "INSERT INTO inventory_by_product_zone "
            "(product_id, zone_id, available_quantity, reserved_quantity, "
            " last_event_id, last_event_timestamp, supplier_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )

        self._prepared["upsert_product"] = s.prepare(
            "INSERT INTO inventory_by_product "
            "(product_id, zone_id, available_quantity, reserved_quantity, last_event_timestamp) "
            "VALUES (?, ?, ?, ?, ?)"
        )

        self._prepared["upsert_zone_idx"] = s.prepare(
            "INSERT INTO inventory_by_zone "
            "(zone_id, product_id, available_quantity, reserved_quantity, last_event_timestamp) "
            "VALUES (?, ?, ?, ?, ?)"
        )

        self._prepared["check_event"] = s.prepare(
            "SELECT event_id FROM processed_events WHERE event_id=?"
        )

        self._prepared["mark_event"] = s.prepare(
            "INSERT INTO processed_events (event_id, event_type, processed_at, partition, offset) "
            "VALUES (?, ?, ?, ?, ?) IF NOT EXISTS"
        )

        self._prepared["upsert_order"] = s.prepare(
            "INSERT INTO orders (order_id, status, items, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)"
        )

        self._prepared["update_order_status"] = s.prepare(
            "UPDATE orders SET status=?, updated_at=? WHERE order_id=?"
        )

        self._prepared["select_order"] = s.prepare(
            "SELECT order_id, status, items FROM orders WHERE order_id=?"
        )

        self._prepared["insert_history"] = s.prepare(
            "INSERT INTO event_history "
            "(event_date, event_timestamp, event_id, event_type, product_id, zone_id, quantity, payload_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )

    def stmt(self, name: str):
        return self._prepared[name]

    # ── High-level API ─────────────────────────────────────────────
    def is_event_processed(self, event_id: str) -> bool:
        assert self.session is not None
        rs = self.session.execute(self.stmt("check_event"), (event_id,))
        return rs.one() is not None

    def mark_event_if_new(
        self,
        event_id: str,
        event_type: str,
        processed_at: dt.datetime,
        partition: int,
        offset: int,
    ) -> bool:
        """
        Idempotency через LWT.

        Важно:
        Cassandra НЕ разрешает класть INSERT ... IF NOT EXISTS
        в один batch с операциями по другим таблицам.

        Поэтому mark_event выполняется отдельно от execute_batch().

        Returns:
            True  -> событие новое, можно применять state changes.
            False -> событие уже обработано, его надо пропустить.
        """
        assert self.session is not None

        rs = self.session.execute(
            self.stmt("mark_event"),
            (event_id, event_type, processed_at, partition, offset),
        )

        row = rs.one()

        if row is None:
            return False

        # У LWT Cassandra возвращает колонку [applied].
        # В драйвере она обычно доступна как row.applied.
        try:
            return bool(row.applied)
        except AttributeError:
            return bool(row[0])

    def get_zone_state(self, product_id: str, zone_id: str):
        assert self.session is not None
        return self.session.execute(
            self.stmt("select_zone"),
            (product_id, zone_id),
        ).one()

    def get_order(self, order_id: str):
        assert self.session is not None
        return self.session.execute(
            self.stmt("select_order"),
            (order_id,),
        ).one()

    def execute_batch(self, statements: list) -> None:
        """
        Выполняет обычный LOGGED BATCH.

        ВАЖНО:
        сюда нельзя передавать mark_event,
        потому что mark_event содержит IF NOT EXISTS.

        Если mark_event случайно попадёт сюда, мы его пропускаем,
        чтобы Cassandra не упала с ошибкой:
        "Batch with conditions cannot span multiple tables".
        """
        assert self.session is not None

        try:
            batch = BatchStatement(
                batch_type=BatchType.LOGGED,
                consistency_level=ConsistencyLevel.QUORUM,
            )

            added = 0

            for prepared_stmt, params in statements:
                if prepared_stmt is self.stmt("mark_event"):
                    log.warning("mark_event_skipped_inside_batch")
                    continue

                batch.add(prepared_stmt, params)
                added += 1

            if added == 0:
                log.info("empty_batch_skipped")
                return

            self.session.execute(batch)
            log.info("cassandra_batch_executed", statements=added)

        except Exception as e:
            cassandra_write_errors_total.inc()
            log.error("cassandra_batch_failed", error=str(e))
            raise

    def healthcheck(self) -> bool:
        assert self.session is not None

        try:
            self.session.execute("SELECT now() FROM system.local")
            cassandra_connected.set(1)
            return True
        except Exception as e:
            cassandra_connected.set(0)
            log.warning("cassandra_healthcheck_failed", error=str(e))
            return False