from __future__ import annotations

import asyncio
import datetime as dt
import logging
import signal
import sys
import threading
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from config import cfg
from cassandra_client import CassandraClient
from handlers import HANDLERS, ValidationError, HandlerError
from schema_evolution import EventDeserializer
from dlq import DLQ
from health import start_http
from metrics import (
    events_processed_total,
    events_failed_total,
    events_skipped_total,
    event_processing_duration_seconds,
    consumer_lag,
    kafka_connected,
    cassandra_connected,
)

# ── Логирование ─────────────────────────────────────────────────
logging.basicConfig(level=cfg.LOG_LEVEL, format="%(message)s", stream=sys.stdout)

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, cfg.LOG_LEVEL, logging.INFO)
    ),
)

log = structlog.get_logger("consumer")


# ── Сигналы остановки ───────────────────────────────────────────
_running = True


def _stop(signum, _frame):
    global _running
    log.info("signal_received", signum=signum)
    _running = False


# ── Lag updater ─────────────────────────────────────────────────
def _start_lag_updater(consumer: Consumer, topic: str) -> threading.Thread:
    def loop():
        while _running:
            try:
                assignment = consumer.assignment()

                for tp in assignment:
                    if tp.topic != topic:
                        continue

                    try:
                        _, hi = consumer.get_watermark_offsets(
                            tp,
                            timeout=2.0,
                            cached=False,
                        )
                        committed = consumer.committed([tp], timeout=2.0)
                        com = (
                            committed[0].offset
                            if committed and committed[0].offset >= 0
                            else 0
                        )
                        lag = max(hi - com, 0)

                        consumer_lag.labels(
                            topic=tp.topic,
                            partition=str(tp.partition),
                        ).set(lag)

                    except Exception as e:
                        log.warning(
                            "lag_update_failed",
                            partition=tp.partition,
                            error=str(e),
                        )

            except Exception as e:
                log.warning("lag_loop_error", error=str(e))

            for _ in range(50):
                if not _running:
                    return
                threading.Event().wait(0.1)

    t = threading.Thread(target=loop, name="lag-updater", daemon=True)
    t.start()
    return t


# ── Основной цикл ───────────────────────────────────────────────
async def main_async() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    http_runner = await start_http(cfg.HTTP_PORT)

    cass = CassandraClient()
    cass.connect()

    deser = EventDeserializer(cfg.SCHEMA_REGISTRY_URL)
    dlq = DLQ()

    consumer = Consumer(
        {
            "bootstrap.servers": cfg.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": cfg.CONSUMER_GROUP,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 600000,
            "fetch.min.bytes": 1,
        }
    )

    consumer.subscribe([cfg.INPUT_TOPIC])
    kafka_connected.set(1)

    log.info(
        "consumer_started",
        bootstrap=cfg.KAFKA_BOOTSTRAP_SERVERS,
        group=cfg.CONSUMER_GROUP,
        topic=cfg.INPUT_TOPIC,
    )

    _start_lag_updater(consumer, cfg.INPUT_TOPIC)

    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _consume_loop, consumer, cass, deser, dlq)

    finally:
        log.info("shutting_down")

        try:
            consumer.close()
        except Exception:
            pass

        kafka_connected.set(0)

        try:
            dlq.flush(5.0)
        except Exception:
            pass

        cass.close()

        await http_runner.cleanup()
        log.info("shutdown_complete")


def _consume_loop(
    consumer: Consumer,
    cass: CassandraClient,
    deser: EventDeserializer,
    dlq: DLQ,
) -> None:
    while _running:
        msg = consumer.poll(timeout=cfg.POLL_TIMEOUT_SEC)

        if msg is None:
            continue

        if msg.error():
            err = msg.error()

            if err.code() == KafkaError._PARTITION_EOF:
                continue

            log.error("kafka_error", error=str(err))
            kafka_connected.set(0)
            continue

        kafka_connected.set(1)
        _process_one(consumer, cass, deser, dlq, msg)


def _process_one(consumer, cass: CassandraClient, deser: EventDeserializer, dlq: DLQ, msg) -> None:
    raw_value = msg.value()
    topic = msg.topic()
    partition = msg.partition()
    offset = msg.offset()

    log.info(
        "event_received",
        topic=topic,
        partition=partition,
        offset=offset,
        size=len(raw_value or b""),
    )

    # 1) Десериализация. Если падает — DLQ + commit, чтобы не зациклиться.
    try:
        ev: dict[str, Any] = deser.deserialize(raw_value, topic)

    except Exception as e:
        log.warning(
            "deserialization_failed",
            error=str(e),
            partition=partition,
            offset=offset,
        )

        events_failed_total.labels(
            event_type="UNKNOWN",
            error_code="DESERIALIZATION_ERROR",
        ).inc()

        dlq.send(
            raw_value=raw_value,
            decoded=None,
            error_reason=str(e),
            error_code="DESERIALIZATION_ERROR",
            topic=topic,
            partition=partition,
            offset=offset,
            key=msg.key(),
        )

        _commit_offset(consumer, msg)
        return

    event_id = ev.get("event_id")
    event_type = ev.get("event_type") or "UNKNOWN"

    if not event_id or not event_type:
        events_failed_total.labels(
            event_type=event_type,
            error_code="VALIDATION_ERROR",
        ).inc()

        dlq.send(
            raw_value=raw_value,
            decoded=ev,
            error_reason="Missing event_id or event_type",
            error_code="VALIDATION_ERROR",
            topic=topic,
            partition=partition,
            offset=offset,
            key=msg.key(),
        )

        _commit_offset(consumer, msg)
        return

    log.info(
        "event_parsed",
        event_id=event_id,
        event_type=event_type,
        partition=partition,
        offset=offset,
    )

    # 2) Поиск handler'а.
    handler = HANDLERS.get(event_type)

    if handler is None:
        events_failed_total.labels(
            event_type=event_type,
            error_code="UNKNOWN_EVENT_TYPE",
        ).inc()

        dlq.send(
            raw_value=raw_value,
            decoded=ev,
            error_reason=f"Unknown event_type: {event_type}",
            error_code="UNKNOWN_EVENT_TYPE",
            topic=topic,
            partition=partition,
            offset=offset,
            key=msg.key(),
        )

        _commit_offset(consumer, msg)
        return

    # 3) Handler строит обычные операции без mark_event.
    started = dt.datetime.now(dt.timezone.utc)

    try:
        ops = handler(cass, ev, partition, offset)

        if ops is None:
            # out-of-order: ничего не пишем, но offset коммитим.
            _commit_offset(consumer, msg)
            return

        # 4) Idempotency через отдельный LWT.
        # В Cassandra нельзя класть INSERT ... IF NOT EXISTS
        # в batch с несколькими таблицами.
        processed_at = dt.datetime.now(dt.timezone.utc)

        is_new = cass.mark_event_if_new(
            event_id=event_id,
            event_type=event_type,
            processed_at=processed_at,
            partition=partition,
            offset=offset,
        )

        if not is_new:
            events_skipped_total.labels(reason="duplicate").inc()

            log.info(
                "event_duplicate_skipped",
                event_id=event_id,
                event_type=event_type,
                partition=partition,
                offset=offset,
            )

            _commit_offset(consumer, msg)
            return

        # 5) Обычный LOGGED BATCH без IF NOT EXISTS.
        cass.execute_batch(ops)
        cassandra_connected.set(1)

    except ValidationError as e:
        events_failed_total.labels(
            event_type=event_type,
            error_code=e.code,
        ).inc()

        log.warning(
            "event_validation_failed",
            event_id=event_id,
            event_type=event_type,
            error=str(e),
        )

        dlq.send(
            raw_value=raw_value,
            decoded=ev,
            error_reason=str(e),
            error_code=e.code,
            topic=topic,
            partition=partition,
            offset=offset,
            key=msg.key(),
        )

        _commit_offset(consumer, msg)
        return

    except HandlerError as e:
        events_failed_total.labels(
            event_type=event_type,
            error_code=e.code,
        ).inc()

        log.warning(
            "handler_error",
            event_id=event_id,
            event_type=event_type,
            error=str(e),
        )

        dlq.send(
            raw_value=raw_value,
            decoded=ev,
            error_reason=str(e),
            error_code=e.code,
            topic=topic,
            partition=partition,
            offset=offset,
            key=msg.key(),
        )

        _commit_offset(consumer, msg)
        return

    except Exception as e:
        # Cassandra/infra ошибки: НЕ коммитим, чтобы Kafka повторила сообщение.
        cassandra_connected.set(0)

        log.error(
            "processing_failed_will_retry",
            event_id=event_id,
            event_type=event_type,
            error=str(e),
        )

        return

    finally:
        elapsed = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
        event_processing_duration_seconds.labels(event_type=event_type).observe(elapsed)

    # 6) Успех: метрики + commit.
    events_processed_total.labels(event_type=event_type).inc()

    log.info(
        "event_processed",
        event_id=event_id,
        event_type=event_type,
        partition=partition,
        offset=offset,
    )

    _commit_offset(consumer, msg)


def _commit_offset(consumer, msg) -> None:
    """Коммитим offset current + 1 после успеха или обработанной ошибки."""
    try:
        tp = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
        consumer.commit(offsets=[tp], asynchronous=False)

    except KafkaException as e:
        log.warning("commit_failed", error=str(e))


if __name__ == "__main__":
    asyncio.run(main_async())