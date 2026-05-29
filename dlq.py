from __future__ import annotations
import base64
import datetime as dt
import json
import threading
from typing import Any

from confluent_kafka import Producer
import structlog

from config import cfg

log = structlog.get_logger(__name__)


class DLQ:
    def __init__(self) -> None:
        self.producer = Producer({
            "bootstrap.servers": cfg.KAFKA_BOOTSTRAP_SERVERS,
            "linger.ms": 50,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
        })
        self._lock = threading.Lock()

    def send(
        self,
        *,
        raw_value: bytes | None,
        decoded: dict[str, Any] | None,
        error_reason: str,
        error_code: str,
        topic: str,
        partition: int,
        offset: int,
        key: bytes | None = None,
    ) -> None:
        original: Any
        if decoded is not None:
            original = _jsonable(decoded)
        elif raw_value is not None:
            original = {"_base64": base64.b64encode(raw_value).decode("ascii")}
        else:
            original = None

        msg = {
            "original_event": original,
            "error_reason": error_reason,
            "error_code": error_code,
            "failed_at": dt.datetime.utcnow().isoformat() + "Z",
            "kafka_metadata": {"topic": topic, "partition": partition, "offset": offset},
        }
        body = json.dumps(msg, ensure_ascii=False, default=str).encode("utf-8")
        with self._lock:
            self.producer.produce(cfg.DLQ_TOPIC, value=body, key=key)
            self.producer.poll(0)

    def flush(self, timeout: float = 5.0) -> None:
        self.producer.flush(timeout)


def _jsonable(d):
    if isinstance(d, dict):
        return {k: _jsonable(v) for k, v in d.items()}
    if isinstance(d, list):
        return [_jsonable(v) for v in d]
    if isinstance(d, dt.datetime):
        return d.isoformat()
    if isinstance(d, (bytes, bytearray)):
        return base64.b64encode(bytes(d)).decode("ascii")
    return d
