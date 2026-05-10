"""Prometheus-метрики консьюмера."""
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST

REGISTRY = CollectorRegistry()

events_processed_total = Counter(
    "events_processed_total",
    "Кол-во успешно обработанных событий",
    labelnames=("event_type",),
    registry=REGISTRY,
)

events_failed_total = Counter(
    "events_failed_total",
    "Кол-во событий, отправленных в DLQ",
    labelnames=("event_type", "error_code"),
    registry=REGISTRY,
)

events_skipped_total = Counter(
    "events_skipped_total",
    "Кол-во пропущенных событий (дубликаты, out-of-order)",
    labelnames=("reason",),  # reason: duplicate | out_of_order
    registry=REGISTRY,
)

cassandra_write_errors_total = Counter(
    "cassandra_write_errors_total",
    "Ошибки записи в Cassandra",
    registry=REGISTRY,
)

event_processing_duration_seconds = Histogram(
    "event_processing_duration_seconds",
    "Время обработки одного события (от полного парсинга до коммита)",
    labelnames=("event_type",),
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
    registry=REGISTRY,
)

consumer_lag = Gauge(
    "consumer_lag",
    "Отставание консьюмера от HEAD (latest_offset - committed_offset)",
    labelnames=("topic", "partition"),
    registry=REGISTRY,
)

kafka_connected = Gauge(
    "kafka_connected", "1 если consumer подключён к Kafka, иначе 0", registry=REGISTRY
)
cassandra_connected = Gauge(
    "cassandra_connected", "1 если consumer подключён к Cassandra, иначе 0", registry=REGISTRY
)


def render_metrics() -> tuple[bytes, str]:
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
