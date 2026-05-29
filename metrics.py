"""Prometheus-метрики WMS-API сервиса.

Реализует обязательную для ДЗ №7 (п.4) тройку метрик на каждый сервис:
  * http_requests_total          Counter   {method, endpoint, status}
  * http_request_errors_total    Counter   {method, endpoint, error_type}
  * http_request_duration_seconds Histogram {method, endpoint}

Плюс доменные метрики write-path (события, отправленные в Kafka).

Сбор HTTP-метрик автоматический — через ASGI middleware (см. PrometheusMiddleware),
поэтому ни один handler не считает метрики руками.
"""
from __future__ import annotations

import time

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from starlette.requests import Request
from starlette.responses import Response
from starlette.middleware.base import BaseHTTPMiddleware

REGISTRY = CollectorRegistry()

# ── Обязательная тройка HTTP-метрик ────────────────────────────────
http_requests_total = Counter(
    "http_requests_total",
    "Общее количество HTTP-запросов",
    labelnames=("method", "endpoint", "status"),
    registry=REGISTRY,
)

http_request_errors_total = Counter(
    "http_request_errors_total",
    "Количество HTTP-запросов, завершившихся ошибкой (4xx/5xx или исключение)",
    labelnames=("method", "endpoint", "error_type"),
    registry=REGISTRY,
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "Время обработки HTTP-запроса, секунды",
    labelnames=("method", "endpoint"),
    # Бакеты подобраны вокруг SLO p95 < 500ms, чтобы перцентили считались точно.
    buckets=(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0),
    registry=REGISTRY,
)

# ── Доменные метрики write-path ────────────────────────────────────
events_published_total = Counter(
    "events_published_total",
    "События, успешно опубликованные в Kafka",
    labelnames=("event_type",),
    registry=REGISTRY,
)

events_rejected_total = Counter(
    "events_rejected_total",
    "События, отклонённые на этапе валидации запроса (не дошли до Kafka)",
    labelnames=("event_type", "reason"),
    registry=REGISTRY,
)

kafka_connected = Gauge(
    "api_kafka_connected", "1 если publisher подключён к Kafka, иначе 0", registry=REGISTRY
)
cassandra_connected = Gauge(
    "api_cassandra_connected", "1 если read-path подключён к Cassandra, иначе 0", registry=REGISTRY
)


def render_metrics() -> tuple[bytes, str]:
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST


def _route_template(request: Request) -> str:
    """Возвращает шаблон маршрута (например /api/v1/inventory/{product_id}),
    а не конкретный путь, чтобы не плодить кардинальность меток."""
    route = request.scope.get("route")
    if route is not None and getattr(route, "path", None):
        return route.path
    return request.url.path


class PrometheusMiddleware(BaseHTTPMiddleware):
    """Автоматически собирает HTTP-метрики для всех запросов."""

    async def dispatch(self, request: Request, call_next):
        # /metrics сам себя не учитывает, чтобы не зашумлять статистику.
        endpoint = _route_template(request)
        if endpoint == "/metrics":
            return await call_next(request)

        method = request.method
        start = time.perf_counter()
        status_code = 500
        error_type = ""

        try:
            response: Response = await call_next(request)
            status_code = response.status_code
            if status_code >= 500:
                error_type = "server_error"
            elif status_code >= 400:
                error_type = "client_error"
            return response
        except Exception:
            error_type = "exception"
            raise
        finally:
            elapsed = time.perf_counter() - start
            http_request_duration_seconds.labels(method=method, endpoint=endpoint).observe(elapsed)
            http_requests_total.labels(
                method=method, endpoint=endpoint, status=str(status_code)
            ).inc()
            if error_type:
                http_request_errors_total.labels(
                    method=method, endpoint=endpoint, error_type=error_type
                ).inc()
