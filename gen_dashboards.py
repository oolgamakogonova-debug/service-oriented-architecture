#!/usr/bin/env python3
"""Генератор Grafana-дашбордов (ДЗ №7, п.5 и п.6).

Собирает дашборды как Python-словари и пишет валидный JSON в
grafana/dashboards/. Так JSON гарантированно корректен и легко поддерживается.

Запуск:  python scripts/gen_dashboards.py
"""
from __future__ import annotations

import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}
OUT = Path(__file__).resolve().parents[1] / "grafana" / "dashboards"

_pid = 0


def _next_id() -> int:
    global _pid
    _pid += 1
    return _pid


def ts_panel(title, targets, x, y, w=12, h=8, unit=None, desc=""):
    """timeseries-панель."""
    return {
        "id": _next_id(),
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {
            "defaults": {
                "unit": unit or "short",
                "custom": {"drawStyle": "line", "fillOpacity": 10, "showPoints": "never"},
            },
            "overrides": [],
        },
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        "targets": [
            {"expr": e, "legendFormat": lf, "refId": chr(65 + i), "datasource": DS}
            for i, (e, lf) in enumerate(targets)
        ],
    }


def stat_panel(title, expr, x, y, w=6, h=6, unit=None, thresholds=None, desc=""):
    steps = thresholds or [{"color": "green", "value": None}]
    return {
        "id": _next_id(),
        "type": "stat",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {
            "defaults": {"unit": unit or "short", "thresholds": {"mode": "absolute", "steps": steps}},
            "overrides": [],
        },
        "options": {
            "colorMode": "background",
            "graphMode": "area",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        },
        "targets": [{"expr": expr, "refId": "A", "datasource": DS}],
    }


def dashboard(uid, title, panels, tags):
    return {
        "uid": uid,
        "title": title,
        "tags": tags,
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "templating": {"list": []},
        "annotations": {"list": []},
        "panels": panels,
    }


def build_api():
    global _pid
    _pid = 0
    P = []
    P.append(stat_panel(
        "Availability (SLI), %", "100 * sli:api_availability:ratio_5m", 0, 0, w=6, unit="percent",
        thresholds=[{"color": "red", "value": None}, {"color": "yellow", "value": 95}, {"color": "green", "value": 99}],
        desc="Доля не-5xx запросов за 5m (SLO >= 99%)",
    ))
    P.append(stat_panel(
        "p95 latency (SLI), ms", "1000 * sli:api_latency_p95_seconds:5m", 6, 0, w=6, unit="ms",
        thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 300}, {"color": "red", "value": 500}],
        desc="p95 времени ответа (SLO < 500ms)",
    ))
    P.append(stat_panel(
        "Throughput, req/s", "sum(rate(http_requests_total[1m]))", 12, 0, w=6, unit="reqps",
    ))
    P.append(stat_panel(
        "Error rate, %",
        "100 * (sum(rate(http_requests_total{status=~\"5..\"}[5m])) or vector(0)) "
        "/ clamp_min(sum(rate(http_requests_total[5m])), 1)",
        18, 0, w=6, unit="percent",
        thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 1}, {"color": "red", "value": 5}],
    ))
    P.append(ts_panel(
        "Latency percentiles (p50/p95/p99)",
        [
            ("1000 * histogram_quantile(0.50, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))", "p50"),
            ("1000 * histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))", "p95"),
            ("1000 * histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))", "p99"),
        ],
        0, 6, w=12, unit="ms", desc="Перцентили времени ответа",
    ))
    P.append(ts_panel(
        "Throughput by endpoint, req/s",
        [("sum by (endpoint) (rate(http_requests_total[1m]))", "{{endpoint}}")],
        12, 6, w=12, unit="reqps",
    ))
    P.append(ts_panel(
        "Requests by status",
        [("sum by (status) (rate(http_requests_total[1m]))", "{{status}}")],
        0, 14, w=12, unit="reqps",
    ))
    P.append(ts_panel(
        "Errors by type, /s",
        [("sum by (error_type) (rate(http_request_errors_total[1m]))", "{{error_type}}")],
        12, 14, w=12, unit="short",
    ))
    return dashboard("wms-api-service", "WMS-API — Service Dashboard", P, ["wms", "service"])


def build_consumer():
    global _pid
    _pid = 0
    P = []
    P.append(stat_panel(
        "Processing p95, ms", "1000 * sli:event_processing_p95_seconds:5m", 0, 0, w=6, unit="ms",
        thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 250}, {"color": "red", "value": 1000}],
    ))
    P.append(stat_panel("Throughput, ev/s", "sum(rate(events_processed_total[1m]))", 6, 0, w=6, unit="short"))
    P.append(stat_panel(
        "Max consumer lag", "max(consumer_lag)", 12, 0, w=6, unit="short",
        thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 100}, {"color": "red", "value": 1000}],
    ))
    P.append(stat_panel(
        "Cassandra connected", "cassandra_connected", 18, 0, w=6, unit="bool",
        thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}],
    ))
    P.append(ts_panel(
        "Events processed by type, /s",
        [("sum by (event_type) (rate(events_processed_total[1m]))", "{{event_type}}")],
        0, 6, w=12, unit="short",
    ))
    P.append(ts_panel(
        "Processing duration percentiles, ms",
        [
            ("1000 * histogram_quantile(0.50, sum by (le) (rate(event_processing_duration_seconds_bucket[5m])))", "p50"),
            ("1000 * histogram_quantile(0.95, sum by (le) (rate(event_processing_duration_seconds_bucket[5m])))", "p95"),
            ("1000 * histogram_quantile(0.99, sum by (le) (rate(event_processing_duration_seconds_bucket[5m])))", "p99"),
        ],
        12, 6, w=12, unit="ms",
    ))
    P.append(ts_panel(
        "Failed (DLQ) by error_code, /s",
        [("sum by (error_code) (rate(events_failed_total[1m]))", "{{error_code}}")],
        0, 14, w=12, unit="short",
    ))
    P.append(ts_panel(
        "Consumer lag by partition",
        [("max by (partition) (consumer_lag)", "p{{partition}}")],
        12, 14, w=12, unit="short",
    ))
    return dashboard("warehouse-consumer-service", "Consumer — Service Dashboard", P, ["warehouse", "service"])


def build_infra():
    global _pid
    _pid = 0
    P = []
    # Заголовок-подсказка: где узкое место?
    P.append(stat_panel(
        "Kafka brokers up", "kafka_brokers", 0, 0, w=6, unit="short",
        thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}],
        desc="Статус брокеров Kafka (kafka-exporter)",
    ))
    P.append(stat_panel(
        "Total consumer lag (Kafka)", "sum(kafka_consumergroup_lag)", 6, 0, w=6, unit="short",
        thresholds=[{"color": "green", "value": None}, {"color": "yellow", "value": 100}, {"color": "red", "value": 1000}],
        desc="Суммарный lag по группам (узкое место: консьюмер не успевает)",
    ))
    P.append(stat_panel(
        "Cassandra connected", "cassandra_connected", 12, 0, w=6, unit="bool",
        thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}],
    ))
    P.append(stat_panel(
        "Cassandra write errors /s", "rate(cassandra_write_errors_total[1m])", 18, 0, w=6, unit="short",
        thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 0.1}],
    ))
    P.append(ts_panel(
        "Kafka consumer lag by topic/partition",
        [("sum by (topic, partition) (kafka_consumergroup_lag)", "{{topic}}/p{{partition}}")],
        0, 6, w=12, unit="short",
        desc="Растёт lag -> узкое место в обработке (consumer/Cassandra)",
    ))
    P.append(ts_panel(
        "Kafka ingestion rate (offset growth), msg/s",
        [("sum(rate(kafka_topic_partition_current_offset{topic=\"warehouse-events\"}[1m]))", "warehouse-events")],
        12, 6, w=12, unit="short",
        desc="Скорость прихода сообщений в топик",
    ))
    P.append(ts_panel(
        "Cassandra write latency (observed by consumer), ms",
        [
            ("1000 * histogram_quantile(0.95, sum by (le) (rate(event_processing_duration_seconds_bucket[5m])))", "p95"),
            ("1000 * histogram_quantile(0.99, sum by (le) (rate(event_processing_duration_seconds_bucket[5m])))", "p99"),
        ],
        0, 14, w=12, unit="ms",
        desc="Время обработки события ~ время записи в Cassandra. Рост -> БД узкое место.",
    ))
    P.append(ts_panel(
        "Consumer lag (app metric) by partition",
        [("max by (partition) (consumer_lag)", "p{{partition}}")],
        12, 14, w=12, unit="short",
    ))
    return dashboard("warehouse-infrastructure", "Infrastructure — Where is the bottleneck?", P, ["infra"])


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for name, builder in (
        ("api_service.json", build_api),
        ("consumer_service.json", build_consumer),
        ("infrastructure.json", build_infra),
    ):
        (OUT / name).write_text(json.dumps(builder(), ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"written {name}")


if __name__ == "__main__":
    main()
