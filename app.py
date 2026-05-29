"""FastAPI-приложение WMS-API.

Маршруты:
  POST /api/v1/events/product-received     приёмка
  POST /api/v1/events/product-shipped      отгрузка
  POST /api/v1/events/product-reserved     резерв
  POST /api/v1/events/product-released      снятие резерва
  POST /api/v1/events/product-moved         перемещение между зонами
  POST /api/v1/events/inventory-counted     инвентаризация
  POST /api/v1/orders                        создать заказ
  POST /api/v1/orders/{order_id}/complete    завершить заказ
  GET  /api/v1/inventory/{product_id}        остатки товара по зонам (read-path)
  GET  /api/v1/inventory/{product_id}/{zone_id}
  GET  /api/v1/zones/{zone_id}               товары в зоне
  GET  /health                               liveness/readiness
  GET  /metrics                              Prometheus

Фабрика create_app(publisher, reader) принимает зависимости извне —
это позволяет в unit-тестах подменить их фейками без Kafka/Cassandra.
"""
from __future__ import annotations

from typing import Any

import structlog
from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response

import models
from metrics import (
    PrometheusMiddleware,
    cassandra_connected,
    events_rejected_total,
    kafka_connected,
    render_metrics,
)

log = structlog.get_logger("api")


def create_app(publisher, reader) -> FastAPI:
    app = FastAPI(title="WMS API", version="1.0.0")
    app.add_middleware(PrometheusMiddleware)

    def _publish(built: models.BuiltEvent) -> JSONResponse:
        try:
            publisher.publish(
                record_full=built.record_full,
                value=built.value,
                key=built.key,
                event_type=built.value["event_type"],
            )
        except Exception as e:  # noqa: BLE001
            log.error("publish_failed", error=str(e))
            raise HTTPException(status_code=503, detail="kafka unavailable") from e
        return JSONResponse(
            status_code=202,
            content={"accepted": True, "event_id": built.value["event_id"], "key": built.key},
        )

    # ── write-path ─────────────────────────────────────────────────
    def _product_route(event_type: str):
        def handler(payload: dict[str, Any] = Body(...)) -> JSONResponse:
            try:
                built = models.build_product_event(event_type, payload)
            except Exception as e:  # noqa: BLE001
                events_rejected_total.labels(event_type=event_type, reason="validation").inc()
                raise HTTPException(status_code=422, detail=str(e)) from e
            return _publish(built)

        return handler

    app.post("/api/v1/events/product-received", status_code=202)(_product_route("PRODUCT_RECEIVED"))
    app.post("/api/v1/events/product-shipped", status_code=202)(_product_route("PRODUCT_SHIPPED"))
    app.post("/api/v1/events/product-reserved", status_code=202)(_product_route("PRODUCT_RESERVED"))
    app.post("/api/v1/events/product-released", status_code=202)(_product_route("PRODUCT_RELEASED"))

    @app.post("/api/v1/events/product-moved", status_code=202)
    def product_moved(payload: dict[str, Any] = Body(...)) -> JSONResponse:
        try:
            built = models.build_move_event(payload)
        except Exception as e:  # noqa: BLE001
            events_rejected_total.labels(event_type="PRODUCT_MOVED", reason="validation").inc()
            raise HTTPException(status_code=422, detail=str(e)) from e
        return _publish(built)

    @app.post("/api/v1/events/inventory-counted", status_code=202)
    def inventory_counted(payload: dict[str, Any] = Body(...)) -> JSONResponse:
        try:
            built = models.build_inventory_count_event(payload)
        except Exception as e:  # noqa: BLE001
            events_rejected_total.labels(event_type="INVENTORY_COUNTED", reason="validation").inc()
            raise HTTPException(status_code=422, detail=str(e)) from e
        return _publish(built)

    @app.post("/api/v1/orders", status_code=202)
    def create_order(payload: dict[str, Any] = Body(...)) -> JSONResponse:
        try:
            built = models.build_order_created_event(payload)
        except Exception as e:  # noqa: BLE001
            events_rejected_total.labels(event_type="ORDER_CREATED", reason="validation").inc()
            raise HTTPException(status_code=422, detail=str(e)) from e
        return _publish(built)

    @app.post("/api/v1/orders/{order_id}/complete", status_code=202)
    def complete_order(order_id: str) -> JSONResponse:
        try:
            built = models.build_order_completed_event(order_id)
        except Exception as e:  # noqa: BLE001
            events_rejected_total.labels(event_type="ORDER_COMPLETED", reason="validation").inc()
            raise HTTPException(status_code=422, detail=str(e)) from e
        return _publish(built)

    # ── read-path ──────────────────────────────────────────────────
    @app.get("/api/v1/inventory/{product_id}")
    def get_inventory(product_id: str) -> dict[str, Any]:
        if reader is None:
            raise HTTPException(status_code=503, detail="read-path disabled")
        rows = reader.inventory_by_product(product_id)
        return {"product_id": product_id, "zones": rows}

    @app.get("/api/v1/inventory/{product_id}/{zone_id}")
    def get_inventory_zone(product_id: str, zone_id: str) -> dict[str, Any]:
        if reader is None:
            raise HTTPException(status_code=503, detail="read-path disabled")
        rows = [r for r in reader.inventory_by_product(product_id) if r["zone_id"] == zone_id]
        if not rows:
            raise HTTPException(status_code=404, detail="not found")
        return rows[0]

    @app.get("/api/v1/zones/{zone_id}")
    def get_zone(zone_id: str) -> dict[str, Any]:
        if reader is None:
            raise HTTPException(status_code=503, detail="read-path disabled")
        return {"zone_id": zone_id, "products": reader.inventory_by_zone(zone_id)}

    # ── служебные ──────────────────────────────────────────────────
    @app.get("/health")
    def health() -> JSONResponse:
        k = bool(kafka_connected._value.get())  # type: ignore[attr-defined]
        c = True if reader is None else bool(cassandra_connected._value.get())  # type: ignore[attr-defined]
        ok = k and c
        return JSONResponse(
            status_code=200 if ok else 503,
            content={"status": "ok" if ok else "degraded", "kafka": k, "cassandra": c},
        )

    @app.get("/metrics")
    def metrics() -> Response:
        body, content_type = render_metrics()
        return Response(content=body, media_type=content_type.split(";")[0])

    return app
