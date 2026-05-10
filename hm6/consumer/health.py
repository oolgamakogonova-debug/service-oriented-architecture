"""HTTP-сервер с /health и /metrics."""
from aiohttp import web
import structlog

from metrics import (
    render_metrics, kafka_connected, cassandra_connected,
)

log = structlog.get_logger(__name__)


async def health_handler(_: web.Request) -> web.Response:
    """Liveness/readiness: 200 если оба коннекта живы, 503 иначе."""
    k = kafka_connected._value.get()  # type: ignore[attr-defined]
    c = cassandra_connected._value.get()  # type: ignore[attr-defined]
    if k == 1 and c == 1:
        return web.json_response({"status": "ok", "kafka": True, "cassandra": True}, status=200)
    return web.json_response(
        {"status": "degraded", "kafka": bool(k), "cassandra": bool(c)},
        status=503,
    )


async def metrics_handler(_: web.Request) -> web.Response:
    body, content_type = render_metrics()
    return web.Response(body=body, content_type=content_type.split(";")[0])


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/metrics", metrics_handler)
    return app


async def start_http(port: int) -> web.AppRunner:
    app = build_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    log.info("http_started", port=port)
    return runner
