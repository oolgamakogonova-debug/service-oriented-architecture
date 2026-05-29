#!/usr/bin/env python3
"""Комплексная проверка метрик из Prometheus (ДЗ №7, п.8 и п.10).

Запрашивает PromQL-выражения через Prometheus HTTP API, сравнивает значения
SLI с порогами отказа и при нарушении завершается с кодом 1 (CI падает).
Результат сохраняется в JSON-артефакт.

Пороги (обоснование — см. README, раздел «SLI / SLO»):
  * API availability  >= 99.0 %   (порог отказа: < 99 %)
  * API latency p95    < 500 ms   (порог отказа: > 500 ms)
  * Event processing p95 < 1000 ms (порог отказа: > 1000 ms)

Использование:
  python scripts/slo_check.py --prometheus http://localhost:9090 \
      --window 5m --out slo_report.json
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request


def query(prom_url: str, expr: str) -> float | None:
    url = prom_url.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {data}")
    result = data["data"]["result"]
    if not result:
        return None
    return float(result[0]["value"][1])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--window", default="5m")
    ap.add_argument("--out", default="slo_report.json")
    args = ap.parse_args()
    w = args.window

    # ── SLI как PromQL (не hardcoded значения) ─────────────────────
    checks = [
        {
            "name": "api_availability_pct",
            "description": "Доля успешных HTTP-запросов WMS-API",
            "expr": (
                f"100 * (1 - "
                f"(sum(rate(http_requests_total{{status=~\"5..\"}}[{w}])) or vector(0)) "
                f"/ clamp_min(sum(rate(http_requests_total[{w}])), 1))"
            ),
            "threshold": 99.0,
            "op": ">=",
        },
        {
            "name": "api_latency_p95_ms",
            "description": "p95 времени ответа WMS-API",
            "expr": (
                f"1000 * histogram_quantile(0.95, "
                f"sum by (le) (rate(http_request_duration_seconds_bucket[{w}])))"
            ),
            "threshold": 500.0,
            "op": "<",
        },
        {
            "name": "event_processing_p95_ms",
            "description": "p95 времени обработки события консьюмером",
            "expr": (
                f"1000 * histogram_quantile(0.95, "
                f"sum by (le) (rate(event_processing_duration_seconds_bucket[{w}])))"
            ),
            "threshold": 1000.0,
            "op": "<",
        },
    ]

    report = {"prometheus": args.prometheus, "window": w, "checks": [], "passed": True}

    for c in checks:
        try:
            value = query(args.prometheus, c["expr"])
        except Exception as e:  # noqa: BLE001
            print(f"[ERROR] {c['name']}: запрос не выполнен: {e}", file=sys.stderr)
            report["checks"].append({**c, "value": None, "ok": False, "error": str(e)})
            report["passed"] = False
            continue

        if value is None:
            # Нет данных — считаем нарушением (нет трафика = нечего гарантировать)
            ok = False
            note = "нет данных в Prometheus (нет трафика?)"
        else:
            ok = (value >= c["threshold"]) if c["op"] == ">=" else (value < c["threshold"])
            note = ""

        status = "OK" if ok else "FAIL"
        shown = "n/a" if value is None else f"{value:.2f}"
        print(f"[{status}] {c['name']}: {shown} (порог {c['op']} {c['threshold']}) {note}")

        report["checks"].append(
            {
                "name": c["name"],
                "description": c["description"],
                "value": value,
                "threshold": c["threshold"],
                "op": c["op"],
                "ok": ok,
            }
        )
        if not ok:
            report["passed"] = False

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nОтчёт сохранён: {args.out}")

    if not report["passed"]:
        print("SLO НАРУШЕНЫ — пайплайн падает (exit 1)", file=sys.stderr)
        return 1
    print("Все SLO в норме (exit 0)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
