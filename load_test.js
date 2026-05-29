// k6 нагрузочный тест WMS-API (ДЗ №7, п.7 и п.8).
//
// Создаёт реалистичный поток: смесь записи (POST приёмка/резерв) и чтения
// (GET остатки). Пороги (thresholds) завязаны на SLO: при их превышении k6
// возвращает ненулевой exit code -> CI падает.
//
// Запуск:
//   k6 run -e BASE_URL=http://localhost:8080 load/k6/load_test.js
//
// Параметры через env:
//   BASE_URL  (default http://localhost:8080)
//   VUS       (default 15)   — виртуальные пользователи (>= 10 по ТЗ)
//   DURATION  (default 45s)  — длительность (>= 30s по ТЗ)

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const VUS = parseInt(__ENV.VUS || '15');
const DURATION = __ENV.DURATION || '45s';

const writeErrors = new Counter('wms_write_errors');
const readErrors = new Counter('wms_read_errors');
const businessOk = new Rate('wms_business_ok');

export const options = {
  scenarios: {
    constant_load: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
    },
  },
  thresholds: {
    // Пороги совпадают с SLO системы (см. README, раздел SLI/SLO):
    'http_req_duration{expected_response:true}': ['p(95)<500'], // p95 < 500ms
    http_req_failed: ['rate<0.01'],                              // error rate < 1%
    wms_business_ok: ['rate>0.99'],
  },
};

// Небольшой пул товаров/зон, чтобы нагрузка попадала в одни и те же партиции
// (реалистично: «горячие» SKU) и одновременно были чтения существующих данных.
const ZONES = ['ZONE-A', 'ZONE-B', 'ZONE-C'];
function pid() {
  return `LOAD-SKU-${(__VU % 50)}`;
}
function zone() {
  return ZONES[Math.floor(Math.random() * ZONES.length)];
}

export function setup() {
  // Предзаполняем склад, чтобы GET-запросы возвращали данные.
  for (let i = 0; i < 50; i++) {
    http.post(
      `${BASE_URL}/api/v1/events/product-received`,
      JSON.stringify({ product_id: `LOAD-SKU-${i}`, zone_id: 'ZONE-A', quantity: 100000 }),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }
  sleep(2);
}

export default function () {
  const r = Math.random();

  if (r < 0.6) {
    // 60% — запись (приёмка): write-path через Kafka
    const res = http.post(
      `${BASE_URL}/api/v1/events/product-received`,
      JSON.stringify({ product_id: pid(), zone_id: zone(), quantity: 1 }),
      { headers: { 'Content-Type': 'application/json' }, tags: { op: 'write' } }
    );
    const ok = check(res, { 'received 202': (x) => x.status === 202 });
    businessOk.add(ok);
    if (!ok) writeErrors.add(1);
  } else if (r < 0.8) {
    // 20% — резерв
    const res = http.post(
      `${BASE_URL}/api/v1/events/product-reserved`,
      JSON.stringify({ product_id: pid(), zone_id: 'ZONE-A', quantity: 1 }),
      { headers: { 'Content-Type': 'application/json' }, tags: { op: 'reserve' } }
    );
    const ok = check(res, { 'reserved 202': (x) => x.status === 202 });
    businessOk.add(ok);
    if (!ok) writeErrors.add(1);
  } else {
    // 20% — чтение остатков: read-path из Cassandra
    const res = http.get(`${BASE_URL}/api/v1/inventory/${pid()}`, { tags: { op: 'read' } });
    const ok = check(res, { 'read 200': (x) => x.status === 200 });
    businessOk.add(ok);
    if (!ok) readErrors.add(1);
  }

  sleep(0.1);
}

export function handleSummary(data) {
  return {
    stdout: textSummary(data),
    'load/k6/summary.json': JSON.stringify(data, null, 2),
  };
}

// Минимальный текстовый отчёт (без зависимости от k6 utils).
function textSummary(data) {
  const m = data.metrics;
  const p95 = m.http_req_duration ? m.http_req_duration.values['p(95)'] : 0;
  const failed = m.http_req_failed ? m.http_req_failed.values.rate : 0;
  const reqs = m.http_reqs ? m.http_reqs.values.count : 0;
  const rps = m.http_reqs ? m.http_reqs.values.rate : 0;
  return (
    `\n=== k6 summary ===\n` +
    `requests:        ${reqs}\n` +
    `throughput:      ${rps.toFixed(1)} req/s\n` +
    `p95 latency:     ${p95.toFixed(1)} ms\n` +
    `error rate:      ${(failed * 100).toFixed(2)} %\n`
  );
}
