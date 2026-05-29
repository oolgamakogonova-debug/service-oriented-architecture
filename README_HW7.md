# Домашнее задание №7 — CI/CD, Testing & Observability

Документ описывает, что добавлено к складской системе (ДЗ предыдущего блока)
для выполнения ДЗ №7, как это запускать, как соотносится с каждым из 10 пунктов
задания, и что отвечать на защите.

> Базовая система (event-driven Warehouse: **Kafka + Schema Registry +
> 3-node Cassandra + consumer**) описана в `README.md`. Здесь — только
> надстройка ДЗ №7.

---

## 1. Что нового (кратко)

| Компонент | Было | Стало |
|---|---|---|
| `producer` | Kafka-продюсер с demo/idle | **WMS-API** — HTTP-сервис (FastAPI): write-path → Kafka, read-path → Cassandra, `/metrics`, `/health` |
| Метрики | только доменные у consumer | `http_*` тройка у WMS-API + доменные у consumer |
| Тесты | host-скрипт сценариев | **unit / integration / e2e** (pytest), запускаются в CI |
| Нагрузка | — | **k6** (≥10 VU, ≥30s), интегрирован в CI |
| Алерты | 4 правила, без Alertmanager | **Alertmanager** + правила на SLI |
| SLI/SLO | — | 3 SLI как recording rules + проверка в CI |
| Дашборды | 1 | **3**: WMS-API, consumer, инфраструктура |
| Exporters | — | **kafka-exporter** |
| CI | — | **GitHub Actions**: unit → build → integration/e2e → load → SLO |

Архитектура двух сервисов = **CQRS**: запись идёт командой через Kafka
(WMS-API → топик → consumer → Cassandra), чтение — напрямую из
материализованных таблиц Cassandra (WMS-API read-path).

```
            HTTP                         Kafka                       CQL
client ─────────▶ WMS-API ──(Avro)──▶ warehouse-events ──▶ consumer ──▶ Cassandra
   ▲   POST /events  │                                         │            │
   └── GET /inventory◀┘────────────────────(read-path)─────────┼────────────┘
                     │                                         │
            /metrics │                                /metrics │
                     ▼                                         ▼
                  Prometheus ◀── kafka-exporter ──┐      (scrape 5s)
                     │  ▲                          │
        alerts/SLI   │  └── recording rules (SLI)  │
                     ▼                              │
              Alertmanager        Grafana ◀─────────┘  (3 дашборда)
```

---

## 2. Как запустить

### Полный стенд (для разработки и **защиты**)
```bash
docker compose up -d --build      # 3-node Cassandra (RF=3), все сервисы
bash scripts/wait_for_stack.sh    # дождаться готовности
```
- WMS-API:     http://localhost:8080  (`/health`, `/metrics`, `/docs`)
- consumer:    http://localhost:8000/metrics
- Prometheus:  http://localhost:9090  (`/targets`, `/alerts`)
- Alertmanager:http://localhost:9093
- Grafana:     http://localhost:3000  (admin/admin)

### Облегчённый CI-стенд (single-node Cassandra, RF=1)
```bash
docker compose -f docker-compose.ci.yml up -d --build
```
Используется в CI и для быстрой локальной прогонки (см. `make ci-local`).
Отличие — одна нода Cassandra c `schema.ci.cql` (RF=1), чтобы укладываться в
ресурсы раннера. Код сервисов и метрики идентичны полному стенду.

### Тесты и нагрузка локально
```bash
make test-unit          # unit (без инфраструктуры)
make test-integration   # нужен поднятый стенд
make test-e2e
make load               # k6
make slo                # проверка SLO из Prometheus
make ci-local           # весь CI на CI-стенде одной командой
```

---

## 3. Соответствие пунктам задания

### Блок 1–4

**П.1 — CI pipeline** (`.github/workflows/ci.yml`)
Запускается на `push` и `pull_request`. Jobs: `unit` → `build` →
`integration-e2e` → `load-and-slo`. Любой упавший шаг роняет job (стандартное
поведение GitHub Actions; ненулевой exit code = красный пайплайн). Шаги
последовательны (каждый блок задания проверяется только при прохождении
предыдущего), unit вынесен в начало как быстрый барьер. Логи доступны в UI
Actions; при падении выгружаются `docker compose logs`.

**П.2 — Интеграционные тесты** (`tests/integration/`)
- Поднимают зависимости через Docker (`docker-compose.ci.yml`).
- Проверяют **взаимодействие сервисов**: HTTP-запрос в WMS-API → Kafka →
  consumer → запись в Cassandra (`test_api_to_cassandra.py`), а также путь DLQ
  (`test_dlq.py`).
- **Изолированы**: каждый тест использует уникальный `product_id` (uuid).
- **Чистят состояние**: фикстура `cleanup` удаляет созданные строки (teardown).
- Запускаются одной командой, `exit 0/1`: `pytest tests/integration`.

**П.3 — E2E тест** (`tests/e2e/test_full_lifecycle.py`)
Полный пользовательский сценарий **через API**: приёмка → заказ (резерв) →
завершение заказа → отгрузка. Проверяются: HTTP-статусы (202), тело ответа
(`accepted`, `event_id`, типы полей), и **состояние в Cassandra** на каждом
шаге + финальная сверка через read-path API.

**П.4 — Prometheus + базовые метрики** (`producer/metrics.py`, `prometheus/prometheus.yml`)
Каждый сервис отдаёт `/metrics`. WMS-API через ASGI-middleware
(`PrometheusMiddleware`) автоматически собирает обязательную тройку:

| Метрика | Тип | Labels |
|---|---|---|
| `http_requests_total` | Counter | `method, endpoint, status` |
| `http_request_errors_total` | Counter | `method, endpoint, error_type` |
| `http_request_duration_seconds` | Histogram | `method, endpoint` |

`endpoint` — это **шаблон маршрута** (`/api/v1/inventory/{product_id}`), а не
конкретный путь, чтобы не раздувать кардинальность.

Для consumer (Kafka-сервис, не HTTP) используется **Kafka-аналог** той же
тройки (так и разрешено заданием — «или аналог для gRPC/Kafka»):

| HTTP-аналог | Метрика consumer |
|---|---|
| requests total | `events_processed_total{event_type}` |
| errors total | `events_failed_total{event_type, error_code}` |
| duration histogram | `event_processing_duration_seconds{event_type}` |

Plus: `consumer_lag`, `events_skipped_total`, `cassandra_write_errors_total`,
`kafka_connected`, `cassandra_connected`. Prometheus реально скрапит оба сервиса
(`/targets`).

### Блок 5–7

**П.5 — Дашборды сервисов** (`grafana/dashboards/api_service.json`, `consumer_service.json`)
По отдельному дашборду на сервис, автопровижининг (datasource + dashboards в
`grafana/provisioning/`). На каждом ≥4 панели (по 8). Видны:
- **latency** p50/p95/p99 (`histogram_quantile` по `*_bucket`),
- **errors** (error rate %, errors by type),
- **throughput** (req/s, events/s).
Обновление 5s (real-time). JSON хранятся в репозитории.

**П.6 — Дашборд инфраструктуры** (`grafana/dashboards/infrastructure.json`)
Отвечает на вопрос **«где узкое место?»**, ≥3 панели инфраметрик:
- **Kafka** (kafka-exporter): brokers up, consumer lag by topic/partition,
  ingestion rate (рост offset) — растёт lag ⇒ consumer/БД не успевают.
- **Cassandra**: write latency p95/p99 (наблюдаемая consumer'ом — время
  обработки события ≈ время записи batch в Cassandra), write errors rate,
  `cassandra_connected` — рост latency/ошибок ⇒ узкое место в БД.
JSON в репозитории, автопровижининг.

> Про экспорт метрик Cassandra: задание прямо разрешает выбирать способ.
> Здесь Cassandra-сигналы берутся на уровне приложения (latency/ошибки/доступность),
> а Kafka — через kafka-exporter. Это устойчиво и не требует возни с JMX.
> Опционально можно добавить JMX-exporter к Cassandra (см. раздел 8).

**П.7 — Нагрузочное тестирование в CI** (`load/k6/load_test.js`, job `load-and-slo`)
k6, `constant-vus`: **VUS=15** (≥10), **DURATION=40s** (≥30s). Поток: 60% запись
(приёмка), 20% резерв, 20% чтение. Пороги (`thresholds`): `p95 < 500ms`,
`http_req_failed < 1%`, `wms_business_ok > 99%` — при нарушении k6 возвращает
ненулевой код и **CI падает**. До и под нагрузкой проверяется `/health`
(сервис остаётся доступен). Результат `summary.json` выгружается как артефакт.

### Блок 8–10

**П.8 — E2E + нагрузка + метрики в одном прогоне** (job `load-and-slo`, `scripts/slo_check.py`)
Один job: поднимает стек → k6-нагрузка → даёт Prometheus доскрапить →
`slo_check.py` запрашивает **Prometheus HTTP API** (`/api/v1/query`) и проверяет
числовые условия: `error rate < 1%` (availability ≥ 99%) и `p95 < 500ms`. При
нарушении — `exit 1`, CI падает. Артефакты (`summary.json`, `slo_report.json`)
сохраняются. Пороги обоснованы (см. раздел SLI/SLO).

**П.9 — Alert rules как код** (`prometheus/alerts.yml`, `alertmanager/alertmanager.yml`)
Покрыты **все 4** рекомендованные ситуации:
- `HighErrorRate` — высокий error rate (availability < 95%),
- `HighLatencyP95` — высокая latency (p95 > 1s),
- `ConsumerLagHigh` — consumer lag > 100,
- `TargetDown` — недоступность сервиса (`up == 0`).
Плюс `EventProcessingSlow`, `CassandraWriteErrors`, `HighEventFailureRate`.
Alertmanager поднят в docker-compose (`:9093`). Как показать firing — раздел 5.

**П.10 — System-level SLI/SLO** (`prometheus/recording_rules.yml`)
3 SLI как **recording rules** (считаются из метрик, не hardcoded), используются
в алертах и в CI-проверке. Таблица — раздел 4.

---

## 4. SLI / SLO (п.10) — подробно

Все SLI вычисляются PromQL-выражениями (recording rules в
`prometheus/recording_rules.yml`), а не зашиты константами.

### SLI-1. API availability — доля успешных запросов
- **Recording rule:** `sli:api_availability:ratio_5m`
- **PromQL:**
  `1 - (sum(rate(http_requests_total{status=~"5.."}[5m])) or vector(0)) / clamp_min(sum(rate(http_requests_total[5m])), 1)`
- **SLO (норма):** ≥ 99.5%
- **Порог отказа:** < 95% → алерт `HighErrorRate` (firing)
- **CI-гейт:** ≥ 99% (error rate < 1%) → `slo_check.py` роняет пайплайн
- **Обоснование:** при отсутствии багов 5xx практически нет (валидация даёт
  4xx, а не 5xx; 5xx = недоступность Kafka/Cassandra). >1% ошибок — заметная
  регрессия и разумный порог для блокировки релиза; <95% — серьёзная поломка,
  требующая немедленного алерта.

### SLI-2. API latency p95
- **Recording rule:** `sli:api_latency_p95_seconds:5m`
- **PromQL:** `histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))`
- **SLO:** < 500 ms · **Порог отказа:** > 1000 ms (алерт `HighLatencyP95`)
- **CI-гейт:** p95 < 500 ms (`slo_check.py`)
- **Обоснование:** write-path = валидация + Avro-сериализация + produce(acks=all)
  — единицы–десятки мс на здоровой системе; 500 ms даёт большой запас и
  соответствует типичному «интерактивному» SLO. >1s — деградация, ощутимая
  клиентом. Бакеты гистограммы специально сгущены вокруг 500 ms для точного p95.

### SLI-3. Event processing delay p95
- **Recording rule:** `sli:event_processing_p95_seconds:5m`
- **PromQL:** `histogram_quantile(0.95, sum by (le) (rate(event_processing_duration_seconds_bucket[5m])))`
- **SLO:** < 250 ms · **Порог отказа:** > 1000 ms (алерт `EventProcessingSlow`)
- **CI-гейт:** < 1000 ms (`slo_check.py`)
- **Обоснование:** обработка события ≈ время LOGGED BATCH в Cassandra при
  QUORUM — десятки мс; 250 ms — норма с запасом, >1s означает деградацию БД или
  перегрузку и расхождение состояния (растущий lag).

---

## 5. Как продемонстрировать срабатывание алертов (п.9)

Самый надёжный — **TargetDown**:
```bash
docker compose stop consumer
# через ~30s в http://localhost:9090/alerts алерт TargetDown -> Pending -> Firing,
# затем виден в http://localhost:9093 (Alertmanager)
docker compose start consumer        # вернуть
```

**HighErrorRate** (через 5xx на read-path):
```bash
docker compose stop cassandra-1 cassandra-2 cassandra-3
# GET /inventory начнёт отдавать 503 (5xx) -> availability падает
for i in $(seq 1 200); do curl -s -o /dev/null http://localhost:8080/api/v1/inventory/X; done
# availability < 0.95 -> алерт HighErrorRate firing
```

**ConsumerLagHigh** — дать всплеск нагрузки больше пропускной способности:
```bash
make load   # или k6 с большим VUS; lag по партиции кратко превысит 100
```

---

## 6. Структура CI (п.1)

```
push / pull_request
        │
        ▼
   ┌─────────┐   ┌──────────┐   ┌──────────────────┐   ┌────────────────────┐
   │  unit   │──▶│  build   │──▶│ integration-e2e  │──▶│   load-and-slo     │
   │ (быстро)│   │ (образы) │   │ compose up + pytest│  │ k6 + slo_check.py  │
   └─────────┘   └──────────┘   └──────────────────┘   └────────────────────┘
```
- `unit` — два прогона pytest (producer и consumer запускаются **раздельно**,
  т.к. оба содержат модули `config.py`/`metrics.py` и не уживаются в одном
  процессе Python).
- `integration-e2e` — поднимает `docker-compose.ci.yml`, ждёт health, гоняет
  интеграцию и e2e, при падении дампит логи, в конце `down -v`.
- `load-and-slo` — k6 (артефакт `summary.json`) + проверка SLO из Prometheus
  (артефакт `slo_report.json`). Падает при нарушении порогов.

---

## 7. Карта файлов

```
.github/workflows/ci.yml          CI pipeline (п.1, п.7, п.8)
producer/                         WMS-API сервис (FastAPI)
  app.py                          маршруты, /health, /metrics, middleware
  metrics.py                      http_* метрики + PrometheusMiddleware (п.4)
  models.py                       валидация запросов + построение Avro-событий
  kafka_publisher.py              регистрация схем + produce
  cassandra_reader.py             read-path (CQRS)
tests/
  unit/producer, unit/consumer    unit-тесты (логика, без инфраструктуры)
  integration/                    п.2 (взаимодействие сервисов, DLQ)
  e2e/                            п.3 (полный сценарий через API)
load/k6/load_test.js              нагрузочный тест (п.7)
prometheus/prometheus.yml         scrape + alerting + rule_files
prometheus/recording_rules.yml    SLI (п.10)
prometheus/alerts.yml             alert rules (п.9)
alertmanager/alertmanager.yml     Alertmanager (п.9)
grafana/dashboards/*.json         3 дашборда (п.5, п.6)
scripts/slo_check.py              проверка SLO из Prometheus (п.8)
scripts/gen_dashboards.py         генератор дашбордов (воспроизводимость)
scripts/wait_for_stack.sh         ожидание готовности стенда
docker-compose.yml                полный стенд (3-node Cassandra)
docker-compose.ci.yml             CI-стенд (single-node, RF=1)
cassandra/schema.ci.cql           схема RF=1 для CI
Makefile                          удобные команды
```

---

## 8. Возможные улучшения (если спросят «что дальше»)

- JMX-exporter к Cassandra для «честных» инфра-метрик (read/write latency,
  pending compactions): добавить `-javaagent` jmx_exporter к cassandra-1 и
  scrape-job — на дашборде появятся точные метрики БД вместо наблюдаемых.
- Несколько воркеров uvicorn / horizontal scale WMS-API.
- Error budget burn-rate алерты (multi-window) поверх SLI.

---

## 9. Подготовка к защите

### Демонстрация (E2E на защите)
1. `docker compose up -d --build` → показать логи запуска.
2. Открыть GitHub Actions → показать зелёный/красный пайплайн и его шаги.
3. Prometheus `/targets` (все UP), `/alerts`.
4. Grafana → 3 дашборда с живыми данными (после `make load` данные «оживают»).
5. Показать падение пайплайна при ошибке (например, временно ужесточить порог
   k6 или сломать тест) и срабатывание алерта (`docker compose stop consumer`).

### Теоретические вопросы — короткие ответы

**Почему Cassandra, а не Postgres?** Высокая нагрузка на запись, горизонтальное
масштабирование, отказоустойчивость (RF=3, запись при недоступности части нод).
Модель — query-first денормализация, без JOIN.

**Что такое QUORUM и зачем?** Кворум = `RF/2 + 1` реплик подтверждают операцию.
При RF=3 это 2. Гарантирует строгую согласованность чтения-после-записи
(`R + W > RF`). На CI-стенде RF=1 → QUORUM=1 (одна нода).

**Зачем Schema Registry и Avro?** Контракт сообщений, эволюция схем без поломки
потребителей. `BACKWARD`-совместимость: новые читатели читают старые данные
(поэтому `supplier_id` в V2 — union `["null","string"]` с default=null).
Subject-стратегия `TopicRecordNameStrategy` — разные типы событий в одном топике.

**Идемпотентность consumer'а?** `processed_events` + LWT `INSERT ... IF NOT
EXISTS`. Дубликат события (Kafka at-least-once) не применяется повторно. LWT
вынесен отдельно от batch, т.к. Cassandra не разрешает условные операции в
multi-table batch.

**Что такое Histogram и как считается p95?** Гистограмма копит наблюдения по
бакетам (`_bucket{le=...}`), `histogram_quantile(0.95, ...)` интерполирует
перцентиль по бакетам. Точность зависит от выбора бакетов (поэтому они сгущены
вокруг SLO 500 ms).

**Counter vs Gauge vs Histogram?** Counter — монотонно растёт (кол-во запросов),
для скорости берут `rate()`. Gauge — может расти/падать (lag, connected).
Histogram — распределение длительностей (для перцентилей).

**Почему метки endpoint — это шаблон, а не путь?** Иначе на каждый
`product_id` появлялась бы своя серия → взрыв кардинальности. Берём
`request.scope["route"].path`.

**Recording rule vs alert rule?** Recording — предвычисляет выражение и
сохраняет как новую метрику (наши SLI). Alert — выражение + `for`, при
истинности → Pending → Firing → Alertmanager.

**Зачем `for:` у алерта?** Подавляет ложные срабатывания на коротких всплесках:
условие должно держаться непрерывно указанное время.

**at-least-once / at-most-once / exactly-once?** Consumer: ручной commit offset
**после** успешной обработки = at-least-once; идемпотентность через
`processed_events` даёт эффект exactly-once на стороне состояния.

**Что такое DLQ и когда туда уходит событие?** Dead Letter Queue
(`warehouse-events-dlq`): события, которые нельзя обработать (ошибка
десериализации, неизвестный тип, бизнес-валидация — например отгрузка больше
остатка). Инфраструктурные ошибки (Cassandra недоступна) в DLQ **не** уходят —
offset не коммитится, Kafka повторит.

**Как нагрузочный тест роняет CI?** k6 `thresholds` при нарушении → ненулевой
exit code; шаг GitHub Actions падает → весь job красный.

**Что такое SLI/SLO/error budget?** SLI — измеримый индикатор (availability,
latency). SLO — целевое значение SLI. Error budget — допустимая доля нарушений
(например, 0.5% при SLO 99.5%); его расход — сигнал притормозить релизы.
