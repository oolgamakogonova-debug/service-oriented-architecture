# Warehouse Management System (Event-Driven, Kafka + Cassandra)

Event-driven система управления складом с at-least-once семантикой,
идемпотентной обработкой, Cassandra-кластером, мониторингом и Schema Evolution.

Реализовано **все 10 пунктов** задания (1–4 + 5–7 + 8–10 = 100/100).

---

## Содержание

1. [Быстрый старт](#быстрый-старт)
2. [Архитектура](#архитектура)
3. [Cassandra: модель данных](#модель-данных-cassandra)
4. [Идемпотентность](#идемпотентность)
5. [Out-of-order events](#обработка-событий-вне-порядка)
6. [Atomic multi-table updates (BATCH)](#консистентность-таблиц-batch)
7. [Dead Letter Queue](#dead-letter-queue)
8. [Cassandra cluster + consistency levels](#cassandra-cluster--consistency-levels)
9. [Monitoring (Prometheus + Grafana)](#monitoring-prometheus--grafana)
10. [Schema Evolution](#schema-evolution)
11. [E2E сценарии — пошагово для защиты](#e2e-сценарии)

---

## Быстрый старт

```bash
docker compose up -d --build
# Подождать ~90 секунд, пока поднимется Cassandra-кластер и применятся миграции.

# Health-check
curl http://localhost:8000/health
# {"status":"ok","kafka":true,"cassandra":true}

# Метрики
curl http://localhost:8000/metrics | grep events_processed_total

# Grafana — admin / admin
open http://localhost:3000
```

Producer контейнер сразу при старте отправляет демо-сценарий, чтобы сразу
было что показать в Grafana.

Чтобы прогнать **все 8 E2E сценариев** автоматически:
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r tests/requirements.txt
python tests/run_scenarios.py
```

---

## Архитектура

```
WMS Service (Producer) ──► Kafka topic: warehouse-events
                                        │
                                        ▼
                                Consumer Service (Python)
                          ┌─────────────┼──────────────┐
                          ▼             ▼              ▼
              warehouse-events-dlq   Cassandra    Prometheus + Grafana
              (Kafka)                (3-node      (метрики через /metrics)
                                      cluster, RF=3)
```

| Компонент          | Роль                                                                    |
|--------------------|-------------------------------------------------------------------------|
| WMS Service        | публикует события (`PRODUCT_RECEIVED`, `ORDER_CREATED`, …)              |
| Kafka              | надёжная доставка событий, key=product_id (ordering per product)        |
| Schema Registry    | хранение Avro схем, проверка backward compatibility                     |
| Consumer Service   | читает из Kafka, обновляет Cassandra, шлёт ошибки в DLQ                 |
| Cassandra (3 ноды) | состояние склада, RF=3, NetworkTopologyStrategy                         |
| DLQ topic          | проблемные события                                                      |
| Prometheus+Grafana | мониторинг consumer-lag, throughput, ошибок                             |

---

## Модель данных Cassandra

Таблицы спроектированы **под конкретные запросы** (см. п.2 ТЗ).
Схема — `cassandra/schema.cql`.

| Таблица                      | PK / CK                            | Назначение                                              |
|------------------------------|------------------------------------|---------------------------------------------------------|
| `inventory_by_product_zone`  | PK = `(product_id, zone_id)`       | точечный остаток в зоне (`Получить остаток X в Y`)      |
| `inventory_by_product`       | PK = `product_id`, CK = `zone_id`  | все зоны товара (`Все остатки товара X`)                |
| `inventory_by_zone`          | PK = `zone_id`, CK = `product_id`  | все товары в зоне (`Все товары в зоне Y`)               |
| `processed_events`           | PK = `event_id`, TTL=30d           | идемпотентность                                          |
| `orders`                     | PK = `order_id`                    | заказы и их статусы                                      |
| `event_history`              | PK = `event_date`, CK = `event_timestamp,event_id`, TTL=30d | аудит         |

**Почему такие ключи:**
- `inventory_by_product_zone` — самый частый запрос (точечная проверка
  остатка). Композитный partition key даёт O(1) lookup и хорошее
  распределение по нодам.
- `inventory_by_product` — partition по `product_id`, чтобы все зоны
  одного товара лежали в одной партиции и читались одним запросом.
- `inventory_by_zone` — зеркальная таблица, partition по `zone_id`
  (для задач инвентаризации/аудита одной зоны).
- `event_history` партиционируется по дню, чтобы партиции не
  разрастались (`time-series partitioning`); CK `(event_timestamp DESC,
  event_id)` — для эффективного «последние события за сегодня».

**Денормализация осознанная:** одни и те же `available_quantity`/
`reserved_quantity` хранятся в трёх таблицах. Это требует атомарного
обновления (см. ниже про BATCH), но даёт быстрые целевые чтения без JOIN.

**JOIN'ов нет** — каждый сценарий запроса покрывается выделенной таблицей.

---

## Идемпотентность

**Цель:** повторная обработка одного `event_id` не приводит к двойному
списанию/прибавлению.

**Механизм:**

1. Перед обработкой `is_event_processed(event_id)` (`SELECT` из
   `processed_events`).
2. Если уже обрабатывали — пропускаем, метрика
   `events_skipped_total{reason="duplicate"}` инкрементируется, offset
   коммитится.
3. Маркировка `INSERT INTO processed_events ... IF NOT EXISTS`
   (LWT) — гонки между конкурентами невозможны.
4. Запись в `processed_events` идёт в **одной** `LOGGED BATCH` с обновлением
   inventory-таблиц — нельзя обновить состояние и забыть пометить событие.

`processed_events.event_id` — partition key, lookup по нему — точечный
и быстрый. TTL = 30 дней (audit window достаточен для типичного
retention.ms Kafka).

**Дубликаты из at-least-once delivery Kafka не приводят к дублированию
изменений.**

---

## Обработка событий вне порядка

**Цель:** старые события не должны затирать более новое состояние.

**Подход:** каждая строка в `inventory_by_product_zone` хранит
`last_event_timestamp` — `timestamp` последнего применённого события.
Перед записью в handler'е:

```python
if event.timestamp <= row.last_event_timestamp:
    skip()  # events_skipped_total{reason=out_of_order}++
```

**Почему `timestamp`, а не `version` сущности:**
- В системе много источников событий (несколько WMS-инстансов), у каждого
  свой счётчик. Согласовать version сложно.
- Timestamp — естественный, монотонный, не требует координации.
- В крайнем случае можно перейти на hybrid logical clock (HLC).

Producer используется с `key=product_id` → все события одного товара
идут в одну Kafka-партицию, обрабатываются последовательно одним
consumer-инстансом → reorder возможен только из-за retry/replay, и
timestamp-проверки достаточно.

**Пример (ТЗ):**
- Event 1: `PRODUCT_RECEIVED` @ 12:00:00 → `available=100`
- Event 2: `PRODUCT_SHIPPED` @ 12:05:00 → `available=80`
- Event 3: `PRODUCT_RECEIVED` @ 12:02:00 (поздняя доставка) → **проигнорировано**.

---

## Консистентность таблиц (BATCH)

**Цель:** не должно быть состояния, когда `inventory_by_zone` обновлена,
а `inventory_by_product` — нет.

**Решение:** на каждое событие handler собирает список statement'ов,
описывающих изменения **всех** связанных таблиц + `processed_events`,
и выполняет их в одной `LOGGED BATCH` с `CL=QUORUM`.

```python
batch = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=QUORUM)
batch.add(upsert_zone, ...)
batch.add(upsert_product, ...)
batch.add(upsert_zone_idx, ...)
batch.add(insert_history, ...)
batch.add(mark_event_processed, ...)
session.execute(batch)
```

**LOGGED BATCH** в Cassandra:
- координатор записывает batchlog на 2 другие ноды;
- если координатор падает между applying — другая нода доиграет batchlog
  до конца;
- атомарность не «все-или-ничего как RDBMS», а **eventual atomicity**:
  все записи будут применены, либо все будут восстановлены при
  следующем gossip-цикле;
- этого достаточно для требования «не бывает частичного обновления».

`UNLOGGED BATCH` НЕ используется — он не даёт атомарности (только
batched performance), а нам нужна именно атомарность.

---

## Dead Letter Queue

**Цель:** ошибки обработки не должны блокировать consumer-цикл.

**Реализовано:**
- При `ValidationError` (например, `quantity < 0`, `insufficient
  available`) или `DESERIALIZATION_ERROR` → событие отправляется в
  `warehouse-events-dlq`.
- Структура DLQ-сообщения:
  ```json
  {
    "original_event": { ...полностью... },
    "error_reason":   "Invalid quantity: -5",
    "error_code":     "VALIDATION_ERROR",
    "failed_at":      "2026-04-01T12:00:00Z",
    "kafka_metadata": { "topic": "warehouse-events", "partition": 2, "offset": 12345 }
  }
  ```
- Если декодирование Avro упало — сохраняется raw bytes в base64.
- После отправки в DLQ offset коммитится, consumer продолжает работу.
- Метрики: `events_failed_total{error_code="..."}`.

**Инфраструктурные ошибки** (Cassandra недоступна, потеря коннекта) **не**
отправляются в DLQ — они приводят к НЕ-коммиту offset'а, и consumer
ретраит при следующей итерации.

---

## Cassandra cluster + consistency levels

**Кластер из 3 нод** (`cassandra-1, cassandra-2, cassandra-3`) поднимается
в `docker-compose.yml`. Используется `GossipingPropertyFileSnitch`,
все ноды в одном DC `dc1`.

**Keyspace:**
```cql
CREATE KEYSPACE warehouse
WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': 3};
```

`NetworkTopologyStrategy` + `RF=3` — все данные реплицируются на все 3
ноды. Это даёт максимальную доступность и читаемость в маленьком
кластере.

**Consistency levels:**

- **Запись:** `QUORUM` (= 2 из 3). Гарантирует, что данные записаны
  на большинство нод. При падении одной ноды записи **продолжают
  работать** (2 живых ≥ QUORUM).

- **Чтение:** `QUORUM`. **Обоснование выбора QUORUM, а не ONE:**

  | Уровень | Latency | Consistency                                           | Доступность           |
  |---------|---------|-------------------------------------------------------|-----------------------|
  | ONE     | низкая  | возможен stale read (ещё не догнала replication)      | 1 любая нода          |
  | QUORUM  | средняя | strong consistency: write QUORUM + read QUORUM        | 2 ноды из 3           |
  | ALL     | высокая | strong consistency                                    | требует все 3 ноды    |

  В нашей задаче **критична консистентность** (read-modify-write при
  обработке событий: если прочитать stale state — можно вычесть из
  устаревшего количества). Поэтому **QUORUM**.

  Если бы было больше read-only нагрузки (отчёты), можно было бы
  переключить чтение на `ONE` — мы потеряли бы strong-consistency, но
  получили бы скорость. В системе с двумя видами нагрузки это часто
  делают per-statement.

**Демонстрация отказоустойчивости** (см. сценарий 6):
```bash
docker exec cassandra-1 nodetool status        # 3 UN
docker stop cassandra-2
# отправить PRODUCT_RECEIVED — консьюмер продолжает работать (QUORUM=2/3)
docker start cassandra-2
docker exec cassandra-1 nodetool status        # 3 UN
```

С `CL=ALL` после `docker stop cassandra-2` запись бы упала — это можно
показать на защите для контраста.

---

## Monitoring (Prometheus + Grafana)

**HTTP-эндпоинты consumer'а** (порт 8000):

| Endpoint    | Назначение                                                              |
|-------------|-------------------------------------------------------------------------|
| `/health`   | liveness/readiness: 200 если оба коннекта (Kafka, Cassandra) живы; 503 иначе |
| `/metrics`  | Prometheus-формат, все ниже метрики                                     |

**Метрики:**

| Метрика                               | Тип       | Метки                | Описание                                         |
|---------------------------------------|-----------|----------------------|--------------------------------------------------|
| `consumer_lag`                        | Gauge     | topic, partition     | latest_offset − committed_offset                 |
| `events_processed_total`              | Counter   | event_type           | успешные                                         |
| `events_failed_total`                 | Counter   | event_type, error_code | ушли в DLQ                                     |
| `events_skipped_total`                | Counter   | reason               | duplicate / out_of_order                         |
| `event_processing_duration_seconds`   | Histogram | event_type           | задержка обработки                               |
| `cassandra_write_errors_total`        | Counter   | —                    | ошибки записи                                    |
| `kafka_connected`, `cassandra_connected` | Gauge  | —                    | 0/1 для health-индикации                         |

**Dashboard** — `grafana/dashboards/warehouse.json`. Панели:
1. Consumer lag по партициям (timeseries, требование ТЗ);
2. Throughput — events processed per second (timeseries, требование ТЗ);
3. Event processing duration p50/p95/p99 (histogram quantiles);
4. Cassandra write errors / DLQ rate (требование ТЗ);
5. Kafka/Cassandra connected stat-панели;
6. Skipped events.

**Алерты** (`prometheus/alerts.yml`):
- `ConsumerLagHigh`: `max(consumer_lag) > 100` 30 секунд → срабатывает.
- `ConsumerDown`: `up == 0`.
- `CassandraWriteErrors`: rate > 0.1/s.
- `HighEventFailureRate`: rate(`events_failed_total`) > 0.5/s.

---

## Schema Evolution

**Цель:** консьюмер обрабатывает V1 и V2 одновременно без рестарта.

**Реализация:**

1. Все события сериализуются Avro через **Confluent Schema Registry**.
2. Используется **`TopicRecordNameStrategy`**: `subject =
   {topic}-{record-fullname}`. В одном топике сосуществуют разные
   record-типы, каждый со своим subject'ом.
3. Для `warehouse.events.v1.ProductReceived` зарегистрированы **две
   версии**:
   - V1 — `schemas/product_received_v1.avsc` (без supplier_id)
   - V2 — `schemas/product_received_v2.avsc` (`supplier_id: ["null", "string"], default: null`)
4. `BACKWARD compatibility` явно выставлена для subject — Schema Registry
   на регистрации V2 проверяет, что V2 может прочитать данные V1.
5. **Confluent Avro Deserializer** автоматически использует writer-schema
   из заголовка сообщения (по `schema_id`) — V1-сообщения и V2-сообщения
   читаются прозрачно.
6. На уровне приложения: после десериализации проверяем `ev.get("supplier_id")
   in (None, value)`. В Cassandra колонка `supplier_id` есть для всех
   записей (V1 → null, V2 → значение). При обработке V1 события `supplier_id`
   в строке остаётся прежним (sticky-семантика).

**Стратегия совместимости — BACKWARD:**
- BACKWARD: новая версия может читать данные старой → только добавляем
  поля с default.
- Это позволяет сначала **раскатать новый consumer**, который понимает
  V2, потом начать слать V2-события.
- Альтернатива FORWARD (старый consumer читает новые данные) у нас
  не подходит, т.к. V1-консьюмер не знает про supplier_id.

**Пошаговая инструкция «как добавить V3»:**

1. Создать `schemas/product_received_v3.avsc`, добавив новое поле:
   ```json
   {"name": "warehouse_zone_temperature", "type": ["null", "double"], "default": null}
   ```
2. Запустить тестовую регистрацию:
   ```bash
   curl -X POST http://localhost:8081/compatibility/subjects/warehouse-events-warehouse.events.v1.ProductReceived/versions/latest \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data @<(jq -Rs '{"schema": .}' < schemas/product_received_v3.avsc)
   ```
   Если `is_compatible: true` — всё ок.
3. Зарегистрировать V3:
   ```bash
   curl -X POST http://localhost:8081/subjects/warehouse-events-warehouse.events.v1.ProductReceived/versions \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data @<(jq -Rs '{"schema": .}' < schemas/product_received_v3.avsc)
   ```
4. Добавить колонку в Cassandra:
   ```cql
   ALTER TABLE warehouse.inventory_by_product_zone
     ADD warehouse_zone_temperature double;
   ```
5. В `consumer/handlers.py` начать читать новое поле из события и
   записывать в Cassandra.
6. Раскатать consumer **до** того, как кто-то начнёт публиковать V3
   события. Старые consumer'ы (без поддержки V3) продолжат игнорировать
   новое поле — backward compatibility сохранена.

---

## Структура репозитория

```
warehouse-system/
├── docker-compose.yml          один компоуз для всей инфры
├── README.md                   этот файл
├── schemas/                    Avro-схемы (V1, V2, остальные типы)
├── cassandra/
│   └── schema.cql              миграции, применяются автоматически
├── producer/                   WMS Service: регистрирует схемы + демо-сценарий
├── consumer/                   главный consumer-сервис
│   ├── main.py                 цикл потребления, at-least-once
│   ├── handlers.py             бизнес-логика по типам событий
│   ├── cassandra_client.py     QUORUM, LOGGED BATCH, prepared
│   ├── schema_evolution.py     Avro deserializer (V1/V2)
│   ├── dlq.py                  отправка в warehouse-events-dlq
│   ├── metrics.py              Prometheus
│   ├── health.py               aiohttp /health, /metrics
│   └── config.py
├── prometheus/
│   ├── prometheus.yml
│   └── alerts.yml              ConsumerLag, ConsumerDown, ...
├── grafana/
│   ├── provisioning/           datasource + dashboard provisioning
│   └── dashboards/
│       └── warehouse.json      готовый dashboard
└── tests/
    └── run_scenarios.py        автоматизация всех 8 E2E сценариев из ТЗ
```

---

## E2E сценарии

### Сценарий 1: базовый цикл склада (пп. 1–3)
```bash
python tests/run_scenarios.py --scenario 1
```
Проверяет: PRODUCT_RECEIVED → RESERVED → MOVED → SHIPPED → ORDER_CREATED → ORDER_COMPLETED.

### Сценарий 2: идемпотентность (п. 4)
```bash
python tests/run_scenarios.py --scenario 2
```
Тот же event_id отправляется дважды → состояние не дублируется.

### Сценарий 3: консистентность таблиц (п. 5)
```bash
python tests/run_scenarios.py --scenario 3
```
После одного события все 3 таблицы имеют одинаковые значения.

### Сценарий 4: события вне порядка (п. 6)
```bash
python tests/run_scenarios.py --scenario 4
```
Событие с timestamp в прошлом игнорируется.

### Сценарий 5: DLQ (п. 7)
```bash
python tests/run_scenarios.py --scenario 5
```
Невалидное событие (`quantity=-5`) → DLQ с error_code=VALIDATION_ERROR.
Валидное событие после него — обрабатывается без проблем.

### Сценарий 6: Cassandra cluster + отказоустойчивость (п. 8)

Автоматическая часть:
```bash
python tests/run_scenarios.py --scenario 6
```

Ручная часть (для защиты):
```bash
# 1. Все 3 ноды UN
docker exec cassandra-1 nodetool status

# 2. Останавливаем одну ноду
docker stop cassandra-2

# 3. Шлём событие (через producer/run_scenarios)
python tests/run_scenarios.py --scenario 1   # отрабатывает корректно при QUORUM=2/3

# 4. Поднимаем обратно
docker start cassandra-2
sleep 30
docker exec cassandra-1 nodetool status

# 5. Демо разницы CL: ONE / QUORUM / ALL — см. секцию выше
```

### Сценарий 7: monitoring + consumer lag (п. 9)
```bash
python tests/run_scenarios.py --scenario 7
```
Затем вручную:
- открыть `http://localhost:3000` → Grafana → dashboard "Warehouse Consumer";
- остановить consumer (`docker stop consumer`) → видно рост lag в Grafana и
  срабатывание alert ConsumerLagHigh после 30 секунд (видно в
  `http://localhost:9090/alerts`).

### Сценарий 8: Schema Evolution (п. 10)
```bash
python tests/run_scenarios.py --scenario 8
```
- V1 событие → `supplier_id IS NULL` в Cassandra.
- V2 событие → `supplier_id = 'SUP-001'`.
- В Schema Registry для subject `warehouse-events-warehouse.events.v1.ProductReceived`
  видны 2 версии:
  ```bash
  curl -s http://localhost:8081/subjects/warehouse-events-warehouse.events.v1.ProductReceived/versions
  # [1, 2]
  ```

---

## Контрольные команды для защиты

```bash
# 1. Поднять всё
docker compose up -d --build

# 2. Проверить что всё работает
curl -s http://localhost:8000/health | jq
curl -s http://localhost:8000/metrics | grep events_processed_total

# 3. Заглянуть в Cassandra
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone LIMIT 20"

# 4. Заглянуть в DLQ
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic warehouse-events-dlq --from-beginning --max-messages 5

# 5. Топики и Schema Registry
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list
curl -s http://localhost:8081/subjects | jq
curl -s http://localhost:8081/subjects/warehouse-events-warehouse.events.v1.ProductReceived/versions | jq

# 6. nodetool
docker exec cassandra-1 nodetool status

# 7. Прогнать все сценарии
python tests/run_scenarios.py
```
