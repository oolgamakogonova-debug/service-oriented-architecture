## Event Schema (Avro)

```json
{
  "event_id": "uuid",
  "user_id": "string (partition key)",
  "movie_id": "string",
  "event_type": "VIEW_STARTED|VIEW_FINISHED|VIEW_PAUSED|VIEW_RESUMED|LIKED|SEARCHED",
  "timestamp": "timestamp-millis (UTC)",
  "device_type": "MOBILE|DESKTOP|TV|TABLET",
  "session_id": "string",
  "progress_seconds": "int"
}
```

**Partition key: `user_id`** — гарантирует порядок событий одного пользователя
в рамках одной партиции (критично для сессионного анализа: `VIEW_STARTED`
всегда раньше `VIEW_FINISHED`).

## Fault Tolerance (Task 8)

- **2 Kafka брокера** в docker-compose (kafka1, kafka2)
- **replication.factor=2**, **min.insync.replicas=1** для топика `movie-events`
- **Schema Registry** поднят (порт 8081)
- **Healthchecks** для всех компонентов
- **Producer acks=all + idempotence=true** — гарантированная доставка

## Producer Guarantees (Task 9)

- **Avro сериализация** через Schema Registry
- **acks=all** — ждём подтверждения от всех ISR
- **enable.idempotence=true** — защита от дубликатов
- **Retry с exponential backoff** при `BufferError`
- **Структурированное логирование** каждого отправленного события

## ClickHouse (Task 10)

- **Kafka Engine** таблица подключена к топику `movie-events`
- **AvroConfluent** формат с использованием Schema Registry
- **Materialized View** перекладывает данные в `MergeTree`
- **Партиционирование** по месяцу (`toYYYYMM(event_date)`)
- **Сортировка** `(event_date, event_type, user_id, event_time)` — оптимально для аналитики

## Aggregation Service (Task 11)

- Каждые **60 секунд** (`AGGREGATION_INTERVAL_SECONDS`) запускает расчёт
- Считает **TOP-фильмов** за 7 дней
- Считает **per-day аггрегаты** (views, unique_users, total_watch_time)
- Логирует каждый запуск в `aggregation_runs`
- Retry с exponential backoff

## S3 Export (Task 12)

- Экспорт в **Parquet** (snappy compression)
- **Hive-style partitioning**: `year=YYYY/month=MM/day=DD/`
- MinIO как S3-совместимое хранилище

## PostgreSQL (Task 13)

- Таблица `movie_aggregates` с UNIQUE constraint на `(date, movie_id)`
- **UPSERT** через `ON CONFLICT DO UPDATE` — идемпотентность
- Индексы по `(date, rank)` и `movie_id`
- Ранжирование (`rank_position`) по `views_count DESC`

## Grafana Dashboard (Task 14)

Автопровижн дашборда "Online Cinema Analytics" с панелями:

1. **Events per Hour** (timeseries, last 24h, по `event_type`)
2. **Top 10 Movies** (bar chart, last 7 days)
3. **Events by Device Type** (pie chart)
4. **Daily Unique Users** (timeseries, last 30 days)
5. **Average Watch Time per Movie** (bar chart)
6. **Total Events / Unique Users / Views / Likes (24h)** (stat panels)

## Testing

```bash
# Локальный запуск тестов (после `docker-compose up`)
cd tests
pip install -r requirements.txt
pytest -v --tb=short -s

# Или через Docker
docker-compose run --rm tests
```

Покрытие тестов:

- Task 8: Schema Registry, Kafka topic config, healthchecks
- Task 9: Producer API, валидация, все типы событий, flush
- Task 10: ClickHouse Kafka Engine, Materialized View, ingestion
- Task 11: Aggregation endpoints, scheduler, triggered aggregation
- Task 12: S3 bucket, Parquet export
- Task 13: PostgreSQL aggregates, UPSERT
- End-to-End: полный путь события Producer → Kafka → ClickHouse

## Project Structure

```text
online-cinema-pipeline/
├── docker-compose.yml       # Orchestration (все сервисы)
├── .env                     # Environment variables
├── avro/movie_event.avsc    # Avro schema
├── producer/                # Producer service (FastAPI)
├── aggregation/             # Aggregation service (FastAPI + APScheduler)
├── clickhouse/              # ClickHouse migrations
├── postgres/init.sql        # PostgreSQL schema
├── grafana/provisioning/    # Grafana datasources + dashboards
├── minio/                   # MinIO bucket init
├── scripts/                 # Helper scripts
└── tests/                   # Integration tests
```

## Troubleshooting

ClickHouse не видит события:

```bash
docker-compose logs clickhouse | grep -i kafka

docker exec -it clickhouse clickhouse-client \
  --query "SELECT count() FROM cinema.movie_events"
```

Проверить, что Kafka топик создан:

```bash
docker exec -it kafka1 kafka-topics --describe \
  --bootstrap-server kafka1:9092 --topic movie-events
```

Проверить Schema Registry:

```bash
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/movie-events-value/versions/latest
```

Перезапустить с чистого листа:

```bash
docker-compose down -v
docker-compose up -d --build
```

## Stop

```bash
docker-compose down         # остановить
docker-compose down -v      # + удалить volumes
```
