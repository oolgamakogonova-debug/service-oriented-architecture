-- ──────────────────────────────────────────────────────────────────
-- Warehouse Management System — Cassandra schema
-- Принципы:
--   * Таблицы спроектированы под конкретные запросы (query-first design)
--   * Нет JOIN, осознанная денормализация
--   * NetworkTopologyStrategy + RF=1 (одно-нодовый стенд для CI)
-- ──────────────────────────────────────────────────────────────────

CREATE KEYSPACE IF NOT EXISTS warehouse
WITH REPLICATION = {
  'class': 'NetworkTopologyStrategy',
  'dc1': 1
} AND DURABLE_WRITES = true;

USE warehouse;

-- ──────────────────────────────────────────────────────────────────
-- 1. inventory_by_product_zone
--    Запрос: «остаток конкретного товара в конкретной зоне»
--    PK = (product_id, zone_id) — точечный доступ
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
    product_id            text,
    zone_id               text,
    available_quantity    bigint,
    reserved_quantity     bigint,
    last_event_id         text,
    last_event_timestamp  timestamp,
    -- supplier_id появляется в Schema Evolution V2 (п.10), для V1 = null
    supplier_id           text,
    PRIMARY KEY ((product_id, zone_id))
);

-- ──────────────────────────────────────────────────────────────────
-- 2. inventory_by_product
--    Запрос: «все остатки товара по всем зонам»
--    PK = product_id, CK = zone_id — все зоны в одной партиции
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inventory_by_product (
    product_id            text,
    zone_id               text,
    available_quantity    bigint,
    reserved_quantity     bigint,
    last_event_timestamp  timestamp,
    PRIMARY KEY ((product_id), zone_id)
) WITH CLUSTERING ORDER BY (zone_id ASC);

-- ──────────────────────────────────────────────────────────────────
-- 3. inventory_by_zone
--    Запрос: «все товары в данной зоне»
--    PK = zone_id, CK = product_id
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inventory_by_zone (
    zone_id               text,
    product_id            text,
    available_quantity    bigint,
    reserved_quantity     bigint,
    last_event_timestamp  timestamp,
    PRIMARY KEY ((zone_id), product_id)
) WITH CLUSTERING ORDER BY (product_id ASC);

-- ──────────────────────────────────────────────────────────────────
-- 4. processed_events — идемпотентность
--    Запрос: «обрабатывали ли мы уже это событие?»
--    PK = event_id, default_time_to_live = 30 дней (audit window)
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS processed_events (
    event_id      text PRIMARY KEY,
    event_type    text,
    processed_at  timestamp,
    partition     int,
    offset        bigint
) WITH default_time_to_live = 2592000;

-- ──────────────────────────────────────────────────────────────────
-- 5. orders — заказы и их статусы
--    Запрос: «получить заказ по id»
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    order_id     text PRIMARY KEY,
    status       text,             -- CREATED | COMPLETED | CANCELLED
    items        list<frozen<map<text, text>>>,  -- [{product_id, zone_id, quantity}]
    created_at   timestamp,
    updated_at   timestamp
);

-- ──────────────────────────────────────────────────────────────────
-- 6. event_history — журнал событий для аудита (опционально, но полезно)
--    Партиционирование по дню, чтобы партиции не разрастались.
--    Запрос: «все события за день / интервал в течение дня»
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS event_history (
    event_date        date,
    event_timestamp   timestamp,
    event_id          text,
    event_type        text,
    product_id        text,
    zone_id           text,
    quantity          bigint,
    payload_json      text,
    PRIMARY KEY ((event_date), event_timestamp, event_id)
) WITH CLUSTERING ORDER BY (event_timestamp DESC, event_id ASC)
  AND default_time_to_live = 2592000;
