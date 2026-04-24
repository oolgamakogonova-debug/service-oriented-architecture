-- Main storage table
CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id          String,
    user_id           String,
    movie_id          String,
    event_type        LowCardinality(String),
    event_time        DateTime64(3, 'UTC'),
    event_date        Date MATERIALIZED toDate(event_time),
    device_type       LowCardinality(String),
    session_id        String,
    progress_seconds  Int32,
    ingested_at       DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, user_id, event_time)
TTL event_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;