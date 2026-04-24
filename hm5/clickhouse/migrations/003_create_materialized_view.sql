-- Materialized view: pulls data from Kafka engine to the storage table
CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.mv_movie_events
TO cinema.movie_events AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    fromUnixTimestamp64Milli(timestamp, 'UTC') AS event_time,
    device_type,
    session_id,
    progress_seconds
FROM cinema.kafka_movie_events;

-- Aggregating materialized view for fast TOP-movies queries
CREATE TABLE IF NOT EXISTS cinema.movie_views_daily
(
    event_date        Date,
    movie_id          String,
    views_count       AggregateFunction(count, UInt64),
    unique_users      AggregateFunction(uniq, String),
    total_progress    AggregateFunction(sum, Int64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, movie_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.mv_movie_views_daily
TO cinema.movie_views_daily AS
SELECT
    event_date,
    movie_id,
    countState() AS views_count,
    uniqState(user_id) AS unique_users,
    sumState(toInt64(progress_seconds)) AS total_progress
FROM cinema.movie_events
WHERE event_type = 'VIEW_STARTED'
GROUP BY event_date, movie_id;