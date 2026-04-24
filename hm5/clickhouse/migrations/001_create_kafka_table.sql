CREATE DATABASE IF NOT EXISTS cinema;

-- Kafka engine table: reads messages directly from Kafka in Avro format
CREATE TABLE IF NOT EXISTS cinema.kafka_movie_events
(
    event_id          String,
    user_id           String,
    movie_id          String,
    event_type        LowCardinality(String),
    timestamp         Int64,
    device_type       LowCardinality(String),
    session_id        String,
    progress_seconds  Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka1:9092,kafka2:9093',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-consumer',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 2,
    kafka_thread_per_consumer = 1,
    kafka_max_block_size = 1048576;