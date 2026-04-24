CREATE TABLE IF NOT EXISTS movie_aggregates (
    id                SERIAL PRIMARY KEY,
    aggregation_date  DATE NOT NULL,
    movie_id          VARCHAR(64) NOT NULL,
    views_count       BIGINT NOT NULL DEFAULT 0,
    unique_users      BIGINT NOT NULL DEFAULT 0,
    total_watch_time  BIGINT NOT NULL DEFAULT 0,
    rank_position     INTEGER,
    computed_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_movie_date UNIQUE (aggregation_date, movie_id)
);

CREATE INDEX IF NOT EXISTS idx_agg_date_rank
    ON movie_aggregates (aggregation_date, rank_position);

CREATE INDEX IF NOT EXISTS idx_agg_movie
    ON movie_aggregates (movie_id);

CREATE TABLE IF NOT EXISTS aggregation_runs (
    id              SERIAL PRIMARY KEY,
    run_started_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    run_finished_at TIMESTAMP,
    status          VARCHAR(32) NOT NULL DEFAULT 'running',
    rows_processed  BIGINT DEFAULT 0,
    error_message   TEXT,
    s3_path         TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_started_at
    ON aggregation_runs (run_started_at);

CREATE INDEX IF NOT EXISTS idx_runs_status
    ON aggregation_runs (status);