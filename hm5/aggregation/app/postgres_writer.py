import logging
import psycopg2
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt, wait_exponential
from app.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

logger = logging.getLogger(__name__)


class PostgresWriter:
    def __init__(self):
        self._conn_params = dict(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )

    def _connect(self):
        return psycopg2.connect(**self._conn_params)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def upsert_aggregates(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        # Rank by views_count within a given date
        rows_sorted = sorted(
            rows, key=lambda r: (-r["views_count"], r["movie_id"])
        )
        for idx, r in enumerate(rows_sorted, start=1):
            r["rank_position"] = idx

        sql = """
        INSERT INTO movie_aggregates
            (aggregation_date, movie_id, views_count, unique_users, total_watch_time, rank_position)
        VALUES %s
        ON CONFLICT (aggregation_date, movie_id)
        DO UPDATE SET
            views_count = EXCLUDED.views_count,
            unique_users = EXCLUDED.unique_users,
            total_watch_time = EXCLUDED.total_watch_time,
            rank_position = EXCLUDED.rank_position,
            computed_at = NOW();
        """
        values = [
            (
                r["event_date"],
                r["movie_id"],
                r["views_count"],
                r["unique_users"],
                r["total_watch_time"],
                r["rank_position"],
            )
            for r in rows_sorted
        ]
        with self._connect() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, values)
                return cur.rowcount

    def start_run(self) -> int:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO aggregation_runs (status) VALUES ('running') RETURNING id;"
                )
                return cur.fetchone()[0]

    def finish_run(
        self,
        run_id: int,
        status: str,
        rows_processed: int = 0,
        error_message: str | None = None,
        s3_path: str | None = None,
    ):
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE aggregation_runs
                    SET run_finished_at = NOW(),
                        status = %s,
                        rows_processed = %s,
                        error_message = %s,
                        s3_path = %s
                    WHERE id = %s;
                    """,
                    (status, rows_processed, error_message, s3_path, run_id),
                )

    def ping(self) -> bool:
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    return cur.fetchone()[0] == 1
        except Exception as e:
            logger.error("Postgres ping failed: %s", e)
            return False