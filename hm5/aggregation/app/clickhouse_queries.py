import logging
from datetime import date, timedelta
import clickhouse_connect
from tenacity import retry, stop_after_attempt, wait_exponential
from app.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_DB,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
)

logger = logging.getLogger(__name__)


class ClickHouseClient:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB,
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def top_movies_last_7_days(self, limit: int = 10, as_of: date | None = None):
        """
        Returns top-N movies by views over the last 7 days.
        """
        as_of = as_of or date.today()
        start = as_of - timedelta(days=7)

        query = """
        SELECT
            movie_id,
            count() AS views_count,
            uniq(user_id) AS unique_users,
            sum(progress_seconds) AS total_watch_time
        FROM cinema.movie_events
        WHERE event_type = 'VIEW_STARTED'
          AND event_date >= %(start)s
          AND event_date <= %(end)s
        GROUP BY movie_id
        ORDER BY views_count DESC
        LIMIT %(limit)s
        """
        rows = self.client.query(
            query,
            parameters={"start": start, "end": as_of, "limit": limit},
        ).result_rows
        return [
            {
                "movie_id": r[0],
                "views_count": int(r[1]),
                "unique_users": int(r[2]),
                "total_watch_time": int(r[3]),
            }
            for r in rows
        ]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def daily_aggregates(self, target_date: date):
        """
        Returns per-movie aggregates for a given date.
        """
        query = """
        SELECT
            event_date,
            movie_id,
            count() AS views_count,
            uniq(user_id) AS unique_users,
            sum(progress_seconds) AS total_watch_time
        FROM cinema.movie_events
        WHERE event_type = 'VIEW_STARTED'
          AND event_date = %(target)s
        GROUP BY event_date, movie_id
        ORDER BY views_count DESC
        """
        rows = self.client.query(
            query, parameters={"target": target_date}
        ).result_rows
        return [
            {
                "event_date": r[0],
                "movie_id": r[1],
                "views_count": int(r[2]),
                "unique_users": int(r[3]),
                "total_watch_time": int(r[4]),
            }
            for r in rows
        ]

    def ping(self) -> bool:
        try:
            return self.client.ping()
        except Exception as e:
            logger.error("ClickHouse ping failed: %s", e)
            return False